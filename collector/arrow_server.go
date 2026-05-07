package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ──────────────────────────────────────────────────────────────────────
// Arrow Flight сервер: отдаёт демографические данные потоком RecordBatch
// ──────────────────────────────────────────────────────────────────────

const flightBatchSize = 1000

// ticketFilter описывает фильтр, передаваемый клиентом в DoGet.
type ticketFilter struct {
	Indicator string `json:"indicator,omitempty"`
	YearFrom  int    `json:"year_from,omitempty"`
	YearTo    int    `json:"year_to,omitempty"`
	Region    string `json:"region,omitempty"`
}

// demographicsFlightServer реализует flight.FlightServer.
type demographicsFlightServer struct {
	flight.BaseFlightServer
	sourcePath string
	logger     *zap.Logger
	alloc      memory.Allocator
}

// schema — описание Arrow-схемы для одной записи.
func demoSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "federal_district", Type: arrow.BinaryTypes.String},
		{Name: "year", Type: arrow.PrimitiveTypes.Int32},
		{Name: "indicator", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "collected_at", Type: arrow.FixedWidthTypes.Timestamp_ms},
	}, nil)
}

// DoGet обрабатывает запрос клиента: парсит ticket, читает NDJSON,
// фильтрует, упаковывает в RecordBatch по 1000 записей и стримит.
func (s *demographicsFlightServer) DoGet(
	ticket *flight.Ticket,
	stream flight.FlightService_DoGetServer,
) error {
	var f ticketFilter
	if err := json.Unmarshal(ticket.Ticket, &f); err != nil {
		return fmt.Errorf("invalid ticket: %w", err)
	}
	s.logger.Info("DoGet request",
		zap.String("indicator", f.Indicator),
		zap.Int("year_from", f.YearFrom),
		zap.Int("year_to", f.YearTo),
		zap.String("region", f.Region))

	file, err := os.Open(s.sourcePath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer file.Close()

	schema := demoSchema()
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer writer.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 4*1024*1024)

	bldr := array.NewRecordBuilder(s.alloc, schema)
	defer bldr.Release()

	totalRows := 0
	totalBytes := int64(0)
	totalBatches := 0
	bufferedRows := 0

	flushBatch := func() error {
		if bufferedRows == 0 {
			return nil
		}
		rec := bldr.NewRecord()
		defer rec.Release()
		if err := writer.Write(rec); err != nil {
			return err
		}
		totalBatches++
		// грубая оценка размера: bufferedRows * 64 байт + строки
		totalBytes += approxRecordSize(rec)
		bufferedRows = 0
		return nil
	}

	for scanner.Scan() {
		var rec DemoRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		// _agg_ записи (от tumbling-window) пропускаем — отдаём только сырые
		if strings.HasPrefix(rec.Indicator, "_agg_") {
			continue
		}
		if !matchFilter(&rec, &f) {
			continue
		}
		appendRow(bldr, &rec)
		bufferedRows++
		totalRows++

		if bufferedRows >= flightBatchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := flushBatch(); err != nil {
		return err
	}

	s.logger.Info("DoGet finished",
		zap.Int("total_rows", totalRows),
		zap.Int("batches", totalBatches),
		zap.Int64("approx_bytes", totalBytes))
	return nil
}

// matchFilter применяет ticket-фильтр к одной записи.
func matchFilter(r *DemoRecord, f *ticketFilter) bool {
	if f.Indicator != "" && r.Indicator != f.Indicator {
		return false
	}
	if f.YearFrom != 0 && r.Year < f.YearFrom {
		return false
	}
	if f.YearTo != 0 && r.Year > f.YearTo {
		return false
	}
	if f.Region != "" && r.Region != f.Region {
		return false
	}
	return true
}

// appendRow добавляет одну запись в RecordBuilder.
func appendRow(b *array.RecordBuilder, r *DemoRecord) {
	b.Field(0).(*array.StringBuilder).Append(r.Region)
	b.Field(1).(*array.StringBuilder).Append(r.FederalDistrict)
	b.Field(2).(*array.Int32Builder).Append(int32(r.Year))
	b.Field(3).(*array.StringBuilder).Append(r.Indicator)
	b.Field(4).(*array.Float64Builder).Append(r.Value)
	b.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(r.CollectedAt.UnixMilli()))
}

func approxRecordSize(rec arrow.Record) int64 {
	var total int64
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		for _, b := range col.Data().Buffers() {
			if b != nil {
				total += int64(b.Len())
			}
		}
	}
	return total
}

// StartArrowFlightServer поднимает gRPC-сервер на :8815.
func StartArrowFlightServer(ctx context.Context, sourcePath string, logger *zap.Logger) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", ":8815")
	if err != nil {
		return nil, fmt.Errorf("listen 8815: %w", err)
	}

	gs := grpc.NewServer()
	srv := &demographicsFlightServer{
		sourcePath: sourcePath,
		logger:     logger,
		alloc:      memory.NewGoAllocator(),
	}
	flight.RegisterFlightServiceServer(gs, srv)

	logger.Info("Arrow Flight server listening", zap.String("addr", ":8815"))

	go func() {
		if err := gs.Serve(lis); err != nil {
			logger.Warn("flight server stopped", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()

	return gs, nil
}

// убираем неиспользованную ссылку на time в случае пустого билда
var _ = time.Now
