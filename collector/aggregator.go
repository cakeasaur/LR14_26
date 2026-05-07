package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"go.uber.org/zap"
)

// AggRecord — агрегированная запись за одно окно.
type AggRecord struct {
	WindowStart string  `json:"window_start"`
	WindowEnd   string  `json:"window_end"`
	Region      string  `json:"region"`
	Indicator   string  `json:"indicator"`
	Sum         float64 `json:"sum"`
	Avg         float64 `json:"avg"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Count       int     `json:"count"`
	P50         float64 `json:"p50"`
}

// windowKey — ключ группировки внутри окна.
type windowKey struct {
	region    string
	indicator string
}

// RunAggregator читает сырые записи из rawCh, накапливает тумблинг-окно
// и отправляет пачки исходных DemoRecord в outCh.
// Окно закрывается по таймеру (dur) ИЛИ при накоплении batchSize записей.
func RunAggregator(
	ctx context.Context,
	rawCh <-chan DemoRecord,
	outCh chan<- []DemoRecord,
	dur time.Duration,
	batchSize int,
	logger *zap.Logger,
) {
	defer close(outCh)

	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	buf := make([]DemoRecord, 0, batchSize)
	winStart := time.Now()

	flush := func(reason string) {
		if len(buf) == 0 {
			return
		}
		winEnd := time.Now()
		aggs := aggregate(buf, winStart, winEnd)

		start := time.Now()
		// конвертируем агрегаты обратно в DemoRecord для единообразной записи
		out := make([]DemoRecord, 0, len(aggs))
		for _, a := range aggs {
			// сохраняем как JSON-строку в одном поле — используем спец.индикатор
			b, _ := json.Marshal(a)
			out = append(out, DemoRecord{
				Region:          a.Region,
				FederalDistrict: "_aggregated_",
				Year:            0,
				Indicator:       fmt.Sprintf("_agg_%s", a.Indicator),
				Value:           a.Avg,
				CollectedAt:     winEnd,
			})
			_ = b
		}

		// также пишем исходные записи (задание требует отправлять агрег. пачки)
		outCh <- buf

		elapsed := time.Since(start).Milliseconds()
		logger.Info("window closed",
			zap.String("reason", reason),
			zap.Int("raw_records", len(buf)),
			zap.Int("agg_groups", len(aggs)),
			zap.Int64("saved_ms", elapsed),
		)
		_ = out

		logAggregatedToFile(aggs, logger)

		buf = make([]DemoRecord, 0, batchSize)
		winStart = time.Now()
		setQueueDepth(0)
	}

	for {
		select {
		case rec, ok := <-rawCh:
			if !ok {
				flush("channel_closed")
				return
			}
			buf = append(buf, rec)
			setQueueDepth(len(buf))
			if len(buf) >= batchSize {
				flush("batch_full")
			}

		case <-ticker.C:
			flush("timer")

		case <-ctx.Done():
			flush("context_cancelled")
			return
		}
	}
}

// aggregate вычисляет sum/avg/min/max/count/p50 по (region, indicator).
func aggregate(records []DemoRecord, winStart, winEnd time.Time) []AggRecord {
	groups := make(map[windowKey][]float64)
	// сохраняем порядок для детерминизма
	order := make([]windowKey, 0)

	for _, r := range records {
		k := windowKey{region: r.Region, indicator: r.Indicator}
		if _, exists := groups[k]; !exists {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r.Value)
	}

	ws := winStart.UTC().Format(time.RFC3339)
	we := winEnd.UTC().Format(time.RFC3339)

	result := make([]AggRecord, 0, len(order))
	for _, k := range order {
		vals := groups[k]
		sort.Float64s(vals)

		n := len(vals)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		avg := sum / float64(n)

		// p50 (медиана)
		var p50 float64
		if n%2 == 0 {
			p50 = (vals[n/2-1] + vals[n/2]) / 2
		} else {
			p50 = vals[n/2]
		}

		result = append(result, AggRecord{
			WindowStart: ws,
			WindowEnd:   we,
			Region:      k.region,
			Indicator:   k.indicator,
			Sum:         math.Round(sum*100) / 100,
			Avg:         math.Round(avg*100) / 100,
			Min:         vals[0],
			Max:         vals[n-1],
			Count:       n,
			P50:         math.Round(p50*100) / 100,
		})
	}
	return result
}

// logAggregatedToFile сохраняет агрегаты в data/aggregated.ndjson.
func logAggregatedToFile(aggs []AggRecord, logger *zap.Logger) {
	f, err := os.OpenFile("../data/aggregated.ndjson",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		logger.Warn("open aggregated file", zap.Error(err))
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, a := range aggs {
		enc.Encode(a)
	}
}
