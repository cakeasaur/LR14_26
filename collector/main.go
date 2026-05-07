package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// ──────────────────────────────────────────────
// Типы данных
// ──────────────────────────────────────────────

// Region описывает регион из regions.json.
type Region struct {
	Name            string `json:"name"`
	FederalDistrict string `json:"federal_district"`
}

// DemoRecord — одна запись демографических данных.
type DemoRecord struct {
	Region          string    `json:"region"`
	FederalDistrict string    `json:"federal_district"`
	Year            int       `json:"year"`
	Indicator       string    `json:"indicator"`
	Value           float64   `json:"value"`
	CollectedAt     time.Time `json:"collected_at"`
}

// ──────────────────────────────────────────────
// Эмулятор данных Росстата
// ──────────────────────────────────────────────

var indicators = []string{
	"population", "birth_rate", "death_rate",
	"natural_growth", "migration_growth", "life_expectancy",
}

// Исторические средние по РФ, sigma — разброс.
var indicatorParams = map[string][2]float64{
	"population":       {1_200_000, 800_000}, // среднее по региону ~1.2 млн
	"birth_rate":       {11.5, 2.5},
	"death_rate":       {13.0, 2.0},
	"natural_growth":   {-1.5, 3.0},
	"migration_growth": {0.5, 2.0},
	"life_expectancy":  {70.5, 3.5},
}

// populationBaseByRegion — примерные базовые значения населения для крупных регионов.
var populationBaseByRegion = map[string]float64{
	"Москва":               12_500_000,
	"Санкт-Петербург":      5_400_000,
	"Московская область":   7_700_000,
	"Краснодарский край":   5_700_000,
	"Республика Башкортостан": 4_000_000,
	"Республика Татарстан": 3_900_000,
	"Свердловская область": 4_300_000,
	"Ростовская область":   4_200_000,
	"Нижегородская область": 3_200_000,
	"Челябинская область":  3_500_000,
}

func generateValue(region Region, indicator string, year int, rng *rand.Rand) float64 {
	params := indicatorParams[indicator]
	mu, sigma := params[0], params[1]

	// для population используем базу по региону
	if indicator == "population" {
		if base, ok := populationBaseByRegion[region.Name]; ok {
			mu = base
		}
		// небольшой тренд: рост/спад ~0.3% в год относительно 2000
		trend := 1.0 + float64(year-2000)*0.003
		mu *= trend
	}

	// birth_rate: небольшой рост с 2006 (матcapital), спад после 2014
	if indicator == "birth_rate" {
		switch {
		case year >= 2006 && year <= 2015:
			mu += 1.5
		case year >= 2016:
			mu -= 0.5
		}
	}
	// death_rate: снижение смертности с 2005
	if indicator == "death_rate" {
		if year >= 2005 {
			mu -= float64(year-2005) * 0.08
		}
		// ковидный всплеск
		if year == 2020 || year == 2021 {
			mu += 3.0
		}
	}
	// life_expectancy: рост ~0.25 лет/год
	if indicator == "life_expectancy" {
		mu += float64(year-2000) * 0.25
		if year == 2020 || year == 2021 {
			mu -= 2.0
		}
	}

	val := mu + rng.NormFloat64()*sigma

	// ограничиваем физические границы
	switch indicator {
	case "birth_rate", "death_rate":
		val = math.Max(2.0, math.Min(val, 50.0))
	case "life_expectancy":
		val = math.Max(40.0, math.Min(val, 85.0))
	case "population":
		val = math.Max(10_000, val)
	}
	return math.Round(val*10) / 10
}

// collectRegion собирает все показатели по одному региону за 2000–2023.
func collectRegion(region Region, ch chan<- DemoRecord, rng *rand.Rand) {
	now := time.Now().UTC()
	for year := 2000; year <= 2023; year++ {
		for _, ind := range indicators {
			ch <- DemoRecord{
				Region:          region.Name,
				FederalDistrict: region.FederalDistrict,
				Year:            year,
				Indicator:       ind,
				Value:           generateValue(region, ind, year, rng),
				CollectedAt:     now,
			}
		}
	}
}

// ──────────────────────────────────────────────
// Пакетная запись в NDJSON
// ──────────────────────────────────────────────

type Writer struct {
	mu      sync.Mutex
	file    *os.File
	bw      *bufio.Writer
	written int
}

func NewWriter(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{file: f, bw: bufio.NewWriterSize(f, 256*1024)}, nil
}

func (w *Writer) Write(records []DemoRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		w.bw.Write(b)
		w.bw.WriteByte('\n')
		w.written++
	}
	return w.bw.Flush()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bw.Flush()
	return w.file.Close()
}

// ──────────────────────────────────────────────
// HTTP /health эндпоинт
// ──────────────────────────────────────────────

func startHealthServer(coord *Coordinator, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if coord.IsEtcdHealthy(ctx) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"status":"etcd_unavailable"}`)
		}
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("health server error", zap.Error(err))
		}
	}()
	return srv
}

// ──────────────────────────────────────────────
// main
// ──────────────────────────────────────────────

func main() {
	etcdAddr := flag.String("etcd", "localhost:2379", "etcd endpoint")
	outputPath := flag.String("output", "../data/raw.ndjson", "output NDJSON file")
	regionsFile := flag.String("regions-file", "regions.json", "JSON file with regions list")
	windowFlag := flag.String("window", "10s", "flush interval (tumbling window)")
	batchSize := flag.Int("batch", 50, "flush after N records")
	workerCount := flag.Int("workers", 8, "number of goroutines per shard")
	flightOnly := flag.Bool("flight-only", false, "только запустить Arrow Flight сервер на :8815 (без сбора)")
	flightSource := flag.String("flight-source", "../data/raw.ndjson", "источник данных для Flight-сервера")
	enableFlight := flag.Bool("enable-flight", false, "запустить Arrow Flight сервер параллельно со сбором")
	flag.Parse()

	// ── режим Flight-only ──
	if *flightOnly {
		logger, _ := zap.NewProduction()
		defer logger.Sync()
		ctx, cancel := context.WithCancel(context.Background())
		_, err := StartArrowFlightServer(ctx, *flightSource, logger)
		if err != nil {
			logger.Fatal("flight server failed", zap.Error(err))
		}
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
		time.Sleep(500 * time.Millisecond)
		return
	}

	// парсим window duration
	windowDur, err := time.ParseDuration(*windowFlag)
	if err != nil {
		windowDur = 10 * time.Second
	}

	// ── logger ──
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// ── загрузка регионов ──
	regData, err := os.ReadFile(*regionsFile)
	if err != nil {
		logger.Fatal("read regions file", zap.Error(err))
	}
	var allRegions []Region
	if err := json.Unmarshal(regData, &allRegions); err != nil {
		logger.Fatal("parse regions", zap.Error(err))
	}
	logger.Info("regions loaded", zap.Int("count", len(allRegions)))

	// ── контекст с отменой ──
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── etcd координация ──
	etcdEndpoints := strings.Split(*etcdAddr, ",")
	coord, err := NewCoordinator(etcdEndpoints, logger)
	if err != nil {
		logger.Fatal("create coordinator", zap.Error(err))
	}

	if err := coord.Register(ctx); err != nil {
		logger.Fatal("register in etcd", zap.Error(err))
	}

	// небольшая пауза, чтобы все реплики успели зарегистрироваться
	time.Sleep(500 * time.Millisecond)

	shard, err := coord.MyShardOf(ctx, allRegions)
	if err != nil {
		logger.Fatal("shard assignment", zap.Error(err))
	}

	// ── health server + Prometheus метрики ──
	healthSrv := startHealthServer(coord, logger)
	initMetrics()
	go startMetricsServer(logger)

	// ── опционально: Arrow Flight сервер параллельно со сбором ──
	if *enableFlight {
		if _, err := StartArrowFlightServer(ctx, *flightSource, logger); err != nil {
			logger.Warn("flight server failed", zap.Error(err))
		}
	}

	// ── writer ──
	writer, err := NewWriter(*outputPath)
	if err != nil {
		logger.Fatal("open output file", zap.Error(err))
	}

	// ── каналы ──
	rawCh := make(chan DemoRecord, 100)

	// ── агрегатор (задание 2) ──
	aggCh := make(chan []DemoRecord, 10)
	go RunAggregator(ctx, rawCh, aggCh, windowDur, *batchSize, logger)

	// ── горутина записи агрегированных пачек ──
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for batch := range aggCh {
			if err := writer.Write(batch); err != nil {
				logger.Error("write batch", zap.Error(err))
				incErrors()
			} else {
				addRecords(len(batch))
			}
		}
	}()

	// ── горутины сбора по регионам ──
	sem := make(chan struct{}, *workerCount)
	var collectWg sync.WaitGroup
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, region := range shard {
		collectWg.Add(1)
		sem <- struct{}{}
		go func(r Region) {
			defer collectWg.Done()
			defer func() { <-sem }()
			logger.Debug("collecting region", zap.String("region", r.Name))
			collectRegion(r, rawCh, rng)
		}(region)
	}

	// ── graceful shutdown ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		collectWg.Wait()
		close(rawCh)
	}()

	select {
	case sig := <-sigCh:
		logger.Info("signal received, shutting down", zap.String("signal", sig.String()))
		cancel()
		// ждём завершения сборщиков
		collectWg.Wait()
		close(rawCh)
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() {
			collectWg.Wait()
			close(done)
		}()
		return done
	}():
		logger.Info("all regions collected, flushing")
	}

	// ждём записи всех пачек
	wg.Wait()
	writer.Close()

	// останавливаем health сервер
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutCancel()
	healthSrv.Shutdown(shutCtx)

	// отписываемся из etcd
	coord.Deregister(context.Background())

	logger.Info("collector finished",
		zap.String("output", *outputPath),
		zap.Int("total_written", writer.written))
}
