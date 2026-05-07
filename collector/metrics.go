package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collector_queue_depth",
		Help: "Current number of records in the raw channel buffer.",
	})
	recordsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "collector_records_total",
		Help: "Total number of records successfully written.",
	})
	errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "collector_errors_total",
		Help: "Total number of write errors.",
	})
)

func initMetrics() {
	prometheus.MustRegister(queueDepth, recordsTotal, errorsTotal)
}

func startMetricsServer(logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9090", Handler: mux}
	logger.Info("metrics server listening on :9090")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Warn("metrics server error", zap.Error(err))
	}
}

func setQueueDepth(n int) { queueDepth.Set(float64(n)) }
func addRecords(n int)     { recordsTotal.Add(float64(n)) }
func incErrors()           { errorsTotal.Inc() }
