package main

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// initMetrics использует MustRegister, который паникует при повторной
// регистрации. В тестах вызываем один раз через sync.Once.
var initOnce sync.Once

func ensureMetrics() {
	initOnce.Do(func() {
		// Если паника всё же случится (например, регистрация в main() уже
		// прошла) — глотаем её, метрики уже доступны.
		defer func() { _ = recover() }()
		initMetrics()
	})
}

func TestSetQueueDepth_UpdatesGauge(t *testing.T) {
	ensureMetrics()
	setQueueDepth(42)
	if got := testutil.ToFloat64(queueDepth); got != 42 {
		t.Errorf("queueDepth = %v, want 42", got)
	}
	setQueueDepth(0)
	if got := testutil.ToFloat64(queueDepth); got != 0 {
		t.Errorf("queueDepth после reset = %v", got)
	}
}

func TestAddRecords_IncrementsCounter(t *testing.T) {
	ensureMetrics()
	before := testutil.ToFloat64(recordsTotal)
	addRecords(10)
	addRecords(5)
	after := testutil.ToFloat64(recordsTotal)
	if after-before != 15 {
		t.Errorf("recordsTotal приросло на %v, ожидалось 15", after-before)
	}
}

func TestIncErrors_IncrementsCounter(t *testing.T) {
	ensureMetrics()
	before := testutil.ToFloat64(errorsTotal)
	incErrors()
	incErrors()
	after := testutil.ToFloat64(errorsTotal)
	if after-before != 2 {
		t.Errorf("errorsTotal приросло на %v, ожидалось 2", after-before)
	}
}
