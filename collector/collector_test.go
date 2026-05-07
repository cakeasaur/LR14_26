package main

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCollectRegion_ProducesAllRecords(t *testing.T) {
	ch := make(chan DemoRecord, 200)
	rng := rand.New(rand.NewSource(1))
	region := Region{Name: "Москва", FederalDistrict: "ЦФО"}

	go func() {
		collectRegion(context.Background(), region, ch, rng)
		close(ch)
	}()

	count := 0
	years := map[int]bool{}
	indicators := map[string]bool{}
	for rec := range ch {
		count++
		if rec.Region != "Москва" || rec.FederalDistrict != "ЦФО" {
			t.Errorf("неверные поля региона: %+v", rec)
		}
		years[rec.Year] = true
		indicators[rec.Indicator] = true
	}
	// 24 года (2000-2023) × 6 показателей = 144 записи
	if count != 144 {
		t.Errorf("collectRegion дал %d записей, ожидалось 144", count)
	}
	if len(years) != 24 {
		t.Errorf("уникальных лет %d, ожидалось 24", len(years))
	}
	if len(indicators) != 6 {
		t.Errorf("уникальных индикаторов %d, ожидалось 6", len(indicators))
	}
}

func TestCollectRegion_AbortsOnContextCancel(t *testing.T) {
	ch := make(chan DemoRecord) // unbuffered — collectRegion заблокируется на send
	rng := rand.New(rand.NewSource(1))
	region := Region{Name: "X"}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collectRegion(ctx, region, ch, rng)
	}()

	// Не читаем из канала, отменяем контекст — collectRegion должен выйти.
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — горутина завершилась после cancel
	case <-time.After(2 * time.Second):
		t.Fatal("collectRegion не вышел после cancel — потенциальный deadlock")
	}
}
