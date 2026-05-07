package main

import (
	"math"
	"testing"
	"time"
)

func mustClose(t *testing.T, got, want, eps float64) {
	t.Helper()
	if math.Abs(got-want) > eps {
		t.Errorf("got %v, want %v (eps=%v)", got, want, eps)
	}
}

func TestAggregate_BasicStats(t *testing.T) {
	now := time.Now()
	records := []DemoRecord{
		{Region: "Москва", Indicator: "birth_rate", Value: 10.0, CollectedAt: now},
		{Region: "Москва", Indicator: "birth_rate", Value: 11.0, CollectedAt: now},
		{Region: "Москва", Indicator: "birth_rate", Value: 12.0, CollectedAt: now},
		{Region: "Москва", Indicator: "birth_rate", Value: 13.0, CollectedAt: now},
		{Region: "Москва", Indicator: "birth_rate", Value: 14.0, CollectedAt: now},
	}
	out := aggregate(records, now, now.Add(time.Second))

	if len(out) != 1 {
		t.Fatalf("ожидалась 1 группа (Москва, birth_rate), получено %d", len(out))
	}
	a := out[0]
	if a.Region != "Москва" || a.Indicator != "birth_rate" {
		t.Errorf("неверный ключ группы: %s/%s", a.Region, a.Indicator)
	}
	mustClose(t, a.Sum, 60.0, 0.01)
	mustClose(t, a.Avg, 12.0, 0.01)
	if a.Min != 10.0 || a.Max != 14.0 {
		t.Errorf("min/max: %v/%v", a.Min, a.Max)
	}
	if a.Count != 5 {
		t.Errorf("count = %d, want 5", a.Count)
	}
	mustClose(t, a.P50, 12.0, 0.01) // нечётное N=5 → центр
}

func TestAggregate_P50EvenAndOdd(t *testing.T) {
	now := time.Now()

	// чётное N=4: медиана = (vals[1]+vals[2])/2 после сортировки
	even := []DemoRecord{
		{Region: "X", Indicator: "v", Value: 1.0, CollectedAt: now},
		{Region: "X", Indicator: "v", Value: 2.0, CollectedAt: now},
		{Region: "X", Indicator: "v", Value: 3.0, CollectedAt: now},
		{Region: "X", Indicator: "v", Value: 4.0, CollectedAt: now},
	}
	out := aggregate(even, now, now)
	mustClose(t, out[0].P50, 2.5, 0.01)

	// нечётное N=3: медиана = vals[1] после сортировки
	odd := []DemoRecord{
		{Region: "Y", Indicator: "v", Value: 100.0, CollectedAt: now},
		{Region: "Y", Indicator: "v", Value: 50.0, CollectedAt: now},
		{Region: "Y", Indicator: "v", Value: 75.0, CollectedAt: now},
	}
	out = aggregate(odd, now, now)
	mustClose(t, out[0].P50, 75.0, 0.01)
}

func TestAggregate_MultipleGroups(t *testing.T) {
	now := time.Now()
	records := []DemoRecord{
		{Region: "Москва", Indicator: "birth_rate", Value: 10.0, CollectedAt: now},
		{Region: "Москва", Indicator: "death_rate", Value: 13.0, CollectedAt: now},
		{Region: "СПб", Indicator: "birth_rate", Value: 9.0, CollectedAt: now},
		{Region: "СПб", Indicator: "birth_rate", Value: 9.5, CollectedAt: now},
	}
	out := aggregate(records, now, now)
	if len(out) != 3 {
		t.Fatalf("ожидалось 3 группы (2 региона × показатели), получено %d", len(out))
	}
	// проверим что ключи уникальны
	seen := map[string]bool{}
	for _, a := range out {
		key := a.Region + "/" + a.Indicator
		if seen[key] {
			t.Errorf("дубль группы: %s", key)
		}
		seen[key] = true
	}
}

func TestAggregate_Empty(t *testing.T) {
	out := aggregate([]DemoRecord{}, time.Now(), time.Now())
	if len(out) != 0 {
		t.Errorf("пустой вход → %d групп, ожидалось 0", len(out))
	}
}

func TestAggregate_SingleRecord(t *testing.T) {
	now := time.Now()
	out := aggregate([]DemoRecord{
		{Region: "X", Indicator: "v", Value: 42.0, CollectedAt: now},
	}, now, now)
	if len(out) != 1 {
		t.Fatalf("одна запись → 1 группа, получено %d", len(out))
	}
	a := out[0]
	if a.Count != 1 || a.Sum != 42.0 || a.Min != 42.0 || a.Max != 42.0 || a.Avg != 42.0 || a.P50 != 42.0 {
		t.Errorf("единичная агрегация неверна: %+v", a)
	}
}

func TestAggregate_Determinism_SameInputSameOutput(t *testing.T) {
	now := time.Now()
	in := []DemoRecord{
		{Region: "A", Indicator: "v", Value: 1.0, CollectedAt: now},
		{Region: "B", Indicator: "v", Value: 2.0, CollectedAt: now},
		{Region: "A", Indicator: "v", Value: 3.0, CollectedAt: now},
	}
	r1 := aggregate(in, now, now)
	r2 := aggregate(in, now, now)
	if len(r1) != len(r2) {
		t.Fatal("разная длина при одинаковом входе")
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Errorf("несовпадение в группе %d: %+v vs %+v", i, r1[i], r2[i])
		}
	}
}
