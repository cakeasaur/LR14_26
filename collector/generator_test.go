package main

import (
	"math/rand"
	"testing"
)

// generateValue: проверяем границы для каждого индикатора.

func TestGenerateValue_BirthRateInBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Москва", FederalDistrict: "ЦФО"}
	for year := 2000; year <= 2023; year++ {
		v := generateValue(region, "birth_rate", year, rng)
		if v < 2.0 || v > 50.0 {
			t.Errorf("birth_rate %v за %d вышел за [2.0, 50.0]", v, year)
		}
	}
}

func TestGenerateValue_DeathRateInBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Тверская область", FederalDistrict: "ЦФО"}
	for year := 2000; year <= 2023; year++ {
		v := generateValue(region, "death_rate", year, rng)
		if v < 2.0 || v > 50.0 {
			t.Errorf("death_rate %v за %d вышел за [2.0, 50.0]", v, year)
		}
	}
}

func TestGenerateValue_LifeExpectancyInBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Дагестан", FederalDistrict: "СКФО"}
	for year := 2000; year <= 2023; year++ {
		v := generateValue(region, "life_expectancy", year, rng)
		if v < 40.0 || v > 85.0 {
			t.Errorf("life_expectancy %v за %d вышел за [40.0, 85.0]", v, year)
		}
	}
}

func TestGenerateValue_PopulationKnownRegion(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Москва", FederalDistrict: "ЦФО"}
	// для известного региона базовое значение ~12.5M
	// Усредним 100 запусков, чтобы убрать шум sigma=800k
	var sum float64
	const n = 100
	for i := 0; i < n; i++ {
		sum += generateValue(region, "population", 2010, rng)
	}
	avg := sum / float64(n)
	// Москва в 2010 ~ 12.5M * (1 + 10*0.003) ≈ 12.875M
	if avg < 11_000_000 || avg > 14_500_000 {
		t.Errorf("Москва population avg = %v, ожидалось около 12-13 млн", avg)
	}
}

func TestGenerateValue_PopulationUnknownRegion(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Несуществующий", FederalDistrict: "??"}
	// дефолт mu=1_200_000, sigma=800_000 → среднее по 100 запускам ~1.2M
	var sum float64
	const n = 100
	for i := 0; i < n; i++ {
		sum += generateValue(region, "population", 2010, rng)
	}
	avg := sum / float64(n)
	if avg < 500_000 || avg > 2_500_000 {
		t.Errorf("default population avg = %v, ожидалось ~1.2 млн", avg)
	}
}

func TestGenerateValue_PopulationFloorEnforced(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "Несуществующий", FederalDistrict: "??"}
	for i := 0; i < 1000; i++ {
		v := generateValue(region, "population", 2010, rng)
		if v < 10_000 {
			t.Errorf("population %v < 10_000 (minimum floor)", v)
		}
	}
}

func TestGenerateValue_RoundedToOneDecimal(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	region := Region{Name: "X"}
	v := generateValue(region, "birth_rate", 2020, rng)
	// одно знак после запятой
	rounded := float64(int(v*10)) / 10
	if v != rounded {
		t.Errorf("значение %v не округлено до 0.1 (ожидалось %v)", v, rounded)
	}
}
