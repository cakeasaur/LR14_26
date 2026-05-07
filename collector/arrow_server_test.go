package main

import "testing"

func TestMatchFilter_NoFilters_AlwaysMatch(t *testing.T) {
	rec := &DemoRecord{Region: "Москва", Indicator: "birth_rate", Year: 2020}
	f := &ticketFilter{}
	if !matchFilter(rec, f) {
		t.Error("пустой фильтр должен пропускать любую запись")
	}
}

func TestMatchFilter_Indicator(t *testing.T) {
	rec := &DemoRecord{Indicator: "birth_rate", Year: 2020, Region: "X"}
	if matchFilter(rec, &ticketFilter{Indicator: "death_rate"}) {
		t.Error("indicator mismatch должен фильтровать")
	}
	if !matchFilter(rec, &ticketFilter{Indicator: "birth_rate"}) {
		t.Error("indicator match должен пройти")
	}
}

func TestMatchFilter_YearRange(t *testing.T) {
	rec := &DemoRecord{Year: 2015, Indicator: "birth_rate", Region: "X"}

	tests := []struct {
		name     string
		filter   ticketFilter
		expected bool
	}{
		{"в диапазоне", ticketFilter{YearFrom: 2010, YearTo: 2020}, true},
		{"раньше", ticketFilter{YearFrom: 2016}, false},
		{"позже", ticketFilter{YearTo: 2010}, false},
		{"граница from", ticketFilter{YearFrom: 2015}, true},
		{"граница to", ticketFilter{YearTo: 2015}, true},
		{"только from", ticketFilter{YearFrom: 2010}, true},
		{"только to", ticketFilter{YearTo: 2020}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchFilter(rec, &tc.filter)
			if got != tc.expected {
				t.Errorf("matchFilter %+v = %v, ожидалось %v", tc.filter, got, tc.expected)
			}
		})
	}
}

func TestMatchFilter_Region(t *testing.T) {
	rec := &DemoRecord{Region: "Москва", Indicator: "birth_rate", Year: 2020}
	if matchFilter(rec, &ticketFilter{Region: "СПб"}) {
		t.Error("region mismatch должен фильтровать")
	}
	if !matchFilter(rec, &ticketFilter{Region: "Москва"}) {
		t.Error("region match должен пройти")
	}
}

func TestMatchFilter_Combined(t *testing.T) {
	rec := &DemoRecord{Region: "Москва", Indicator: "birth_rate", Year: 2020}
	// все условия выполнены
	f := &ticketFilter{Indicator: "birth_rate", YearFrom: 2010, YearTo: 2025, Region: "Москва"}
	if !matchFilter(rec, f) {
		t.Error("все условия совпадают — должно пройти")
	}
	// одно не совпадает
	f.Region = "СПб"
	if matchFilter(rec, f) {
		t.Error("регион не совпадает — должно фильтроваться")
	}
}
