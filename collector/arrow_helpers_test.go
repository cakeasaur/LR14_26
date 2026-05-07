package main

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// Arrow-хелперы, не требующие поднятого Flight-сервера.

func TestDemoSchema_Fields(t *testing.T) {
	schema := demoSchema()
	if schema.NumFields() != 6 {
		t.Fatalf("ожидалось 6 полей, получено %d", schema.NumFields())
	}
	expected := []struct {
		name string
		typ  arrow.DataType
	}{
		{"region", arrow.BinaryTypes.String},
		{"federal_district", arrow.BinaryTypes.String},
		{"year", arrow.PrimitiveTypes.Int32},
		{"indicator", arrow.BinaryTypes.String},
		{"value", arrow.PrimitiveTypes.Float64},
		{"collected_at", arrow.FixedWidthTypes.Timestamp_ms},
	}
	for i, e := range expected {
		f := schema.Field(i)
		if f.Name != e.name {
			t.Errorf("поле %d: имя %q, ожидалось %q", i, f.Name, e.name)
		}
		if !arrow.TypeEqual(f.Type, e.typ) {
			t.Errorf("поле %s: тип %v, ожидалось %v", f.Name, f.Type, e.typ)
		}
	}
}

func TestAppendRow_AndApproxRecordSize(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := demoSchema()
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []DemoRecord{
		{Region: "Москва", FederalDistrict: "ЦФО", Year: 2023,
			Indicator: "birth_rate", Value: 10.5, CollectedAt: now},
		{Region: "СПб", FederalDistrict: "СЗФО", Year: 2022,
			Indicator: "death_rate", Value: 13.2, CollectedAt: now},
	}
	for _, r := range records {
		appendRow(bldr, &r)
	}
	rec := bldr.NewRecord()
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Errorf("rows=%d, want 2", rec.NumRows())
	}
	if rec.NumCols() != 6 {
		t.Errorf("cols=%d, want 6", rec.NumCols())
	}

	// approxRecordSize должен вернуть положительное число
	size := approxRecordSize(rec)
	if size <= 0 {
		t.Errorf("approxRecordSize=%d, ожидалось > 0", size)
	}

	// Проверим что данные действительно записались
	regionCol := rec.Column(0).(*array.String)
	if regionCol.Value(0) != "Москва" {
		t.Errorf("region[0] = %q", regionCol.Value(0))
	}
	if regionCol.Value(1) != "СПб" {
		t.Errorf("region[1] = %q", regionCol.Value(1))
	}
	yearCol := rec.Column(2).(*array.Int32)
	if yearCol.Value(0) != 2023 || yearCol.Value(1) != 2022 {
		t.Errorf("year col: %v, %v", yearCol.Value(0), yearCol.Value(1))
	}
	valueCol := rec.Column(4).(*array.Float64)
	if valueCol.Value(0) != 10.5 {
		t.Errorf("value[0] = %v, want 10.5", valueCol.Value(0))
	}
}

func TestApproxRecordSize_Empty(t *testing.T) {
	alloc := memory.NewGoAllocator()
	bldr := array.NewRecordBuilder(alloc, demoSchema())
	defer bldr.Release()
	rec := bldr.NewRecord()
	defer rec.Release()

	// пустая запись: размер должен быть 0 или маленьким
	size := approxRecordSize(rec)
	if size < 0 {
		t.Errorf("approxRecordSize пустой записи отрицательный: %d", size)
	}
}
