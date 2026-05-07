package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriter_WriteAndRead(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "out.ndjson")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UTC()
	records := []DemoRecord{
		{Region: "Москва", FederalDistrict: "ЦФО", Year: 2023,
			Indicator: "birth_rate", Value: 10.5, CollectedAt: now},
		{Region: "СПб", FederalDistrict: "СЗФО", Year: 2023,
			Indicator: "death_rate", Value: 13.2, CollectedAt: now},
	}
	if err := w.Write(records); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if w.written != 2 {
		t.Errorf("written=%d, want 2", w.written)
	}

	// Прочитаем файл и проверим что записи валидны
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) != 2 {
		t.Fatalf("в файле %d строк, ожидалось 2", len(lines))
	}
	var got DemoRecord
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Errorf("неверный JSON: %v", err)
	}
	if got.Region != "Москва" || got.Value != 10.5 {
		t.Errorf("первая запись: %+v", got)
	}
}

func TestWriter_AppendsToExistingFile(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "out.ndjson")

	// Первый запись
	w1, _ := NewWriter(path)
	w1.Write([]DemoRecord{{Region: "A", Value: 1}})
	w1.Close()

	// Второй вызов — APPEND, не перезаписывает
	w2, _ := NewWriter(path)
	w2.Write([]DemoRecord{{Region: "B", Value: 2}})
	w2.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// две строки
	count := 0
	for _, c := range data {
		if c == '\n' {
			count++
		}
	}
	if count != 2 {
		t.Errorf("в файле %d строк, ожидалось 2 (append-режим)", count)
	}
}

func TestWriter_EmptyBatch(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "out.ndjson")
	w, _ := NewWriter(path)
	if err := w.Write(nil); err != nil {
		t.Errorf("пустая пачка не должна возвращать ошибку: %v", err)
	}
	if w.written != 0 {
		t.Errorf("written=%d после пустой пачки", w.written)
	}
	w.Close()
}

func TestNewWriter_CreatesParentDir(t *testing.T) {
	tmp := t.TempDir()
	deep := filepath.Join(tmp, "a", "b", "c", "out.ndjson")
	w, err := NewWriter(deep)
	if err != nil {
		t.Fatalf("NewWriter в несуществующей директории: %v", err)
	}
	w.Close()
	if _, err := os.Stat(filepath.Dir(deep)); err != nil {
		t.Error("родительская директория не создана")
	}
}

func TestSetAggregatedPath(t *testing.T) {
	SetAggregatedPath("/tmp/dir/raw.ndjson")
	if aggregatedPath != filepath.Join("/tmp/dir", "aggregated.ndjson") {
		t.Errorf("aggregatedPath = %q", aggregatedPath)
	}
}
