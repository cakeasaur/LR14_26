package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ──────────────────────────────────────────────────────────────────────
// Kafka-продюсер: пишет DemoRecord в топик demographics-raw.
// Ключ = region (гарантирует, что записи одного региона попадают в одну
// партицию и сохраняют порядок).
// ──────────────────────────────────────────────────────────────────────

const (
	kafkaTopic         = "demographics-raw"
	kafkaPartitionsHnt = 3 // 3 партиции по ТЗ — создание выполняется отдельно
)

// KafkaSink реализует тот же контракт, что и обычный NDJSON Writer:
//
//	Write(records []DemoRecord) error
//	Close() error
//
// чтобы main.go мог переключаться между --mode=file и --mode=kafka.
type KafkaSink struct {
	w       *kafka.Writer
	logger  *zap.Logger
	mu      sync.Mutex
	written int
}

// NewKafkaSink создаёт async-writer kafka-go с балансировкой Hash(region)
// → партиция. Топик должен быть создан заранее (см. ensureTopic).
func NewKafkaSink(brokers []string, logger *zap.Logger) *KafkaSink {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  kafkaTopic,
		Balancer:               &kafka.Hash{}, // ключ region → стабильная партиция
		BatchSize:              100,
		BatchTimeout:           500 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		Async:                  false, // sync режим — упрощает учёт ошибок
	}
	return &KafkaSink{w: w, logger: logger}
}

// EnsureTopic создаёт топик с 3 партициями, если его нет.
// kafka-go AllowAutoTopicCreation создал бы 1 партицию по умолчанию,
// поэтому делаем явно.
func EnsureKafkaTopic(brokers []string, logger *zap.Logger) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	ctrlConn, err := kafka.Dial("tcp",
		fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrlConn.Close()

	cfg := []kafka.TopicConfig{{
		Topic:             kafkaTopic,
		NumPartitions:     kafkaPartitionsHnt,
		ReplicationFactor: 1,
	}}
	if err := ctrlConn.CreateTopics(cfg...); err != nil {
		// "Topic already exists" — нормально, не ошибка
		logger.Info("create topic", zap.Error(err))
	} else {
		logger.Info("kafka topic created",
			zap.String("topic", kafkaTopic),
			zap.Int("partitions", kafkaPartitionsHnt))
	}
	return nil
}

// Write конвертирует пачку DemoRecord в kafka.Message[] и шлёт батчем.
// Сигнатура совместима с обычным Writer для подмены в main.go.
func (k *KafkaSink) Write(records []DemoRecord) error {
	if len(records) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(records))
	for _, r := range records {
		body, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(r.Region), // ключ = регион → одна партиция/региону
			Value: body,
			Time:  r.CollectedAt,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	k.mu.Lock()
	k.written += len(records)
	k.mu.Unlock()
	return nil
}

func (k *KafkaSink) Close() error {
	k.logger.Info("closing kafka writer", zap.Int("total_written", k.written))
	return k.w.Close()
}

// Sink — общий интерфейс файлового и kafka-приёмников. Используется в main.go
// для прозрачного переключения --mode=file | --mode=kafka.
type Sink interface {
	Write(records []DemoRecord) error
	Close() error
}

// проверка что оба типа реализуют интерфейс на этапе компиляции
var (
	_ Sink = (*Writer)(nil)
	_ Sink = (*KafkaSink)(nil)
)
