package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	err = ctrlConn.CreateTopics(cfg...)
	switch {
	case err == nil:
		logger.Info("kafka topic created",
			zap.String("topic", kafkaTopic),
			zap.Int("partitions", kafkaPartitionsHnt))
	case isTopicExistsError(err):
		// "Topic already exists" — это OK, идём дальше
		logger.Info("kafka topic already exists",
			zap.String("topic", kafkaTopic))
	default:
		// Любая другая ошибка (broker unreachable, invalid RF и т.д.) —
		// возвращаем наверх, иначе раньше мы её тихо проглатывали
		return fmt.Errorf("create topic %q: %w", kafkaTopic, err)
	}
	return nil
}

// isTopicExistsError распознаёт kafka-ошибку "topic already exists" (код 36).
// kafka-go возвращает либо kafka.Error (typed), либо обёрнутую ошибку —
// ловим оба варианта.
func isTopicExistsError(err error) bool {
	if err == nil {
		return false
	}
	var kerr kafka.Error
	if errors.As(err, &kerr) && kerr == kafka.TopicAlreadyExists {
		return true
	}
	// Fallback по тексту — более старые версии kafka-go возвращают plain error
	return strings.Contains(err.Error(), "Topic with this name already exists") ||
		strings.Contains(err.Error(), "TopicAlreadyExists")
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
