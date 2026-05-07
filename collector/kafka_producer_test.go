package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestIsTopicExistsError_Nil(t *testing.T) {
	if isTopicExistsError(nil) {
		t.Error("nil → false ожидался")
	}
}

func TestIsTopicExistsError_TypedKafkaError(t *testing.T) {
	err := kafka.TopicAlreadyExists
	if !isTopicExistsError(err) {
		t.Error("kafka.TopicAlreadyExists должна распознаваться")
	}
}

func TestIsTopicExistsError_WrappedKafkaError(t *testing.T) {
	wrapped := fmt.Errorf("create topic: %w", kafka.TopicAlreadyExists)
	if !isTopicExistsError(wrapped) {
		t.Error("обёрнутая kafka.TopicAlreadyExists должна распознаваться через errors.As")
	}
}

func TestIsTopicExistsError_TextFallback(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"совпадение текста (TopicAlreadyExists)",
			errors.New("kafka server: TopicAlreadyExists"), true},
		{"совпадение текста (Topic with this name already exists)",
			errors.New("Topic with this name already exists"), true},
		{"произвольная ошибка",
			errors.New("connection refused"), false},
		{"пустое сообщение",
			errors.New(""), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isTopicExistsError(tc.err)
			if got != tc.want {
				t.Errorf("isTopicExistsError(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Sink-интерфейс должен реализовываться обоими типами (compile-time проверка
// уже стоит в kafka_producer.go, дублируем здесь для документирования).
func TestSinkInterface_Compliance(t *testing.T) {
	var _ Sink = (*Writer)(nil)
	var _ Sink = (*KafkaSink)(nil)
}
