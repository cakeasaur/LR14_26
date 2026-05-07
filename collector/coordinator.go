package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	collectorPrefix = "/collectors/"
	leaseTTL        = 15 // секунды
)

// Coordinator управляет регистрацией в etcd и распределением шардов.
type Coordinator struct {
	client    *clientv3.Client
	leaseID   clientv3.LeaseID
	hostname  string
	logger    *zap.Logger
	endpoints []string
}

func NewCoordinator(endpoints []string, logger *zap.Logger) (*Coordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd dial: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("collector-%d", os.Getpid())
	}
	// добавляем PID, чтобы несколько экземпляров на одном хосте не конфликтовали
	hostname = fmt.Sprintf("%s-%d", hostname, os.Getpid())

	return &Coordinator{
		client:    cli,
		hostname:  hostname,
		logger:    logger,
		endpoints: endpoints,
	}, nil
}

// Register регистрирует этот экземпляр в etcd с TTL-лизом.
// Запускает фоновый keepalive, чтобы запись не устаревала.
func (c *Coordinator) Register(ctx context.Context) error {
	resp, err := c.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("lease grant: %w", err)
	}
	c.leaseID = resp.ID

	key := collectorPrefix + c.hostname
	_, err = c.client.Put(ctx, key, "active", clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("etcd put: %w", err)
	}

	// keepalive в фоне — обновляем лиз каждые leaseTTL/3 секунды
	ch, err := c.client.KeepAlive(ctx, resp.ID)
	if err != nil {
		return fmt.Errorf("keepalive: %w", err)
	}
	go func() {
		for {
			select {
			case ka, ok := <-ch:
				if !ok {
					c.logger.Warn("etcd keepalive channel closed")
					return
				}
				c.logger.Debug("etcd keepalive", zap.Int64("ttl", ka.TTL))
			case <-ctx.Done():
				return
			}
		}
	}()

	c.logger.Info("registered in etcd", zap.String("key", key))
	return nil
}

// Deregister удаляет ключ из etcd и отзывает лиз при graceful shutdown.
func (c *Coordinator) Deregister(ctx context.Context) {
	key := collectorPrefix + c.hostname
	_, err := c.client.Delete(ctx, key)
	if err != nil {
		c.logger.Warn("etcd delete failed", zap.Error(err))
	}
	_, err = c.client.Revoke(ctx, c.leaseID)
	if err != nil {
		c.logger.Warn("etcd revoke failed", zap.Error(err))
	}
	c.client.Close()
	c.logger.Info("deregistered from etcd", zap.String("key", key))
}

// MyShardOf возвращает срез регионов, закреплённых за этим экземпляром.
// Алгоритм детерминирован: сортируем хостнеймы всех активных сборщиков,
// определяем свой индекс и берём каждый N-й регион начиная с индекса.
func (c *Coordinator) MyShardOf(ctx context.Context, regions []Region) ([]Region, error) {
	// получаем всех активных сборщиков
	resp, err := c.client.Get(ctx, collectorPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get collectors: %w", err)
	}

	var peers []string
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), collectorPrefix)
		peers = append(peers, key)
	}
	if len(peers) == 0 {
		// fallback: работаем в одиночку
		peers = []string{c.hostname}
	}
	sort.Strings(peers)

	myIdx := -1
	for i, p := range peers {
		if p == c.hostname {
			myIdx = i
			break
		}
	}
	if myIdx == -1 {
		// нас нет в списке (гонка после регистрации) — берём всё
		c.logger.Warn("hostname not found in peers, taking all regions",
			zap.String("hostname", c.hostname),
			zap.Strings("peers", peers))
		return regions, nil
	}

	n := len(peers)
	var shard []Region
	for i, r := range regions {
		if i%n == myIdx {
			shard = append(shard, r)
		}
	}

	c.logger.Info("shard assigned",
		zap.Int("my_index", myIdx),
		zap.Int("total_collectors", n),
		zap.Int("shard_size", len(shard)),
		zap.Strings("peers", peers))

	return shard, nil
}

// IsEtcdHealthy проверяет доступность etcd (для /health эндпоинта).
func (c *Coordinator) IsEtcdHealthy(ctx context.Context) bool {
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := c.client.Get(tctx, "/health-check")
	return err == nil
}
