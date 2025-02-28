package serialization

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
)

// ShardManagerConfig defines the configuration for the shard manager
type ShardManagerConfig struct {
	// Number of serializer shards to create
	NumShards int
	// Base serializer configuration to use for each shard
	SerializerConfig types.SerializerConfig
	// Maximum wait time for a free shard before timing out
	MaxWaitTime time.Duration
}

// shardManager distributes serialization workload across multiple serializer instances.
// It implements the same interface as serializer and distributes work across shards.
type shardManager struct {
	shards     []types.PrometheusSerializer
	shardMutex sync.Mutex
	// Channel for each shard to indicate when it's free
	shardFree     []chan struct{}
	maxWaitTime   time.Duration
	logger        log.Logger
	stats         func(stats types.SerializerStats)
	defaultConfig types.SerializerConfig
}

// NewShardManager creates a new sharding manager with the given configuration
func NewShardManager(cfg ShardManagerConfig, q types.FileStorage, stats func(stats types.SerializerStats), l log.Logger) (types.PrometheusSerializer, error) {
	if cfg.NumShards <= 0 {
		return nil, fmt.Errorf("number of shards must be greater than 0")
	}

	sm := &shardManager{
		shards:        make([]types.PrometheusSerializer, cfg.NumShards),
		shardFree:     make([]chan struct{}, cfg.NumShards),
		maxWaitTime:   cfg.MaxWaitTime,
		logger:        l,
		stats:         stats,
		defaultConfig: cfg.SerializerConfig,
	}

	// Create individual serializers for each shard
	for i := 0; i < cfg.NumShards; i++ {
		serializer, err := NewSerializer(cfg.SerializerConfig, q, stats, log.With(l, "shard", i))
		if err != nil {
			return nil, fmt.Errorf("failed to create serializer for shard %d: %w", i, err)
		}
		sm.shards[i] = serializer
		sm.shardFree[i] = make(chan struct{}, 1)
		sm.shardFree[i] <- struct{}{} // Initialize as free
	}

	return sm, nil
}

// SendMetrics distributes metrics to a free shard, or waits if none are free
func (sm *shardManager) SendMetrics(ctx context.Context, metrics []*types.PrometheusMetric, externalLabels map[string]string) error {
	shardIndex, free, err := sm.getFreeShard(ctx)
	if err != nil {
		return err
	}

	defer func() {
		// Mark shard as free when done
		free <- struct{}{}
	}()

	return sm.shards[shardIndex].SendMetrics(ctx, metrics, externalLabels)
}

// SendMetadata distributes metadata to a free shard, or waits if none are free
func (sm *shardManager) SendMetadata(ctx context.Context, name string, unit string, help string, pType string) error {
	shardIndex, free, err := sm.getFreeShard(ctx)
	if err != nil {
		return err
	}

	defer func() {
		// Mark shard as free when done
		free <- struct{}{}
	}()

	return sm.shards[shardIndex].SendMetadata(ctx, name, unit, help, pType)
}

// Start starts all serializer shards
func (sm *shardManager) Start(ctx context.Context) error {
	for i, shard := range sm.shards {
		if err := shard.Start(ctx); err != nil {
			level.Error(sm.logger).Log("msg", "failed to start shard", "shard", i, "err", err)
			return fmt.Errorf("failed to start shard %d: %w", i, err)
		}
	}
	return nil
}

// Stop stops all serializer shards
func (sm *shardManager) Stop() {
	for _, shard := range sm.shards {
		shard.Stop()
	}
}

// UpdateConfig updates the configuration for all shards
func (sm *shardManager) UpdateConfig(ctx context.Context, cfg types.SerializerConfig) (bool, error) {
	var lastErr error
	allSuccess := true

	for i, shard := range sm.shards {
		success, err := shard.UpdateConfig(ctx, cfg)
		if err != nil {
			level.Error(sm.logger).Log("msg", "failed to update config for shard", "shard", i, "err", err)
			lastErr = err
			allSuccess = false
		} else if !success {
			allSuccess = false
		}
	}

	sm.defaultConfig = cfg
	return allSuccess, lastErr
}

// getFreeShard finds a free shard or waits until one becomes available
func (sm *shardManager) getFreeShard(ctx context.Context) (int, chan struct{}, error) {
	// Try to find a free shard immediately
	for i, free := range sm.shardFree {
		select {
		case <-free:
			return i, free, nil
		default:
			// This shard is busy, try next one
		}
	}

	// No free shards immediately available, wait for one with timeout
	timeoutCtx := ctx
	if sm.maxWaitTime > 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, sm.maxWaitTime)
		defer cancel()
	}

	// Wait for any shard to become free
	for {
		for i, free := range sm.shardFree {
			select {
			case <-free:
				return i, free, nil
			case <-timeoutCtx.Done():
				if ctx.Err() == context.Canceled {
					return 0, nil, ctx.Err()
				}
				return 0, nil, fmt.Errorf("timed out waiting for a free serializer shard")
			default:
				// This shard is still busy
			}
		}

		// Small sleep to prevent tight loop
		select {
		case <-time.After(5 * time.Millisecond):
		case <-timeoutCtx.Done():
			if ctx.Err() == context.Canceled {
				return 0, nil, ctx.Err()
			}
			return 0, nil, fmt.Errorf("timed out waiting for a free serializer shard")
		}
	}
}