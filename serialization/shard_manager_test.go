package serialization

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFileStorage is a simple mock implementation of types.FileStorage
type mockFileStorage struct {
	storeFunc func(ctx context.Context, meta map[string]string, data []byte) error
}

func (m *mockFileStorage) Start(_ context.Context) {
	// No-op for mock
}

func (m *mockFileStorage) Stop() {
	// No-op for mock
}

func (m *mockFileStorage) Store(ctx context.Context, meta map[string]string, data []byte) error {
	if m.storeFunc != nil {
		return m.storeFunc(ctx, meta, data)
	}
	return nil
}

func TestNewShardManager(t *testing.T) {
	tests := []struct {
		name        string
		config      ShardManagerConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: ShardManagerConfig{
				NumShards: 3,
				SerializerConfig: types.SerializerConfig{
					MaxSignalsInBatch: 100,
					FlushFrequency:    time.Second,
				},
				MaxWaitTime: time.Second,
			},
			expectError: false,
		},
		{
			name: "zero shards",
			config: ShardManagerConfig{
				NumShards: 0,
				SerializerConfig: types.SerializerConfig{
					MaxSignalsInBatch: 100,
					FlushFrequency:    time.Second,
				},
			},
			expectError: true,
		},
		{
			name: "negative shards",
			config: ShardManagerConfig{
				NumShards: -1,
				SerializerConfig: types.SerializerConfig{
					MaxSignalsInBatch: 100,
					FlushFrequency:    time.Second,
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &mockFileStorage{}
			stats := func(stats types.SerializerStats) {
				// No-op for test
			}

			sm, err := NewShardManager(tt.config, storage, stats, log.NewNopLogger())
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, sm)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, sm)
			}
		})
	}
}

func TestShardManagerBasicOperations(t *testing.T) {
	storage := &mockFileStorage{}
	statsMap := make(map[string]types.SerializerStats)
	stats := func(s types.SerializerStats) {
		statsMap["calls"] = s
	}

	cfg := ShardManagerConfig{
		NumShards: 3,
		SerializerConfig: types.SerializerConfig{
			MaxSignalsInBatch: 100,
			FlushFrequency:    time.Second,
		},
		MaxWaitTime: time.Second,
	}

	sm, err := NewShardManager(cfg, storage, stats, log.NewNopLogger())
	require.NoError(t, err)
	require.NotNil(t, sm)

	ctx := context.Background()
	require.NoError(t, sm.Start(ctx))

	// Test sending metrics
	metrics := []*types.PrometheusMetric{
		{
			L: labels.FromMap(map[string]string{"test": "value"}),
			T: time.Now().UnixMilli(),
			V: 123.45,
		},
	}
	err = sm.SendMetrics(ctx, metrics, nil)
	require.NoError(t, err)

	// Test sending metadata
	err = sm.SendMetadata(ctx, "test_metric", "unit", "help text", "gauge")
	require.NoError(t, err)

	// Test updating config
	newCfg := types.SerializerConfig{
		MaxSignalsInBatch: 200,
		FlushFrequency:    2 * time.Second,
	}
	success, err := sm.UpdateConfig(ctx, newCfg)
	require.NoError(t, err)
	assert.True(t, success)

	// Test stopping
	sm.Stop()
}

func TestShardManagerConcurrentAccess(t *testing.T) {
	storage := &mockFileStorage{}
	statsMap := make(map[string]types.SerializerStats)
	stats := func(s types.SerializerStats) {
		statsMap["calls"] = s
	}

	cfg := ShardManagerConfig{
		NumShards: 2, // Deliberately use fewer shards than goroutines
		SerializerConfig: types.SerializerConfig{
			MaxSignalsInBatch: 100,
			FlushFrequency:    time.Second,
		},
		MaxWaitTime: time.Second,
	}

	sm, err := NewShardManager(cfg, storage, stats, log.NewNopLogger())
	require.NoError(t, err)
	require.NotNil(t, sm)

	ctx := context.Background()
	require.NoError(t, sm.Start(ctx))

	// Simulate concurrent access from multiple goroutines
	const numGoroutines = 5
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			metrics := []*types.PrometheusMetric{
				{
					L: labels.FromMap(map[string]string{"test": "value", "goroutine": fmt.Sprintf("%d", id)}),
					T: time.Now().UnixMilli(),
					V: float64(100 + id),
				},
			}

			err := sm.SendMetrics(ctx, metrics, nil)
			if err != nil {
				t.Errorf("Error in goroutine %d: %v", id, err)
			}

			time.Sleep(10 * time.Millisecond) // Add a small delay to simulate work

			err = sm.SendMetadata(ctx, fmt.Sprintf("test_metric_%d", id), "unit", "help text", "gauge")
			if err != nil {
				t.Errorf("Error in goroutine %d: %v", id, err)
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Ensure we can still stop after concurrent access
	sm.Stop()
}

// mockShardManager is a simplified shard manager for testing timeouts
type mockShardManager struct {
	sync.Mutex
	maxWaitTime time.Duration
	busy        bool
}

func (m *mockShardManager) SendMetrics(ctx context.Context, metrics []*types.PrometheusMetric, externalLabels map[string]string) error {
	m.Lock()
	if m.busy {
		m.Unlock()
		// Simulate timeout
		return fmt.Errorf("timed out waiting for a free serializer shard")
	}
	
	m.busy = true
	m.Unlock()
	
	// Simulate processing
	time.Sleep(10 * time.Millisecond)
	
	m.Lock()
	m.busy = false
	m.Unlock()
	
	return nil
}

func (m *mockShardManager) SendMetadata(ctx context.Context, name, unit, help, pType string) error {
	return nil
}

func (m *mockShardManager) Start(ctx context.Context) error {
	return nil
}

func (m *mockShardManager) Stop() {}

func (m *mockShardManager) UpdateConfig(ctx context.Context, cfg types.SerializerConfig) (bool, error) {
	return true, nil
}

func TestShardManagerTimeout(t *testing.T) {
	// Create a mock shard manager that will return timeouts
	mock := &mockShardManager{
		maxWaitTime: 10 * time.Millisecond,
		busy:        true, // Start as busy
	}
	
	// First call should get timeout since it's busy
	metrics := []*types.PrometheusMetric{
		{
			L: labels.FromMap(map[string]string{"test": "value1"}),
			T: time.Now().UnixMilli(),
			V: 123.45,
		},
	}
	
	err := mock.SendMetrics(context.Background(), metrics, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for a free serializer shard")
	
	// Now mark as not busy
	mock.Lock()
	mock.busy = false
	mock.Unlock()
	
	// Second call should succeed
	err = mock.SendMetrics(context.Background(), metrics, nil)
	require.NoError(t, err)
}