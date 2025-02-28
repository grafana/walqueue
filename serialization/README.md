# Serialization Package

This package handles serialization of metrics data for storage and retrieval.

## Serializer

The `serializer` is responsible for collecting data from multiple appenders in-memory and periodically flushing the data to disk storage. It will flush based on a configured time duration or when it hits a certain number of items.

## ShardManager

The `ShardManager` provides a way to distribute serialization workload across multiple serializer instances. It implements the same interface as the serializer, ensuring it's a drop-in replacement, while distributing work to multiple shards internally.

### Features

- **Multiple Serializer Shards**: Configurable number of serializer instances to distribute load
- **Wait For Free Shard**: Sends data to a free shard, or waits if none are available
- **Configurable Timeout**: Specify maximum wait time before timing out
- **Same Interface**: Implements the PrometheusSerializer interface, making it a drop-in replacement

### Usage

```go
// Create a ShardManager instead of a single serializer
cfg := serialization.ShardManagerConfig{
    NumShards: 4, // Number of serializer shards to create
    SerializerConfig: types.SerializerConfig{
        MaxSignalsInBatch: 1000,
        FlushFrequency:    30 * time.Second,
    },
    MaxWaitTime: 5 * time.Second, // Maximum wait time for a free shard
}

// Create the shard manager
shardManager, err := serialization.NewShardManager(cfg, fileStorage, statsFunc, logger)
if err != nil {
    // Handle error
}

// Use just like a regular serializer
err = shardManager.Start(ctx)
if err != nil {
    // Handle error
}

// Send metrics - will be distributed across shards
err = shardManager.SendMetrics(ctx, metrics, labels)
```

### Benefits

- **Increased Throughput**: Multiple serializers can process data in parallel
- **Reduced Contention**: Each serializer has its own lock, reducing lock contention
- **Fault Isolation**: Issues in one shard don't directly impact others
- **Configurable Parallelism**: Adjust the number of shards based on system resources

### Configuration Options

- `NumShards`: Number of serializer instances to create and manage
- `SerializerConfig`: Configuration for each individual serializer
- `MaxWaitTime`: Maximum time to wait for a free shard before timing out

Each shard gets its own FileStorage instance, so be sure the underlying storage can handle concurrent operations.