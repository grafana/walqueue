package util

import (
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
)

// HashForShard computes a label hash without "le" label to ensure that all histogram buckets go to the same shard.
// We can't use HashWithoutLabels as it drops the "__name__" label which should be retained for sharding.
func HashForSharding(lbls labels.Labels) uint64 {
	b := make([]byte, 0, 1024)
	return xxhash.Sum64(lbls.BytesWithoutLabels(b, "le"))
}
