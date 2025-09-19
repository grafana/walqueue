package network

import (
	"github.com/cespare/xxhash/v2"
	lru "github.com/elastic/go-freelru"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
)

type cachedMetadata struct {
	SendAttempted bool
	Help          string
	Type          prompb.MetricMetadata_MetricType
	Unit          string
}

type metadataCache struct {
	items *lru.ShardedLRU[string, *cachedMetadata]
}

func hashStringXXHASH(s string) uint32 {
	return uint32(xxhash.Sum64String(s))
}

func NewMetadataCache(size int) (*metadataCache, error) {
	cache, err := lru.NewSharded[string, *cachedMetadata](uint32(size), hashStringXXHASH)
	if err != nil {
		return nil, err
	}
	return &metadataCache{
		items: cache,
	}, nil
}

func (c *metadataCache) GetIfNotSent(key string) (*cachedMetadata, bool) {
	value, ok := c.items.Get(key)
	if ok {
		if value.SendAttempted {
			return nil, false
		}
		value.SendAttempted = true
	}
	return value, ok
}

func (c *metadataCache) Set(value types.MetadataDatum) error {
	mdpb := prompb.MetricMetadata{}
	err := mdpb.Unmarshal(value.Bytes())
	if err != nil {
		return err
	}

	c.items.Add(mdpb.MetricFamilyName, &cachedMetadata{
		Help: mdpb.Help,
		Type: mdpb.Type,
		Unit: mdpb.Unit,
	})
	return nil
}

func (c *metadataCache) Clear() {
	c.items.Purge()
}
