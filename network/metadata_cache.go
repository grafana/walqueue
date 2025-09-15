package network

import (
	"github.com/grafana/walqueue/types"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/prometheus/prompb"
)

type cachedMetadata struct {
	SendAttempted bool
	Help          string
	Type          prompb.MetricMetadata_MetricType
	Unit          string
}

type metadataCache struct {
	items *lru.Cache[string, cachedMetadata]
}

func NewMetadataCache(size int) (*metadataCache, error) {
	cache, err := lru.New[string, cachedMetadata](size)
	if err != nil {
		return nil, err
	}
	return &metadataCache{
		items: cache,
	}, nil
}

func (c *metadataCache) GetIfNotSent(key string) (cachedMetadata, bool) {
	value, ok := c.items.Get(key)
	if ok {
		if !value.SendAttempted {
			value.SendAttempted = true
		} else {
			return value, false
		}
	}
	return value, ok
}

func (c *metadataCache) Set(value types.MetadataDatum) error {
	mdpb := prompb.MetricMetadata{}
	err := mdpb.Unmarshal(value.Bytes())
	if err != nil {
		return err
	}

	c.items.ContainsOrAdd(mdpb.MetricFamilyName, cachedMetadata{
		Help: mdpb.Help,
		Type: mdpb.Type,
		Unit: mdpb.Unit,
	})
	return nil
}

func (c *metadataCache) Clear() {
	c.items.Purge()
}
