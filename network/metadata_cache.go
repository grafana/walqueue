package network

import (
	"sync"

	"github.com/golang/groupcache/lru"
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
	mut   sync.RWMutex
	items *lru.Cache
}

func NewMetadataCache(size int) (*metadataCache, error) {
	cache := lru.New(size)
	return &metadataCache{
		items: cache,
	}, nil
}

func (c *metadataCache) GetIfNotSent(key []string) map[string]cachedMetadata {
	c.mut.RLock()
	defer c.mut.RUnlock()

	results := make(map[string]cachedMetadata)

	for _, k := range key {
		v, ok := c.items.Get(k)
		value := v.(cachedMetadata)
		if ok {
			if !value.SendAttempted {
				value.SendAttempted = true
			} else {
				results[k] = value
			}
		}
	}
	return results
}

func (c *metadataCache) Set(value []types.MetadataDatum) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if len(value) == 0 {
		return nil
	}

	for _, v := range value {
		mdpb := prompb.MetricMetadata{}
		err := mdpb.Unmarshal(v.Bytes())
		if err != nil {
			return err
		}

		c.items.Add(mdpb.MetricFamilyName, cachedMetadata{
			Help: mdpb.Help,
			Type: mdpb.Type,
			Unit: mdpb.Unit,
		})
	}

	return nil
}

func (c *metadataCache) Clear() {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.items.Clear()
}
