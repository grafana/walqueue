package network

import (
	"github.com/maypok86/otter/v2"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/walqueue/types"
)

type cachedMetadata struct {
	SendAttempted bool
	Help          string
	Type          prompb.MetricMetadata_MetricType
	Unit          string
}

type metadataCache struct {
	items *otter.Cache[string, cachedMetadata]
}

func NewMetadataCache(size int) (*metadataCache, error) {
	cache, err := otter.New[string, cachedMetadata](&otter.Options[string, cachedMetadata]{MaximumSize: size})
	if err != nil {
		return nil, err
	}
	return &metadataCache{
		items: cache,
	}, nil
}

func (c *metadataCache) GetIfNotSent(key string) (cachedMetadata, bool) {
	value, ok := c.items.GetIfPresent(key)
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

	// This should probably be set or have some logic to update it if needed.
	c.items.SetIfAbsent(mdpb.MetricFamilyName, cachedMetadata{
		Help: mdpb.Help,
		Type: mdpb.Type,
		Unit: mdpb.Unit,
	})
	return nil
}

func (c *metadataCache) Clear() {
	c.items.InvalidateAll()
}
