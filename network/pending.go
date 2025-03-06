package network

import "github.com/grafana/walqueue/types"

type pending struct {
	items map[int][]types.MetricDatum
	meta  []types.MetadataDatum
}

func NewPending(shards int, defaultCapacity int) *pending {
	p := &pending{
		items: make(map[int][]types.MetricDatum),
	}
	for i := 0; i < shards; i++ {
		p.items[i] = make([]types.MetricDatum, 0, defaultCapacity)
	}
	p.meta = make([]types.MetadataDatum, 0, defaultCapacity)
	return p
}

func (p *pending) AddItems(item []types.Datum) {
	for _, d := range item {
		switch v := d.(type) {
		case types.MetricDatum:
			shardID := int(v.Hash() % uint64(len(p.items)))
			p.items[shardID] = append(p.items[shardID], v)
		case types.MetadataDatum:
			p.meta = append(p.meta, v)
		}
	}
}

func (p *pending) PullMetricItems(shardID int, count int) []types.MetricDatum {
	queue := p.items[shardID]
	pulled := queue[:min(count, len(queue))]
	p.items[shardID] = queue[len(pulled):]
	return pulled
}

func (p *pending) PullMetadataItems(count int) []types.MetadataDatum {
	pulled := p.meta[:min(count, len(p.meta))]
	p.meta = p.meta[len(pulled):]
	return pulled
}

func (p *pending) TotalLen() int {
	total := 0
	for _, v := range p.items {
		total += len(v)
	}
	total += len(p.meta)
	return total
}

func (p *pending) Reshard(shards int, defaultCapacity int) {
	newShards := make(map[int][]types.MetricDatum)
	for i := 0; i < shards; i++ {
		newShards[i] = make([]types.MetricDatum, 0, defaultCapacity)
	}
	for _, v := range p.items {
		for _, d := range v {
			shardID := int(d.Hash() % uint64(shards))
			newShards[shardID] = append(newShards[shardID], d)
		}
	}
	p.items = newShards
}
