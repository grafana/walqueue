package stats

import (
	"context"
	"sync"

	"github.com/grafana/walqueue/types"
)

var _ types.StatsHub = (*stats)(nil)

type stats struct {
	mut             sync.RWMutex
	seriesNetwork   map[int]func(types.NetworkStats)
	metadataNetowrk map[int]func(types.NetworkStats)
	serializer      map[int]func(types.SerializerStats)
	index           int
	ctx             context.Context
}

func NewStats() types.StatsHub {
	return &stats{
		seriesNetwork:   make(map[int]func(types.NetworkStats)),
		serializer:      make(map[int]func(types.SerializerStats)),
		metadataNetowrk: make(map[int]func(types.NetworkStats)),
	}
}

func (s *stats) Start(ctx context.Context) {
	s.ctx = ctx
}

func (s *stats) Stop() {

}

func (s *stats) RegisterMetadataNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Lock()

	s.metadataNetowrk[s.index] = f
	index := s.index
	s.index++

	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		delete(s.metadataNetowrk, index)
	}
}

func (s *stats) RegisterSeriesNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Lock()

	s.seriesNetwork[s.index] = f
	index := s.index
	s.index++

	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		delete(s.seriesNetwork, index)
	}
}

func (s *stats) RegisterSerializer(f func(types.SerializerStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.serializer[s.index] = f
	index := s.index
	s.index++

	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		delete(s.serializer, index)
	}

}

func (s *stats) SendSerializerStats(st types.SerializerStats) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	for _, v := range s.serializer {
		v(st)
	}

}

func (s *stats) SendSeriesNetworkStats(st types.NetworkStats) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	for _, v := range s.seriesNetwork {
		v(st)
	}
}

func (s *stats) SendMetadataNetworkStats(st types.NetworkStats) {
	s.mut.RLock()
	defer s.mut.Unlock()

	for _, v := range s.metadataNetowrk {
		v(st)
	}
}
