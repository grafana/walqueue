package stats

import (
	"sync"

	"github.com/grafana/walqueue/types"
)

var _ types.StatsHub = (*stats)(nil)

// stats is used to collect and distribute stats to interested party.
// It does this by keeping track of interested parties to each type.
// Whenever a interested party registers they are given a NotificationRelease
// that cleans up.
type stats struct {
	mut             sync.RWMutex
	seriesNetwork   map[int]func(types.NetworkStats)
	metadataNetwork map[int]func(types.NetworkStats)
	serializer      map[int]func(types.SerializerStats)
	parralelism     map[int]func(types.ParallelismStats)
	index           int
}

func NewStats() types.StatsHub {
	return &stats{
		seriesNetwork:   make(map[int]func(types.NetworkStats)),
		serializer:      make(map[int]func(types.SerializerStats)),
		metadataNetwork: make(map[int]func(types.NetworkStats)),
		parralelism:     make(map[int]func(types.ParallelismStats)),
	}
}

func (s *stats) RegisterMetadataNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.metadataNetwork[s.index] = f
	index := s.index
	s.index++

	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		delete(s.metadataNetwork, index)
	}
}

func (s *stats) RegisterSeriesNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Unlock()

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

func (s *stats) RegisterParallelism(f func(types.ParallelismStats)) types.NotificationRelease {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.parralelism[s.index] = f
	index := s.index
	s.index++

	return func() {
		s.mut.Lock()
		defer s.mut.Unlock()

		delete(s.parralelism, index)
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
	defer s.mut.RUnlock()

	for _, v := range s.metadataNetwork {
		v(st)
	}
}

func (s *stats) SendParallelismStats(st types.ParallelismStats) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	for _, v := range s.parralelism {
		v(st)
	}
}
