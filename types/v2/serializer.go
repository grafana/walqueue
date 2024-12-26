package v2

import (
	"sync"
	"unsafe"

	"github.com/dolthub/swiss"
	"github.com/grafana/walqueue/types"
)

var bufPool = sync.Pool{New: func() interface{} {
	return make([]byte, 0)
}}

var sgPool = sync.Pool{New: func() interface{} {
	return &SeriesGroup{}
}}

func getSeriesGroup() *SeriesGroup {
	return sgPool.Get().(*SeriesGroup)
}

func putSeriesGroup(sg *SeriesGroup) {
	sg.Series = sg.Series[:0]
	sg.Metadata = sg.Metadata[:0]
	sg.Strings = sg.Strings[:0]

	sgPool.Put(sg)
}

type Serialization struct{}

func GetSerializer() types.Serialization {
	return &Serialization{}
}

func (s *Serialization) Serialize(metrics *types.Metrics, metadata *types.Metrics, handler func([]byte)) error {
	sg := getSeriesGroup()
	defer putSeriesGroup(sg)

	if metrics == nil {
		metrics = &types.Metrics{M: make([]*types.Metric, 0)}
	}
	if metadata == nil {
		metadata = &types.Metrics{M: make([]*types.Metric, 0)}
	}

	if cap(sg.Series) < len(metrics.M) {
		sg.Series = make([]*TimeSeriesBinary, len(metrics.M))
		for i := range sg.Series {
			sg.Series[i] = &TimeSeriesBinary{}
		}
	} else {
		sg.Series = sg.Series[:len(metrics.M)]
	}

	if cap(sg.Metadata) < len(metadata.M) {
		sg.Metadata = make([]*TimeSeriesBinary, len(metadata.M))
		for i := 0; i < len(metadata.M); i++ {
			sg.Metadata[i] = &TimeSeriesBinary{}
		}
	} else {
		sg.Metadata = sg.Metadata[:len(metadata.M)]
	}
	// Swissmap was a 25% improvement over normal maps. At some point go will adopt swiss maps and this
	// might be able to be removed.
	strMapToIndex := swiss.NewMap[string, uint32](uint32((len(metrics.M) + len(metadata.M)) * 10))

	for index, m := range metrics.M {
		sg.Series[index] = fillTimeSeries(sg.Series[index], m, strMapToIndex)
	}
	for index, m := range metadata.M {
		sg.Metadata[index] = fillTimeSeries(sg.Metadata[index], m, strMapToIndex)
	}
	if cap(sg.Strings) < strMapToIndex.Count() {
		sg.Strings = make([]ByteString, strMapToIndex.Count())
	} else {
		sg.Strings = sg.Strings[:strMapToIndex.Count()]
	}
	strMapToIndex.Iter(func(k string, v uint32) (stop bool) {
		d := unsafe.StringData(k)
		sg.Strings[v] = unsafe.Slice(d, len(k))
		return false
	})

	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	buf, err := sg.MarshalMsg(buf)
	if err != nil {
		return err
	}
	// Pass the buffer in with the knowledge that it should not be reused.
	handler(buf)
	return nil
}

func (s *Serialization) Deserialize(bytes []byte) (*types.Metrics, *types.Metrics, error) {
	sg := getSeriesGroup()
	defer putSeriesGroup(sg)
	return DeserializeToSeriesGroup(sg, bytes)
}
