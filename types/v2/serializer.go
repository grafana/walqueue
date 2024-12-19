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

func (s *Serialization) Serialize(metrics []*types.Metric, metadata []*types.Metric, handler func([]byte)) error {
	sg := getSeriesGroup()
	defer putSeriesGroup(sg)

	if cap(sg.Series) < len(metrics) {
		sg.Series = make([]*TimeSeriesBinary, len(metrics))
	} else {
		sg.Series = sg.Series[:len(metrics)]
	}

	if cap(sg.Metadata) < len(metadata) {
		sg.Metadata = make([]*TimeSeriesBinary, len(metadata))
	} else {
		sg.Metadata = sg.Metadata[:len(metadata)]
	}
	// Swissmap was a 25% improvement over normal maps. At some point go will adopt swiss maps and this
	// might be able to be removed.
	strMapToIndex := swiss.NewMap[string, uint32](uint32((len(metrics) + len(metadata)) * 10))

	for index, m := range metrics {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Series[index] = ts
	}
	for index, m := range metadata {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Metadata[index] = ts
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
	handler(buf)
	return nil
}

func (s *Serialization) Deserialize(bytes []byte) ([]*types.Metric, []*types.Metric, error) {
	sg := getSeriesGroup()
	defer putSeriesGroup(sg)
	return DeserializeToSeriesGroup(sg, bytes)
}
