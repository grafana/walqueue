package v2

import (
	"github.com/grafana/walqueue/types"
	"sync"
	"unsafe"
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

func (s *Serialization) Serialize(metrics []*types.Metric, metadata []*types.Metric) ([]byte, error) {
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

	strMapToIndex := make(map[string]uint32, (len(metrics)+len(metadata))*10)
	for index, m := range metrics {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Series[index] = ts
	}
	for index, m := range metadata {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Metadata[index] = ts
	}
	if cap(sg.Strings) < len(strMapToIndex) {
		sg.Strings = make([]ByteString, len(strMapToIndex))
	} else {
		sg.Strings = sg.Strings[:len(strMapToIndex)]
	}
	for stringValue, index := range strMapToIndex {
		d := unsafe.StringData(stringValue)
		sg.Strings[index] = unsafe.Slice(d, len(stringValue))
	}
	buf := bufPool.Get().([]byte)

	return sg.MarshalMsg(buf)
}

func (s *Serialization) Deserialize(bytes []byte) ([]*types.Metric, []*types.Metric, []byte, error) {
	sg := getSeriesGroup()
	defer putSeriesGroup(sg)
	return DeserializeToSeriesGroup(sg, bytes)
}
