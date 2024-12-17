package v2

import (
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/atomic"
	"sync"
	"unique"
	"unsafe"
)

func (v *ByteString) UnmarshalMsg(bts []byte) (o []byte, err error) {
	*v, o, err = msgp.ReadStringZC(bts)
	return o, err
}

func (v *ByteString) MarshalMsg(bts []byte) (o []byte, err error) {
	buf := msgp.AppendString(bts, v.String())
	return buf, nil
}

func (v *ByteString) Msgsize() int {
	return msgp.StringPrefixSize + len(*v)
}

func (v *ByteString) DecodeMsg(dc *msgp.Reader) (err error) {
	s, err := dc.ReadStringAsBytes(nil)
	if err != nil {
		return err
	}
	*v = s
	return nil
}

func (v *ByteString) EncodeMsg(en *msgp.Writer) (err error) {
	return en.WriteString(v.String())
}

func (v ByteString) String() string {
	if len([]byte(v)) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len([]byte(v)))
}

type Serialization struct{}

func GetSerializer() types.Serialization {
	return &Serialization{}
}

func (s *Serialization) Serialize(metrics []*types.Metric, metadata []*types.Metric) ([]byte, error) {
	sg := SeriesGroup{}
	buf := make([]byte, 0)
	sg.Series = make([]*TimeSeriesBinary, 0, len(metrics))
	sg.Metadata = make([]*TimeSeriesBinary, 0, len(metadata))

	strMapToIndex := make(map[string]uint32, (len(metrics)+len(metadata))*10)
	for _, m := range metrics {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Series = append(sg.Series, ts)
	}
	for _, m := range metadata {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Metadata = append(sg.Metadata, ts)
	}
	stringsSlice := make([]ByteString, len(strMapToIndex))
	for stringValue, index := range strMapToIndex {
		d := unsafe.StringData(stringValue)
		stringsSlice[index] = unsafe.Slice(d, len(stringValue))
	}
	sg.Strings = stringsSlice
	return sg.MarshalMsg(buf)
}

func (s *Serialization) Deserialize(bytes []byte) ([]*types.Metric, []*types.Metric, []byte, error) {
	sg := &SeriesGroup{}
	return DeserializeToSeriesGroup(sg, bytes)
}

// createTimeSeries is what does the conversion from labels.Labels to LabelNames and
// LabelValues while filling in the string map, that is later converted to []string.
func createTimeSeries(m *types.Metric, strMapToInt map[string]uint32) *TimeSeriesBinary {
	ts := getTimeSeriesFromPool()
	ts.LabelsNames = setSliceLength(ts.LabelsNames, len(m.Labels))
	ts.LabelsValues = setSliceLength(ts.LabelsValues, len(m.Labels))

	// This is where we deduplicate the ts.Labels into uint32 values
	// that map to a string in the strings slice via the index.
	for i, v := range m.Labels {
		val, found := strMapToInt[v.Name]
		if !found {
			val = uint32(len(strMapToInt))
			strMapToInt[v.Name] = val
		}
		ts.LabelsNames[i] = val

		val, found = strMapToInt[v.Value]
		if !found {
			val = uint32(len(strMapToInt))
			strMapToInt[v.Value] = val
		}
		ts.LabelsValues[i] = val
	}
	return ts
}

func setSliceLength(lbls []uint32, length int) []uint32 {
	if cap(lbls) <= length {
		lbls = make([]uint32, length)
	} else {
		lbls = lbls[:length]
	}
	return lbls
}

var tsBinaryPool = sync.Pool{
	New: func() any {
		return &TimeSeriesBinary{}
	},
}

func getTimeSeriesFromPool() *TimeSeriesBinary {
	OutStandingTimeSeriesBinary.Inc()
	return tsBinaryPool.Get().(*TimeSeriesBinary)
}

type LabelHandle struct {
	Name  unique.Handle[string]
	Value unique.Handle[string]
}

type LabelHandles []LabelHandle

var OutStandingTimeSeriesBinary = atomic.Int32{}

func putTimeSeriesSliceIntoPool(tss []*TimeSeriesBinary) {
	for i := 0; i < len(tss); i++ {
		PutTimeSeriesIntoPool(tss[i])
	}

}

func PutTimeSeriesIntoPool(ts *TimeSeriesBinary) {
	OutStandingTimeSeriesBinary.Dec()
	ts.LabelsNames = ts.LabelsNames[:0]
	ts.LabelsValues = ts.LabelsValues[:0]
	ts.TS = 0
	ts.Value = 0
	ts.Hash = 0
	if ts.Histograms != nil {
		ts.Histograms.Histogram = nil
		ts.Histograms.FloatHistogram = nil
	}

	tsBinaryPool.Put(ts)
}

// DeserializeToSeriesGroup transforms a buffer to a SeriesGroup and converts the stringmap + indexes into actual Labels.
func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) ([]*types.Metric, []*types.Metric, []byte, error) {
	buf, err := sg.UnmarshalMsg(buf)
	if err != nil {
		return nil, nil, nil, err
	}
	metrics := types.GetMetricsFromPool()
	if cap(metrics) < len(sg.Series) {
		metrics = make([]*types.Metric, len(sg.Series))
		for i, _ := range metrics {
			metrics[i] = &types.Metric{}
		}
	} else {
		metrics = metrics[:len(sg.Series)]
	}
	// Need to fill in the labels.
	for seriesIndex, series := range sg.Series {
		metric := metrics[seriesIndex]
		if cap(metric.Labels) < len(series.LabelsNames) {
			metric.Labels = make(labels.Labels, len(series.LabelsNames))
		} else {
			metric.Labels = metric.Labels[:len(series.LabelsNames)]
		}
		// Since the LabelNames/LabelValues are indexes into the Strings slice we can access it like the below.
		// 1 Label corresponds to two entries, one in LabelsNames and one in LabelsValues.
		for i := range series.LabelsNames {
			metric.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]].String(),
				Value: sg.Strings[series.LabelsValues[i]].String(),
			}
		}
		series.LabelsNames = series.LabelsNames[:0]
		series.LabelsValues = series.LabelsValues[:0]
	}

	metadata := types.GetMetricsFromPool()
	if cap(metadata) < len(sg.Metadata) {
		metadata = make([]*types.Metric, 0, len(sg.Metadata))
		for i, _ := range metrics {
			metadata[i] = &types.Metric{}
		}
	} else {
		metadata = metadata[:len(sg.Metadata)]
	}
	for seriesIndex, series := range sg.Metadata {
		meta := metadata[seriesIndex]

		if cap(meta.Labels) < len(series.LabelsNames) {
			meta.Labels = make([]labels.Label, len(series.LabelsNames))
		} else {
			meta.Labels = meta.Labels[:len(series.LabelsNames)]
		}
		for i := range series.LabelsNames {
			meta.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]].String(),
				Value: sg.Strings[series.LabelsValues[i]].String(),
			}
		}
		// Finally ensure we reset the labelnames and labelvalues.
		series.LabelsNames = series.LabelsNames[:0]
		series.LabelsValues = series.LabelsValues[:0]
	}
	sg.Strings = sg.Strings[:0]
	return metrics, metadata, buf, err
}
