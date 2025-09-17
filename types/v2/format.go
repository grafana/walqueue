package v2

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/walqueue/types"
	"github.com/grafana/walqueue/util"
)

// Format describe the v2 data format.
type Format struct {
	series      *prompb.TimeSeries
	buf         *bytes.Buffer
	metric      *Metric
	seriesBuf   []byte
	metricBuf   []byte
	recordCount uint32
}

const PrometheusMetric = uint8(1)
const PrometheusExemplar = uint8(2)
const PrometheusMetadata = uint8(3)

func NewFormat() *Format {
	return &Format{
		buf: bytes.NewBuffer(nil),
		series: &prompb.TimeSeries{
			Samples:   make([]prompb.Sample, 0),
			Exemplars: make([]prompb.Exemplar, 0),
		},
		metric:    &Metric{},
		metricBuf: make([]byte, 0),
		seriesBuf: make([]byte, 0),
	}
}

// AddPrometheusMetric marshals a prometheus metric to its prombpb.TimeSeries representation and writes it to the buffer.
func (s *Format) AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, e exemplar.Exemplar, externalLabels labels.Labels) error {
	defer func() {
		s.series.Labels = s.series.Labels[:0]
		s.series.Samples = s.series.Samples[:0]
		s.series.Exemplars = s.series.Exemplars[:0]
		s.series.Histograms = s.series.Histograms[:0]
		s.seriesBuf = s.seriesBuf[:0]
		s.metricBuf = s.metricBuf[:0]
		s.series.Exemplars = s.series.Exemplars[:0]
	}()
	// Need to find any similar labels, if there is overlap.
	totalLabels := len(lbls)
	for _, lbl := range externalLabels {
		if !lbls.Has(lbl.Name) {
			totalLabels += 1
		}
	}

	if cap(s.series.Labels) < totalLabels {
		s.series.Labels = make([]prompb.Label, totalLabels)
	} else {
		s.series.Labels = s.series.Labels[:totalLabels]
	}
	lblIndex := 0
	for _, l := range lbls {
		s.series.Labels[lblIndex].Name = l.Name
		s.series.Labels[lblIndex].Value = l.Value
		lblIndex++
	}
	// Technically external labels do NOT apply to the hash but since the rule is applied evenly it works out.
	// This works because external labels do not override metric labels, and the actual hash is not sent.
	for _, lbl := range externalLabels {
		if lbls.Has(lbl.Name) {
			continue
		}
		s.series.Labels[lblIndex].Name = lbl.Name
		s.series.Labels[lblIndex].Value = lbl.Value
		lblIndex++
	}

	// A series can either be a Sample (normal metric) or a Histogram (histogram metric).
	if h == nil && fh == nil {
		if cap(s.series.Samples) == 0 {
			s.series.Samples = make([]prompb.Sample, 1)
		} else {
			s.series.Samples = s.series.Samples[:1]
		}
		s.series.Samples[0].Value = value
		s.series.Samples[0].Timestamp = ts
	}
	var isHistogram bool
	if h != nil || fh != nil {
		isHistogram = true
		s.series.Histograms = make([]prompb.Histogram, 1)
		// These FromIntHistograms and FromFloatHistograms were copied from prometheus because Alloy custom fork does not have them yet.
		if h != nil {
			s.series.Histograms[0] = FromIntHistogram(ts, h)
		}
		if fh != nil {
			s.series.Histograms[0] = FromFloatHistogram(ts, fh)
		}
	}
	if e.Labels.Len() > 0 {
		// TODO could probably check cap here instead.
		if len(s.series.Exemplars) == 0 {
			s.series.Exemplars = make([]prompb.Exemplar, 1)
		}
		s.series.Exemplars[0].Value = e.Value
		s.series.Exemplars[0].Timestamp = e.Ts
		s.series.Exemplars[0].Labels = make([]prompb.Label, 0, len(e.Labels))
		for _, v := range e.Labels {
			s.series.Exemplars[0].Labels = append(s.series.Exemplars[0].Labels, prompb.Label{Name: v.Name, Value: v.Value})
		}
	}

	// Figure out the size of the series so we can allocate a big enough buffer.
	seriesSize := s.series.Size()
	if cap(s.seriesBuf) < seriesSize {
		s.seriesBuf = make([]byte, seriesSize)
	} else {
		s.seriesBuf = s.seriesBuf[:seriesSize]
	}
	_, err := s.series.MarshalToSizedBuffer(s.seriesBuf[:seriesSize])
	if err != nil {
		return err
	}

	// Finally fill in our working metric datum.
	s.metric.Hashvalue = util.HashForSharding(lbls)
	s.metric.IsHistogramvalue = isHistogram
	s.metric.Timestampmsvalue = ts
	s.metric.Buf = s.seriesBuf

	metricSize := s.metric.Size()

	if cap(s.metricBuf) < metricSize {
		s.metricBuf = make([]byte, metricSize)
	} else {
		s.metricBuf = s.metricBuf[:metricSize]
	}
	// Marshal the datum.
	s.metric.Marshal(s.metricBuf)
	// Write out the record type.
	err = s.buf.WriteByte(PrometheusMetric)
	if err != nil {
		return err
	}
	_, err = s.buf.Write(s.metricBuf)
	if err != nil {
		return err
	}
	s.recordCount++
	return nil
}

func (s *Format) AddPrometheusMetadata(name string, unit string, help string, pType string) error {
	theType := FromMetadataType(model.MetricType(pType))
	md := &prompb.MetricMetadata{
		Type:             theType,
		MetricFamilyName: name,
		Help:             help,
		Unit:             unit,
	}

	bb, err := md.Marshal()
	if err != nil {
		return err
	}

	mdd := &Metadata{Buf: bb}
	size := mdd.Size()
	buf := make([]byte, size)
	mdd.Marshal(buf)

	s.buf.WriteByte(PrometheusMetadata)
	s.buf.Write(buf)

	s.recordCount++
	return nil
}

func (s *Format) Unmarshal(meta map[string]string, buf []byte) (items []types.Datum, err error) {
	strCount, found := meta["record_count"]
	if !found {
		return nil, fmt.Errorf("missing record count")
	}
	seriesCount, err := strconv.Atoi(strCount)
	if err != nil {
		return nil, err
	}
	datums := make([]types.Datum, seriesCount)
	index := 0
	for i := range datums {
		recordType := buf[index]
		index++
		switch recordType {
		case PrometheusMetric:
			// These are pooled for performance. Whenever they are no longer needed they are returned to the pool via the Free method.
			m := metricPool.Get().(*Metric)
			// The []uint16 allows some backwards compatibility but we dont care about that.
			size, err := m.NestedUnmarshal(0, buf[index:], []uint16{}, 0)
			if err != nil {
				return nil, err
			}
			index += size

			datums[i] = m
		case PrometheusMetadata:
			md := &Metadata{}
			size, err := md.NestedUnmarshal(0, buf[index:], []uint16{}, 0)
			if err != nil {
				return nil, err
			}
			index += size
			datums[i] = md
		}
	}
	return datums, nil
}

func (s *Format) Marshal(handle func(map[string]string, []byte) error) error {
	defer func() {
		s.buf.Reset()
		s.recordCount = 0
	}()
	buf := s.buf.Bytes()
	meta := make(map[string]string)
	meta["record_count"] = strconv.Itoa(int(s.recordCount))
	return handle(meta, buf)
}

var metricPool = sync.Pool{
	New: func() any {
		return &Metric{}
	},
}
