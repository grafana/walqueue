package v2

import (
	"bytes"
	"fmt"
	"github.com/prometheus/common/model"
	"strconv"
	"sync"

	"github.com/grafana/walqueue/types"
	"github.com/mus-format/mus-go/raw"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

type Serialization struct {
	series      *prompb.TimeSeries
	meta        *prompb.MetricMetadata
	buf         *bytes.Buffer
	recordCount uint32
}

const PrometheusMetric = uint8(1)
const PrometheusExemplar = uint8(2)
const PrometheusMetadata = uint8(3)

func (s *Serialization) AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, externalLabels map[string]string) error {
	defer func() {
		s.series.Exemplars = s.series.Exemplars[:0]
		s.series.Samples = s.series.Samples[:0]
		s.series.Labels = s.series.Labels[:0]
		s.series.Histograms = nil
	}()
	// Need to find any similar labels.
	totalLabels := len(lbls)
	for k, _ := range externalLabels {
		if !lbls.Has(k) {
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
	for k, v := range externalLabels {
		if lbls.Has(k) {
			continue
		}
		s.series.Labels[lblIndex].Name = k
		s.series.Labels[lblIndex].Value = v
	}

	if h == nil && fh == nil {
		if cap(s.series.Samples) == 0 {
			s.series.Samples = make([]prompb.Sample, 1)
		} else {
			s.series.Samples = s.series.Samples[:1]
		}
		s.series.Samples[0].Value = value
		s.series.Samples[0].Timestamp = ts
	}
	var isHistogram int8
	if h != nil || fh != nil {
		isHistogram = 1
		s.series.Histograms = make([]prompb.Histogram, 1)
		if h != nil {
			s.series.Histograms[0] = FromIntHistogram(ts, h)
		}
		if fh != nil {
			s.series.Histograms[0] = FromFloatHistogram(ts, fh)
		}
	}
	size := 4 + 1 + 8 + 8 + 1 + s.series.Size() // Total Size + Type  + Timestamp + Hash + IsHistogram + Byte Representation
	bbPtr := smallBB.Get().(*[]byte)
	buf := *bbPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	defer func() {
		buf = buf[:0]
		*bbPtr = buf
		smallBB.Put(bbPtr)
	}()
	hash := lbls.Hash()

	index := 0
	index += raw.MarshalUint32(uint32(size), buf[index:])
	index += raw.MarshalUint8(PrometheusMetric, buf[index:])
	index += raw.MarshalInt64(ts, buf[index:])
	index += raw.MarshalUint64(hash, buf[index:])
	index += raw.MarshalInt8(isHistogram, buf[index:])

	_, err := s.series.MarshalTo(buf[index:])
	if err != nil {
		return err
	}
	_, err = s.buf.Write(buf)
	if err != nil {
		return err
	}
	s.recordCount++
	return nil
}

func (s *Serialization) AddPrometheusMetadata(name string, unit string, help string, pType string) error {
	theType := FromMetadataType(model.MetricType(pType))
	md := &prompb.MetricMetadata{
		Type:             theType,
		MetricFamilyName: name,
		Help:             help,
		Unit:             unit,
	}

	size := md.Size()

	index := 0
	// Total Size + Type + Buffer
	totalSize := 4 + 1 + size
	mdBuf := make([]byte, totalSize)
	raw.MarshalUint32(uint32(totalSize), mdBuf[index:])
	index += 4

	raw.MarshalUint8(PrometheusMetadata, mdBuf[index:])
	index += 1

	_, err := md.MarshalTo(mdBuf[index:])
	if err != nil {
		return err
	}
	_, err = s.buf.Write(mdBuf)
	if err != nil {
		return err
	}
	s.recordCount++
	return nil
}

func (s *Serialization) Deserialize(meta map[string]string, buf []byte) (items []types.Datum, err error) {
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
		size, s, err := raw.UnmarshalUint32(buf[index:])
		if err != nil {
			return nil, err
		}
		index += s
		recordType, s, err := raw.UnmarshalUint8(buf[index:])
		if err != nil {
			return nil, err
		}
		index += s
		if recordType == PrometheusMetric {
			t, s, err := raw.UnmarshalInt64(buf[index:])
			if err != nil {
				return nil, err
			}
			index += s

			hash, s, err := raw.UnmarshalUint64(buf[index:])
			if err != nil {
				return nil, err
			}
			index += s

			isHistogram, s, err := raw.UnmarshalInt8(buf[index:])
			if err != nil {
				return nil, err
			}
			index += s

			leftoverSize := size - 4 - 1 - 8 - 8 - 1
			bb := buf[index : index+int(leftoverSize)]
			index += int(leftoverSize)

			m := datumPool.Get().(*metric)
			m.hash = hash
			m.ts = t
			m.buf = bb
			m.isHistogram = isHistogram == 1
			datums[i] = m
		} else if recordType == PrometheusMetadata {
			leftoverSize := size - 4 - 1
			bb := buf[index : index+int(leftoverSize)]
			index += int(leftoverSize)
			md := &metadata{buf: bb}
			datums[i] = md
		}

	}
	return datums, nil
}

func (s *Serialization) Serialize(handle func(map[string]string, []byte) error) error {
	defer func() {
		s.buf.Reset()
		bufPool.Put(s.buf)
		s.recordCount = 0
	}()
	buf := s.buf.Bytes()
	meta := make(map[string]string)
	meta["record_count"] = strconv.Itoa(int(s.recordCount))
	return handle(meta, buf)
}

func NewSerialization() types.PrometheusSerialization {
	bb := bufPool.Get().(*bytes.Buffer)
	return &Serialization{
		buf: bb,
		series: &prompb.TimeSeries{
			Samples:   make([]prompb.Sample, 0),
			Exemplars: make([]prompb.Exemplar, 0),
		},
	}
}

var bufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

var smallBB = sync.Pool{
	New: func() any {
		bb := make([]byte, 0)
		return &bb
	},
}

var datumPool = sync.Pool{
	New: func() any {
		return &metric{}
	},
}
