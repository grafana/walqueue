package v2

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/grafana/walqueue/types"
	"github.com/mus-format/mus-go/raw"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

type Serialization struct {
	buf         *bytes.Buffer
	recordCount uint32
}

const PrometheusMetric = uint8(1)

func NewSerialization() types.Serialization {
	bb := bufPool.Get().(*bytes.Buffer)
	// Preallocate 4 bytes for number of records
	bb.Write(make([]byte, 4))
	return &Serialization{}
}

func (s *Serialization) AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram) error {
	series := prompb.TimeSeries{}
	series.Labels = make([]prompb.Label, len(lbls))
	for i, l := range lbls {
		series.Labels[i].Name = l.Name
		series.Labels[i].Value = l.Value
	}

	if h == nil && fh == nil {
		series.Samples = make([]prompb.Sample, 1)
		series.Samples[0].Value = value
		series.Samples[0].Timestamp = ts
	}
	if h != nil || fh != nil {
		series.Histograms = make([]prompb.Histogram, 1)
		if h != nil {
			series.Histograms[0] = prompb.FromIntHistogram(ts, h)
		}
		if fh != nil {
			series.Histograms[0] = prompb.FromFloatHistogram(ts, fh)
		}
	}
	size := 4 + 8 + 8 + 1 + series.Size() // Total Size + Timestamp + Hash + Type + Byte Represenation
	bbPtr := smallBB.Get().(*[]byte)
	buf := *bbPtr
	defer func() {
		buf = buf[:0]
		smallBB.Put(bbPtr)
	}()
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	index := 0
	raw.MarshalUint32(uint32(size), buf[index:4])
	index += 4
	// Generate hash, ts, type, then bytes
	hash := lbls.Hash()
	raw.MarshalInt64(ts, buf[index:8])
	index += 8
	raw.MarshalUint64(hash, buf[index:8])
	index += 8
	raw.MarshalUint8(PrometheusMetric, buf[index:1])
	index += 1
	series.MarshalTo(buf[index:])
	_, err := s.buf.Write(buf)
	s.recordCount++
	return err
}

func (Serialization) Deserialize(buf []byte) (items []types.Datum, err error) {
	panic("not implemented") // TODO: Implement
}

func (Serialization) Count() (_ int) {
	panic("not implemented") // TODO: Implement
}

func (s *Serialization) Serialize(handle func(map[string]string, []byte)) {
	defer func() {
		s.buf.Reset()
		bufPool.Put(s.buf)
	}()
	buf := s.buf.Bytes()
	// Since we preallocated 4 bytes at the beginning we can add the number of records there.
	raw.MarshalUint32(s.recordCount, buf[:4])
	meta := make(map[string]string)
	meta["series_count"] = strconv.Itoa(int(s.recordCount))
	handle(meta, buf)
}

func GetSerializer() types.Serialization {
	bb := bufPool.Get().(*bytes.Buffer)
	return &Serialization{buf: bb}
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
