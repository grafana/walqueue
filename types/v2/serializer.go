package v2

import (
	"bytes"
	"sync"

	"github.com/grafana/walqueue/types"
	"github.com/mus-format/mus-go/raw"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

type Serialization struct {
	buf *bytes.Buffer
}

const PrometheusMetric = uint8(1)

func (s *Serialization) AddPrometheusMetric(ts int64, value float64, lbls labels.Labels) error {
	series := prompb.TimeSeries{}
	series.Labels = make([]prompb.Label, len(lbls))
	series.Samples = make([]prompb.Sample, 1)
	series.Samples[0].Value = value
	series.Samples[0].Timestamp = ts
	for i, l := range lbls {
		series.Labels[i].Name = l.Name
		series.Labels[i].Value = l.Value
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
	return err
}

func (Serialization) Deserialize(_ []byte) (items []types.Datum, err error) {
	panic("not implemented") // TODO: Implement
}
func (Serialization) Count() (_ int) {
	panic("not implemented") // TODO: Implement
}
func (Serialization) Serialize(handle func([]byte)) {
	panic("not implemented") // TODO: Implement
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
