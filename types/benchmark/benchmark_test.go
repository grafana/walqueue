package benchmark

import (
	"fmt"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	v1 "github.com/grafana/walqueue/types/v1"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkDeserializeAndSerialize(b *testing.B) {
	lbls := make(labels.Labels, 0)
	for i := 0; i < 10; i++ {
		lbls = append(lbls, labels.Label{
			Name:  fmt.Sprintf("label_%d", i),
			Value: randString(),
		})
	}
	b.ResetTimer()
	type test struct {
		s    types.PrometheusSerialization
		name string
	}
	tests := []test{
		{
			// The issue the large size in V1 is the fact I messed up and used string keys (the default) instead of
			// tuple/index based.
			name: "v1",
			s:    v1.GetSerializer(),
		},
		{
			name: "v2",
			s:    v2.NewSerialization(),
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(t *testing.B) {
			for n := 0; n < t.N; n++ {
				s := tt.s

				for i := 0; i < 10_000; i++ {
					aErr := s.AddPrometheusMetric(time.Now().UnixMilli(), rand.Float64(), lbls, nil, nil, nil)
					require.NoError(t, aErr)
				}
				kv := make(map[string]string)
				var bb []byte
				err := s.Serialize(func(meta map[string]string, buf []byte) error {
					bb = make([]byte, len(buf))
					copy(bb, buf)
					kv = meta
					return nil
				})
				require.NoError(t, err)
				compressed := snappy.Encode(nil, bb)
				t.ReportMetric(float64(len(bb)/1024), "uncompressed_KB")
				t.ReportMetric(float64(len(compressed)/1024), "compressed_KB")

				uncompressed, err := snappy.Decode(nil, compressed)
				require.NoError(t, err)
				items, _ := s.Deserialize(kv, uncompressed)
				for _, item := range items {
					item.Free()
				}
			}
		})
	}

}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString() string {
	b := make([]rune, rand.Intn(20))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
