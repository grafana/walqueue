package v2

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestDeserializeAndSerialize(t *testing.T) {
	s := NewFormat()
	lbls := make(labels.Labels, 0)
	for i := 0; i < 10; i++ {
		lbls = append(lbls, labels.Label{
			Name:  fmt.Sprintf("label_%d", i),
			Value: randString(),
		})
	}
	for i := 0; i < 100; i++ {
		aErr := s.AddPrometheusMetric(time.Now().UnixMilli(), rand.Float64(), lbls, nil, nil, exemplar.Exemplar{}, nil)
		require.NoError(t, aErr)
		aErr = s.AddPrometheusMetadata("name", "unit", "help", "gauge")
		require.NoError(t, aErr)
	}
	kv := make(map[string]string)
	var bb []byte
	err := s.Marshal(func(meta map[string]string, buf []byte) error {
		bb = buf
		kv = meta
		return nil
	})
	require.NoError(t, err)
	items, err := s.Unmarshal(kv, bb)
	require.NoError(t, err)
	require.Len(t, items, 200)
	for _, item := range items {
		ppb := item.Bytes()
		if item.Type() == types.PrometheusMetricV1 {
			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			require.Len(t, met.Labels, 10)
			for j, l := range met.Labels {
				require.Equal(t, l.Name, lbls[j].Name)
				require.Equal(t, l.Value, lbls[j].Value)
			}
		}
		if item.Type() == types.PrometheusMetadataV1 {
			md := &prompb.MetricMetadata{}
			unErr := md.Unmarshal(ppb)
			require.NoError(t, unErr)
			require.True(t, md.Type == prompb.MetricMetadata_GAUGE)
			require.True(t, md.Help == "help")
			require.True(t, md.Unit == "unit")
			require.True(t, md.MetricFamilyName == "name")
		}
	}
}

func TestExternalLabels(t *testing.T) {
	externalLabels := map[string]string{
		"foo":     "bar",
		"label_1": "bad_value",
	}
	s := NewFormat()
	lbls := make(labels.Labels, 0)
	for i := 0; i < 10; i++ {
		lbls = append(lbls, labels.Label{
			Name:  fmt.Sprintf("label_%d", i),
			Value: randString(),
		})
	}
	for i := 0; i < 100; i++ {
		aErr := s.AddPrometheusMetric(time.Now().UnixMilli(), rand.Float64(), lbls, nil, nil, exemplar.Exemplar{}, externalLabels)
		require.NoError(t, aErr)
	}
	kv := make(map[string]string)
	var bb []byte
	err := s.Marshal(func(meta map[string]string, buf []byte) error {
		bb = buf
		kv = meta
		return nil
	})
	require.NoError(t, err)
	items, err := s.Unmarshal(kv, bb)
	require.NoError(t, err)
	require.Len(t, items, 100)
	for _, item := range items {
		ppb := item.Bytes()
		if item.Type() == types.PrometheusMetricV1 {
			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			require.Len(t, met.Labels, 11)
			for j, l := range lbls {
				require.Equal(t, l.Name, met.Labels[j].Name)
				require.Equal(t, l.Value, met.Labels[j].Value)
			}
			require.True(t, met.Labels[10].Name == "foo")
			require.True(t, met.Labels[10].Value == "bar")
		}
	}
}

func TestBackwardsCompatability(t *testing.T) {
	buf, err := os.ReadFile(filepath.Join("testdata", "v2.bin"))
	require.NoError(t, err)
	sg := NewFormat()
	metrics, err := sg.Unmarshal(map[string]string{"record_count": "200"}, buf)
	require.NoError(t, err)
	require.Len(t, metrics, 200)
	for _, item := range metrics {
		ppb := item.Bytes()
		require.True(t, item.FileFormat() == types.AlloyFileVersionV2)
		if item.Type() == types.PrometheusMetricV1 {
			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			require.Len(t, met.Labels, 10)
			for _, l := range met.Labels {
				require.True(t, strings.HasPrefix(l.Name, "label"))
			}
		}
		if item.Type() == types.PrometheusMetadataV1 {
			md := &prompb.MetricMetadata{}
			unErr := md.Unmarshal(ppb)
			require.NoError(t, unErr)
			require.True(t, md.Type == prompb.MetricMetadata_GAUGE)
			require.True(t, md.Help == "help")
			require.True(t, md.Unit == "unit")
			require.True(t, md.MetricFamilyName == "name")
		}
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
