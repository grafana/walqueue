package v2

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/walqueue/types"
)

func TestDeserializeAndSerialize_Metric(t *testing.T) {
	s := NewFormat()
	ts := time.Now().UnixMilli()
	ex := testExemplar()
	value := testValue()
	lbls := testLabels()
	externalLabels := testExternalLabels()

	for i := 0; i < 100; i++ {
		aErr := s.AddPrometheusMetric(ts, value, lbls, nil, nil, ex, externalLabels)
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

	expectedLabels := make([]labels.Label, 0, lbls.Len()+externalLabels.Len())
	lbls.Range(func(l labels.Label) {
		expectedLabels = append(expectedLabels, l)
	})
	externalLabels.Range(func(l labels.Label) {
		expectedLabels = append(expectedLabels, l)
	})

	require.NoError(t, err)
	items, err := s.Unmarshal(kv, bb)
	require.NoError(t, err)
	require.Len(t, items, 200)
	for _, item := range items {
		ppb := item.Bytes()
		if item.Type() == types.PrometheusMetricV1 {
			md, ok := item.(types.MetricDatum)
			require.True(t, ok, "expected item to be of type MetricDatum")
			require.Equal(t, ts, md.TimeStampMS(), "MetricDataum timestamp should match the original timestamp")

			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			require.Len(t, met.Labels, len(expectedLabels))
			for j, l := range met.Labels {
				assert.Equal(t, l.Name, expectedLabels[j].Name)
				assert.Equal(t, l.Value, expectedLabels[j].Value)
			}

			require.Len(t, met.Samples, 1)
			assert.Equal(t, value, met.Samples[0].Value)
			assert.Equal(t, ts, met.Samples[0].Timestamp)

			require.Len(t, met.Exemplars, 1)
			assert.Len(t, met.Exemplars[0].Labels, 1)
			var lname, lvalue string
			ex.Labels.Range(func(l labels.Label) {
				lname = l.Name
				lvalue = l.Value
			})
			assert.Equal(t, lname, met.Exemplars[0].Labels[0].Name)
			assert.Equal(t, lvalue, met.Exemplars[0].Labels[0].Value)
			assert.Equal(t, ex.Ts, met.Exemplars[0].Timestamp)
			assert.Equal(t, ex.Value, met.Exemplars[0].Value)
		}
		if item.Type() == types.PrometheusMetadataV1 {
			metadataDatum, ok := item.(types.MetadataDatum)
			require.True(t, ok, "expected item to be of type MetadataDatum")
			require.True(t, metadataDatum.IsMeta(), "expected item to be a metadata datum marked with IsMeta() true")

			md := &prompb.MetricMetadata{}
			unErr := md.Unmarshal(ppb)
			require.NoError(t, unErr)
			require.True(t, md.Type == prompb.MetricMetadata_GAUGE)
			require.True(t, md.Help == "help")
			require.True(t, md.Unit == "unit")
			require.True(t, md.MetricFamilyName == "name")
		}
	}

	// Uncomment to write the binary file for compatibility testing. This should only be done if the test data changes.
	//f, err := os.Create("testdata/v2_metric.bin")
	//require.NoError(t, err)
	//defer f.Close()
	//n, err := f.Write(bb)
	//require.NoError(t, err)
	//require.Equal(t, len(bb), n)
}

func TestExternalLabels(t *testing.T) {
	externalLabels := labels.FromStrings(
		"bar", "",
		"foo", "bar",
		"label_0", "skipped",
	)
	s := NewFormat()
	labelBuilder := labels.NewScratchBuilder(0)

	for i := range 10 {
		labelBuilder.Add(fmt.Sprintf("label_%d", i), randString())
	}

	labelBuilder.Sort()
	lbls := labelBuilder.Labels()
	for i := range 100 {
		// Only pass in i%10 labels to ensure that when the label size reduces duplicate labels are not added.
		// This is to confirm a regression that occurred when external labels were not correctly iterated over when reusing label buffers which caused duplicates.

		// create a new labels instance containing lbls[:(10-i%10)]
		builder := labels.NewScratchBuilder(10 - i%10)
		count := 0
		lbls.Range(func(l labels.Label) {
			if count < (10 - i%10) {
				builder.Add(l.Name, l.Value)
				count++
			}
		})
		lblsSubset := builder.Labels()
		aErr := s.AddPrometheusMetric(time.Now().UnixMilli(), rand.Float64(), lblsSubset, nil, nil, exemplar.Exemplar{}, externalLabels)
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
	for i, item := range items {
		ppb := item.Bytes()
		if item.Type() == types.PrometheusMetricV1 {
			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			// Length should be 2 (external - duplicate) + (10 - i%10)
			expectLen := 2 + (10 - i%10)
			require.Len(t, met.Labels, expectLen)
			t.Log(met.Labels)

			count := 0
			lbls.Range(func(l labels.Label) {
				if count < expectLen-2 {
					require.Equal(t, l.Name, met.Labels[count].Name)
					require.Equal(t, l.Value, met.Labels[count].Value)
					count++
				}
			})

			expectedSubset := make([]labels.Label, 0, expectLen-2)
			count = 0
			lbls.Range(func(l labels.Label) {
				if count < expectLen-2 {
					expectedSubset = append(expectedSubset, l)
					count++
				}
			})
			for j, l := range expectedSubset {
				require.Equal(t, l.Name, met.Labels[j].Name)
				require.Equal(t, l.Value, met.Labels[j].Value)
			}
			require.Equal(t, met.Labels[expectLen-2].Name, "bar")
			require.Equal(t, met.Labels[expectLen-2].Value, "")
			require.Equal(t, met.Labels[expectLen-1].Name, "foo")
			require.Equal(t, met.Labels[expectLen-1].Value, "bar")
		}
	}
}

func TestBackwardsCompatability_Metric(t *testing.T) {
	buf, err := os.ReadFile(filepath.Join("testdata", "v2_metric.bin"))
	require.NoError(t, err)
	sg := NewFormat()
	metrics, err := sg.Unmarshal(map[string]string{"record_count": "200"}, buf)
	require.NoError(t, err)
	require.Len(t, metrics, 200)

	lbls := testLabels()
	externalLabels := testExternalLabels()
	expectedLabels := make([]labels.Label, 0, lbls.Len()+externalLabels.Len())
	lbls.Range(func(l labels.Label) { expectedLabels = append(expectedLabels, l) })
	externalLabels.Range(func(l labels.Label) { expectedLabels = append(expectedLabels, l) })
	now := time.Now().UnixMilli()
	ex := testExemplar()

	for _, item := range metrics {
		ppb := item.Bytes()
		require.True(t, item.FileFormat() == types.AlloyFileVersionV2)
		if item.Type() == types.PrometheusMetricV1 {
			md, ok := item.(types.MetricDatum)
			require.True(t, ok, "expected item to be of type MetricDatum")
			require.Greater(t, now, md.TimeStampMS(), "timestamp should be in the past for persisted data")

			met := &prompb.TimeSeries{}
			unErr := met.Unmarshal(ppb)
			require.NoError(t, unErr)

			require.Len(t, met.Labels, len(expectedLabels))
			for j, l := range met.Labels {
				assert.Equal(t, l.Name, expectedLabels[j].Name)
				assert.Equal(t, l.Value, expectedLabels[j].Value)
			}

			require.Len(t, met.Samples, 1)
			assert.Equal(t, testValue(), met.Samples[0].Value)
			assert.GreaterOrEqual(t, now, met.Samples[0].Timestamp, "sample timestamp for persisted data should not be large than now")

			require.Len(t, met.Exemplars, 1)
			assert.Len(t, met.Exemplars[0].Labels, 1)
			var lname, lvalue string
			ex.Labels.Range(func(l labels.Label) { lname = l.Name; lvalue = l.Value })
			assert.Equal(t, lname, met.Exemplars[0].Labels[0].Name)
			assert.Equal(t, lvalue, met.Exemplars[0].Labels[0].Value)
			assert.Greater(t, now, met.Exemplars[0].Timestamp, "exemplar timestamp for persisted data should not be larger than now")
			assert.Equal(t, ex.Value, met.Exemplars[0].Value)
		}
		if item.Type() == types.PrometheusMetadataV1 {
			metadataDatum, ok := item.(types.MetadataDatum)
			require.True(t, ok, "expected item to be of type MetadataDatum")
			require.True(t, metadataDatum.IsMeta(), "expected item to be a metadata datum marked with IsMeta() true")

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

// It's important for backwards compatibility testing that this value remain consistent. If you change it, you must
// regenerate the testdata file.
func testValue() float64 {
	return 10.0
}

// It's important for backwards compatibility testing that this value remain consistent. If you change it, you must
// regenerate the testdata file.
func testLabels() labels.Labels {
	builder := labels.NewScratchBuilder(10)
	for i := range 10 {
		builder.Add(fmt.Sprintf("label_%d", i), strconv.Itoa(i))
	}
	builder.Sort()
	return builder.Labels()
}

// It's important for backwards compatibility testing that this value remain consistent. If you change it, you must
// regenerate the testdata file.
func testExternalLabels() labels.Labels {
	builder := labels.NewScratchBuilder(3)
	for i := range 3 {
		builder.Add(fmt.Sprintf("external_%d", i), strconv.Itoa(i))
	}

	builder.Sort()
	return builder.Labels()
}

// It's important for backwards compatibility testing that this value remain consistent. If you change it, you must
// regenerate the testdata file.
func testExemplar() exemplar.Exemplar {
	return exemplar.Exemplar{
		Labels: labels.FromStrings("name_1", "value_1"),
		Ts:     time.Now().UnixMilli(),
		HasTs:  true,
		Value:  float64(10),
	}
}
