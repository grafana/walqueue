//go:build !windows

package prometheus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestE2E(t *testing.T) {
	type e2eTest struct {
		name     string
		maker    func(index int, app storage.Appender) (float64, labels.Labels)
		tester   func(samples *safeSlice[prompb.TimeSeries])
		testMeta func(samples *safeSlice[prompb.MetricMetadata])
		skipV2   bool
	}
	tests := []e2eTest{
		{
			name: "normal",
			maker: func(index int, app storage.Appender) (float64, labels.Labels) {
				ts, v, lbls := makeSeries(index)
				_, errApp := app.Append(0, lbls, ts, v)
				require.NoError(t, errApp)
				return v, lbls
			},
			tester: func(samples *safeSlice[prompb.TimeSeries]) {
				t.Helper()
				for i := 0; i < samples.Len(); i++ {
					s := samples.Get(i)
					require.Equal(t, 1, len(s.Samples))
					require.True(t, s.Samples[0].Timestamp > 0)
					require.True(t, s.Samples[0].Value > 0)
					require.Equal(t, 1, len(s.Labels))
					require.Equal(t, s.Labels[0].Name, fmt.Sprintf("name_%d", int(s.Samples[0].Value)), "%d name %s", int(s.Samples[0].Value), s.Labels[0].Name)
					require.Equal(t, s.Labels[0].Value, fmt.Sprintf("value_%d", int(s.Samples[0].Value)))
				}
			},
		},
		{
			name: "exemplar",
			maker: func(index int, app storage.Appender) (float64, labels.Labels) {
				ts, v, lbls := makeSeries(index)
				_, errApp := app.Append(0, lbls, ts, v)
				require.NoError(t, errApp)
				_, errApp = app.AppendExemplar(0, lbls, exemplar.Exemplar{
					Labels: labels.FromStrings("trace_id", strconv.Itoa(index)),
					Value:  float64(index),
					Ts:     ts,
					HasTs:  true,
				})
				require.NoError(t, errApp)
				return v, lbls
			},
			tester: func(samples *safeSlice[prompb.TimeSeries]) {
				t.Helper()
				for i := 0; i < samples.Len(); i++ {
					s := samples.Get(i)
					require.True(t, len(s.Samples) == 1)
					require.True(t, s.Samples[0].Timestamp > 0)
					require.True(t, s.Samples[0].Value > 0)
					require.True(t, len(s.Labels) == 1)
					require.Truef(t, s.Labels[0].Name == fmt.Sprintf("name_%d", int(s.Samples[0].Value)), "%d name %s", int(s.Samples[0].Value), s.Labels[0].Name)
					require.True(t, s.Labels[0].Value == fmt.Sprintf("value_%d", int(s.Samples[0].Value)))

					require.True(t, len(s.Exemplars) == 1)
					require.True(t, s.Exemplars[0].Value > 0)
					require.True(t, s.Exemplars[0].Timestamp > 0)
					require.True(t, len(s.Exemplars[0].Labels) == 1)
					require.True(t, s.Exemplars[0].Labels[0].Name == "trace_id")
					// Value in no way has to be the same as the label trace id but this lets us verify everything is in order.
					require.True(t, s.Exemplars[0].Labels[0].Value == fmt.Sprintf("%d", int(s.Exemplars[0].Value)))
				}
			},
		},
		{
			name:   "metadata",
			skipV2: true,
			maker: func(index int, app storage.Appender) (float64, labels.Labels) {
				meta, lbls := makeMetadata(index)
				_, errApp := app.UpdateMetadata(0, lbls, meta)
				require.NoError(t, errApp)
				return 0, lbls
			},
			testMeta: func(samples *safeSlice[prompb.MetricMetadata]) {
				for i := 0; i < samples.Len(); i++ {
					s := samples.Get(i)
					require.True(t, s.GetUnit() == "seconds")
					require.True(t, s.Help == "metadata help")
					require.True(t, s.Unit == "seconds")
					require.True(t, s.Type == prompb.MetricMetadata_COUNTER)
					require.True(t, strings.HasPrefix(s.MetricFamilyName, "name_"))
				}
			},
		},
		{
			name: "histogram",
			maker: func(index int, app storage.Appender) (float64, labels.Labels) {
				ts, lbls, h := makeHistogram(index)
				_, errApp := app.AppendHistogram(0, lbls, ts, h, nil)
				require.NoError(t, errApp)
				return h.Sum, lbls
			},
			tester: func(samples *safeSlice[prompb.TimeSeries]) {
				t.Helper()
				for i := 0; i < samples.Len(); i++ {
					s := samples.Get(i)
					require.True(t, len(s.Samples) == 0)
					require.True(t, len(s.Labels) == 1)
					histSame(t, hist(int(s.Histograms[0].Sum)), s.Histograms[0])
				}
			},
		},
		{
			name: "float histogram",
			maker: func(index int, app storage.Appender) (float64, labels.Labels) {
				ts, lbls, h := makeFloatHistogram(index)
				_, errApp := app.AppendHistogram(0, lbls, ts, nil, h)
				require.NoError(t, errApp)
				return h.Sum, lbls
			},
			tester: func(samples *safeSlice[prompb.TimeSeries]) {
				t.Helper()
				for i := 0; i < samples.Len(); i++ {
					s := samples.Get(i)
					require.True(t, len(s.Samples) == 0)
					require.True(t, len(s.Labels) == 1)
					histFloatSame(t, histFloat(int(s.Histograms[0].Sum)), s.Histograms[0])
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s-%s", test.name, "PRWv1"), func(t *testing.T) {
			runTest(t, test.maker, test.tester, test.testMeta)
		})

		if !test.skipV2 {
			t.Run(fmt.Sprintf("%s-%s", test.name, "PRWv2"), func(t *testing.T) {
				runTestV2(t, test.maker, test.tester, test.testMeta)
			})
		}
	}
}

const (
	iterations = 10
	items      = 100
)

func runTest(t *testing.T, add func(index int, appendable storage.Appender) (float64, labels.Labels), test func(samples *safeSlice[prompb.TimeSeries]), metaTest func(meta *safeSlice[prompb.MetricMetadata])) {
	l := log.NewLogfmtLogger(os.Stdout)
	done := make(chan struct{})
	var series atomic.Int32
	var meta atomic.Int32
	samples := newSafeSlice[prompb.TimeSeries]()
	metaSamples := newSafeSlice[prompb.MetricMetadata]()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newSamples, newMetadata := handlePost(t, w, r)
		series.Add(int32(len(newSamples)))
		meta.Add(int32(len(newMetadata)))
		samples.AddSlice(newSamples)
		metaSamples.AddSlice(newMetadata)
		if series.Load() == iterations*items {
			done <- struct{}{}
		}
		if meta.Load() == iterations*items {
			done <- struct{}{}
		}
	}))
	c, err := newComponent(t, l, srv.URL, prometheus.NewRegistry())
	require.NoError(t, err)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	err = c.Start(ctx)
	require.NoError(t, err)
	defer c.Stop()

	index := atomic.NewInt64(0)
	results := &safeMap{
		results: make(map[float64]labels.Labels),
	}

	for i := 0; i < iterations; i++ {
		go func() {
			app := c.Appender(ctx)
			for j := 0; j < items; j++ {
				val := index.Add(1)
				v, lbl := add(int(val), app)
				results.Add(v, lbl)
			}
			require.NoError(t, app.Commit())
		}()
	}

	// This is a weird use case to handle eventually.
	// With race turned on this can take a long time.
	tm := time.NewTimer(20 * time.Second)
	select {
	case <-done:
	case <-tm.C:
		c.Stop()
		require.Truef(t, false, "failed to collect signals in the appropriate time, series found %d", series.Load())
	}

	cancel()

	for i := 0; i < samples.Len(); i++ {
		s := samples.Get(i)
		if len(s.Histograms) == 1 {
			lbls, ok := results.Get(s.Histograms[0].Sum)
			require.True(t, ok)
			for i, sLbl := range s.Labels {
				require.True(t, lbls[i].Name == sLbl.Name)
				require.True(t, lbls[i].Value == sLbl.Value)
			}
		} else {
			lbls, ok := results.Get(s.Samples[0].Value)
			require.True(t, ok)
			for i, sLbl := range s.Labels {
				require.True(t, lbls[i].Name == sLbl.Name)
				require.True(t, lbls[i].Value == sLbl.Value)
			}
		}
	}
	if test != nil {
		test(samples)
	} else {
		metaTest(metaSamples)
	}
}

func runTestV2(t *testing.T, add func(index int, appendable storage.Appender) (float64, labels.Labels), test func(samples *safeSlice[prompb.TimeSeries]), metaTest func(meta *safeSlice[prompb.MetricMetadata])) {
	l := log.NewLogfmtLogger(os.Stdout)
	done := make(chan struct{})
	var series atomic.Int32
	var meta atomic.Int32
	samplesV1 := newSafeSlice[prompb.TimeSeries]()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newSamples, symbols := handlePostV2(t, w, r)
		series.Add(int32(len(newSamples)))
		// Convert v2 data back to v1 for testing compatibility with existing test functions
		samplesV1.AddSlice(convertV2ToV1TimeSeries(t, newSamples, symbols))

		uniqueMetadata := map[uint32]struct{}{}
		for _, s := range newSamples {
			if s.Metadata.HelpRef != 0 || s.Metadata.Type != writev2.Metadata_METRIC_TYPE_UNSPECIFIED || s.Metadata.UnitRef != 0 {
				uniqueMetadata[s.Metadata.HelpRef] = struct{}{}
			}
		}

		meta.Add(int32(len(uniqueMetadata)))

		if series.Load() == iterations*items {
			done <- struct{}{}
		}
		if meta.Load() == iterations*items {
			done <- struct{}{}
		}
	}))
	c, err := newComponentV2(t, l, srv.URL, prometheus.NewRegistry())
	require.NoError(t, err)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	err = c.Start(ctx)
	require.NoError(t, err)
	defer c.Stop()

	index := atomic.NewInt64(0)
	results := &safeMap{
		results: make(map[float64]labels.Labels),
	}

	for range iterations {
		go func() {
			app := c.Appender(ctx)
			for range items {
				val := index.Add(1)
				v, lbl := add(int(val), app)
				results.Add(v, lbl)
			}
			require.NoError(t, app.Commit())
		}()
	}

	// This is a weird use case to handle eventually.
	// With race turned on this can take a long time.
	tm := time.NewTimer(20 * time.Second)
	select {
	case <-done:
	case <-tm.C:
		c.Stop()
		require.Truef(t, false, "failed to collect signals in the appropriate time, series found %d", series.Load())
	}

	cancel()

	if test != nil {
		test(samplesV1)
	} else if metaTest != nil {
		// TODO: Support testing metadata in v2
	}
}

func handlePost(t *testing.T, _ http.ResponseWriter, r *http.Request) ([]prompb.TimeSeries, []prompb.MetricMetadata) {
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	data, err = snappy.Decode(nil, data)
	require.NoError(t, err)

	var req prompb.WriteRequest
	err = req.Unmarshal(data)
	require.NoError(t, err)
	return req.GetTimeseries(), req.Metadata
}

func handlePostV2(t *testing.T, _ http.ResponseWriter, r *http.Request) ([]writev2.TimeSeries, []string) {
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	data, err = snappy.Decode(nil, data)
	require.NoError(t, err)

	var req writev2.Request
	err = req.Unmarshal(data)
	require.NoError(t, err)

	return req.Timeseries, req.Symbols
}

func makeSeries(index int) (int64, float64, labels.Labels) {
	return time.Now().UTC().UnixMilli(), float64(index), labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index))
}

func makeMetadata(index int) (metadata.Metadata, labels.Labels) {
	return metadata.Metadata{
		Type: "counter",
		Unit: "seconds",
		Help: "metadata help",
	}, labels.FromStrings("__name__", fmt.Sprintf("name_%d", index))
}

func makeHistogram(index int) (int64, labels.Labels, *histogram.Histogram) {
	return time.Now().UTC().UnixMilli(), labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index)), hist(index)
}

func makeExemplar(index int) exemplar.Exemplar {
	return exemplar.Exemplar{
		Labels: labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index)),
		Ts:     time.Now().UnixMilli(),
		HasTs:  true,
		Value:  float64(index),
	}
}

func hist(i int) *histogram.Histogram {
	return &histogram.Histogram{
		CounterResetHint: 1,
		Schema:           2,
		ZeroThreshold:    3,
		ZeroCount:        4,
		Count:            5,
		Sum:              float64(i),
		PositiveSpans: []histogram.Span{
			{
				Offset: 1,
				Length: 2,
			},
		},
		NegativeSpans: []histogram.Span{
			{
				Offset: 3,
				Length: 4,
			},
		},
		PositiveBuckets: []int64{1, 2, 3},
		NegativeBuckets: []int64{1, 2, 3},
	}
}

func histSame(t *testing.T, h *histogram.Histogram, pb prompb.Histogram) {
	require.True(t, h.Sum == pb.Sum)
	require.True(t, h.ZeroCount == pb.ZeroCount.(*prompb.Histogram_ZeroCountInt).ZeroCountInt)
	require.True(t, h.Schema == pb.Schema)
	require.True(t, h.Count == pb.Count.(*prompb.Histogram_CountInt).CountInt)
	require.True(t, h.ZeroThreshold == pb.ZeroThreshold)
	require.True(t, int32(h.CounterResetHint) == int32(pb.ResetHint))
	require.True(t, reflect.DeepEqual(h.PositiveBuckets, pb.PositiveDeltas))
	require.True(t, reflect.DeepEqual(h.NegativeBuckets, pb.NegativeDeltas))
	histSpanSame(t, h.PositiveSpans, pb.PositiveSpans)
	histSpanSame(t, h.NegativeSpans, pb.NegativeSpans)
}

func histSpanSame(t *testing.T, h []histogram.Span, pb []prompb.BucketSpan) {
	require.True(t, len(h) == len(pb))
	for i := range h {
		require.True(t, h[i].Length == pb[i].Length)
		require.True(t, h[i].Offset == pb[i].Offset)
	}
}

func makeFloatHistogram(index int) (int64, labels.Labels, *histogram.FloatHistogram) {
	return time.Now().UTC().UnixMilli(), labels.FromStrings(fmt.Sprintf("name_%d", index), fmt.Sprintf("value_%d", index)), histFloat(index)
}

func histFloat(i int) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		CounterResetHint: 1,
		Schema:           2,
		ZeroThreshold:    3,
		ZeroCount:        4,
		Count:            5,
		Sum:              float64(i),
		PositiveSpans: []histogram.Span{
			{
				Offset: 1,
				Length: 2,
			},
		},
		NegativeSpans: []histogram.Span{
			{
				Offset: 3,
				Length: 4,
			},
		},
		PositiveBuckets: []float64{1.1, 2.2, 3.3},
		NegativeBuckets: []float64{1.2, 2.3, 3.4},
	}
}

func histFloatSame(t *testing.T, h *histogram.FloatHistogram, pb prompb.Histogram) {
	require.True(t, h.Sum == pb.Sum)
	require.True(t, h.ZeroCount == pb.ZeroCount.(*prompb.Histogram_ZeroCountFloat).ZeroCountFloat)
	require.True(t, h.Schema == pb.Schema)
	require.True(t, h.Count == pb.Count.(*prompb.Histogram_CountFloat).CountFloat)
	require.True(t, h.ZeroThreshold == pb.ZeroThreshold)
	require.True(t, int32(h.CounterResetHint) == int32(pb.ResetHint))
	require.True(t, reflect.DeepEqual(h.PositiveBuckets, pb.PositiveCounts))
	require.True(t, reflect.DeepEqual(h.NegativeBuckets, pb.NegativeCounts))
	histSpanSame(t, h.PositiveSpans, pb.PositiveSpans)
	histSpanSame(t, h.NegativeSpans, pb.NegativeSpans)
}

func newComponent(t *testing.T, l log.Logger, url string, reg prometheus.Registerer) (Queue, error) {
	return NewQueue("test", types.ConnectionConfig{
		URL:              url,
		Timeout:          30 * time.Second,
		RetryBackoff:     1 * time.Second,
		MaxRetryAttempts: 1,
		BatchCount:       5,
		FlushInterval:    100 * time.Millisecond,
		ProtobufMessage:  config.RemoteWriteProtoMsgV1,
		Parallelism: types.ParallelismConfig{
			AllowedDrift:                60 * time.Second,
			MaxConnections:              4,
			MinConnections:              4,
			ResetInterval:               5 * time.Minute,
			Lookback:                    5 * time.Minute,
			CheckInterval:               10 * time.Second,
			AllowedNetworkErrorFraction: 0.05,
		},
	}, t.TempDir(), 10, 1*time.Second, 1*time.Hour, reg, "alloy", l)
}

func newComponentV2(t *testing.T, l log.Logger, url string, reg prometheus.Registerer) (Queue, error) {
	return NewQueue("test", types.ConnectionConfig{
		URL:              url,
		Timeout:          30 * time.Second,
		RetryBackoff:     1 * time.Second,
		MaxRetryAttempts: 1,
		BatchCount:       5,
		FlushInterval:    100 * time.Millisecond,
		ProtobufMessage:  config.RemoteWriteProtoMsgV2,
		Parallelism: types.ParallelismConfig{
			AllowedDrift:                60 * time.Second,
			MaxConnections:              4,
			MinConnections:              4,
			ResetInterval:               5 * time.Minute,
			Lookback:                    5 * time.Minute,
			CheckInterval:               10 * time.Second,
			AllowedNetworkErrorFraction: 0.05,
		},
	}, t.TempDir(), 10, 1*time.Second, 1*time.Hour, reg, "alloy", l)
}

func newSafeSlice[T any]() *safeSlice[T] {
	return &safeSlice[T]{slice: make([]T, 0)}
}

type safeSlice[T any] struct {
	slice []T
	mut   sync.Mutex
}

func (s *safeSlice[T]) Add(v T) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.slice = append(s.slice, v)
}

func (s *safeSlice[T]) AddSlice(v []T) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.slice = append(s.slice, v...)
}

func (s *safeSlice[T]) Len() int {
	s.mut.Lock()
	defer s.mut.Unlock()
	return len(s.slice)
}

func (s *safeSlice[T]) Get(i int) T {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.slice[i]
}

type safeMap struct {
	mut     sync.Mutex
	results map[float64]labels.Labels
}

func (s *safeMap) Add(v float64, ls labels.Labels) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.results[v] = ls
}

func (s *safeMap) Get(v float64) (labels.Labels, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	res, ok := s.results[v]
	return res, ok
}

// convertV2ToV1TimeSeries converts v2 TimeSeries data back to v1 format for testing compatibility
func convertV2ToV1TimeSeries(t *testing.T, samplesV2 []writev2.TimeSeries, symbols []string) []prompb.TimeSeries {
	v1Samples := make([]prompb.TimeSeries, 0, len(samplesV2))

	for _, v2ts := range samplesV2 {
		if len(v2ts.LabelsRefs) > 2 {
			t.Logf("v2 TimeSeries has more than 2 label references: %v", v2ts)
		}
		v1ts := prompb.TimeSeries{
			Labels: make([]prompb.Label, 0, len(v2ts.LabelsRefs)/2),
		}

		// Convert label references back to actual labels using the symbol table
		for j := 0; j < len(v2ts.LabelsRefs); j += 2 {
			if j+1 < len(v2ts.LabelsRefs) {
				nameIdx := v2ts.LabelsRefs[j]
				valueIdx := v2ts.LabelsRefs[j+1]
				if int(nameIdx) < len(symbols) && int(valueIdx) < len(symbols) {
					v1ts.Labels = append(v1ts.Labels, prompb.Label{
						Name:  symbols[nameIdx],
						Value: symbols[valueIdx],
					})
				} else {
					t.Logf("label index out of range: nameIdx=%d, valueIdx=%d, symbolsLen=%d", nameIdx, valueIdx, len(symbols))
				}
			} else {
				t.Logf("labels not in pairs: %v", v2ts.LabelsRefs)
			}
		}

		// Convert samples
		for _, sample := range v2ts.Samples {
			v1ts.Samples = append(v1ts.Samples, prompb.Sample{
				Value:     sample.Value,
				Timestamp: sample.Timestamp,
			})
		}

		// Convert exemplars
		for _, exemplar := range v2ts.Exemplars {
			v1exemplar := prompb.Exemplar{
				Value:     exemplar.Value,
				Timestamp: exemplar.Timestamp,
			}
			// Convert exemplar labels
			for k := 0; k < len(exemplar.LabelsRefs); k += 2 {
				if k+1 < len(exemplar.LabelsRefs) {
					nameIdx := exemplar.LabelsRefs[k]
					valueIdx := exemplar.LabelsRefs[k+1]
					if int(nameIdx) < len(symbols) && int(valueIdx) < len(symbols) {
						v1exemplar.Labels = append(v1exemplar.Labels, prompb.Label{
							Name:  symbols[nameIdx],
							Value: symbols[valueIdx],
						})
					}
				}
			}
			v1ts.Exemplars = append(v1ts.Exemplars, v1exemplar)
		}

		// Convert Histograms
		for _, hist := range v2ts.Histograms {
			v1hist := prompb.Histogram{
				Schema:        hist.Schema,
				ZeroThreshold: hist.ZeroThreshold,
				Sum:           hist.Sum,
				Timestamp:     hist.Timestamp,
				ResetHint:     prompb.Histogram_ResetHint(hist.ResetHint),
			}

			// Convert count field
			switch count := hist.Count.(type) {
			case *writev2.Histogram_CountInt:
				v1hist.Count = &prompb.Histogram_CountInt{CountInt: count.CountInt}
			case *writev2.Histogram_CountFloat:
				v1hist.Count = &prompb.Histogram_CountFloat{CountFloat: count.CountFloat}
			}

			// Convert zero count field
			switch zeroCount := hist.ZeroCount.(type) {
			case *writev2.Histogram_ZeroCountInt:
				v1hist.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: zeroCount.ZeroCountInt}
			case *writev2.Histogram_ZeroCountFloat:
				v1hist.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: zeroCount.ZeroCountFloat}
			}

			// Convert bucket spans
			for _, span := range hist.NegativeSpans {
				v1hist.NegativeSpans = append(v1hist.NegativeSpans, prompb.BucketSpan{
					Offset: span.Offset,
					Length: span.Length,
				})
			}

			for _, span := range hist.PositiveSpans {
				v1hist.PositiveSpans = append(v1hist.PositiveSpans, prompb.BucketSpan{
					Offset: span.Offset,
					Length: span.Length,
				})
			}

			// Convert deltas/counts
			v1hist.NegativeDeltas = append(v1hist.NegativeDeltas, hist.NegativeDeltas...)
			v1hist.PositiveDeltas = append(v1hist.PositiveDeltas, hist.PositiveDeltas...)
			v1hist.NegativeCounts = append(v1hist.NegativeCounts, hist.NegativeCounts...)
			v1hist.PositiveCounts = append(v1hist.PositiveCounts, hist.PositiveCounts...)

			v1ts.Histograms = append(v1ts.Histograms, v1hist)
		}

		v1Samples = append(v1Samples, v1ts)
	}

	return v1Samples
}
