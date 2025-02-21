// Copyright 2024 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package v2 Copied from prometheus.
package v2

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"strings"
)

// FromIntHistogram returns remote Histogram from the integer Histogram.
func FromIntHistogram(timestamp int64, h *histogram.Histogram) prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		ResetHint:      prompb.Histogram_ResetHint(h.CounterResetHint),
		Timestamp:      timestamp,
	}
}

// FromFloatHistogram returns remote Histogram from the float Histogram.
func FromFloatHistogram(timestamp int64, fh *histogram.FloatHistogram) prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountFloat{CountFloat: fh.Count},
		Sum:            fh.Sum,
		Schema:         fh.Schema,
		ZeroThreshold:  fh.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: fh.ZeroCount},
		NegativeSpans:  spansToSpansProto(fh.NegativeSpans),
		NegativeCounts: fh.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(fh.PositiveSpans),
		PositiveCounts: fh.PositiveBuckets,
		ResetHint:      prompb.Histogram_ResetHint(fh.CounterResetHint),
		Timestamp:      timestamp,
	}
}

func spansToSpansProto(s []histogram.Span) []prompb.BucketSpan {
	spans := make([]prompb.BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = prompb.BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}

// FromMetadataType transforms a Prometheus metricType into prompb metricType. Since the former is a string we need to transform it to an enum.
func FromMetadataType(t model.MetricType) prompb.MetricMetadata_MetricType {
	mt := strings.ToUpper(string(t))
	v, ok := prompb.MetricMetadata_MetricType_value[mt]
	if !ok {
		return prompb.MetricMetadata_UNKNOWN
	}
	return prompb.MetricMetadata_MetricType(v)
}
