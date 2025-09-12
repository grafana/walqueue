package network

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// Used to deserialize the data from the WAL so we can construct the v2 request
var tspbPool = sync.Pool{
	New: func() any {
		return &prompb.TimeSeries{}
	},
}

func resetTs(ts *prompb.TimeSeries) {
	ts.Labels = ts.Labels[:0]
	ts.Samples = ts.Samples[:0]
	ts.Histograms = ts.Histograms[:0]
	ts.Exemplars = ts.Exemplars[:0]
}

// generateWriteRequest creates a proto prombpb.WriteRequest from manual bytes. Since
// the data is already serialized and we just need to wrap it in a proto message.
func generateWriteRequest[T types.Datum](series []T, input []byte) ([]byte, error) {
	bb := bytes.NewBuffer(input)
	for _, t := range series {
		// This conversion is necessary to test for a specific interface.
		mm, valid := any(t).(types.MetricDatum)
		if valid {
			buf := mm.Bytes()
			size := proto.EncodeVarint(uint64(len(buf)))
			// This is the prompb constant for timeseries
			bb.WriteByte(0xa)
			bb.Write(size)
			bb.Write(buf)
			continue
		}
		md, valid := any(t).(types.MetadataDatum)
		if valid {
			buf := md.Bytes()
			size := proto.EncodeVarint(uint64(len(buf)))
			// This is the prompb constant for metadata
			bb.WriteByte(0x1a)
			bb.Write(size)
			bb.Write(buf)
			continue
		}
		return nil, fmt.Errorf("unknown data type %T", t)
	}

	return bb.Bytes(), nil
}

func generateWriteRequestV2[T types.Datum](symbolTable *writev2.SymbolsTable, series []T, metadataCache *metadataCache, input []byte) ([]byte, error) {
	ts := make([]writev2.TimeSeries, 0, len(series))

	metadata := make(map[string]writev2.Metadata)

	for _, t := range series {
		mm, valid := any(t).(types.MetricDatum)
		if valid {
			tspb := tspbPool.Get().(*prompb.TimeSeries)
			if err := tspb.Unmarshal(mm.Bytes()); err != nil {
				return nil, fmt.Errorf("failed to unmarshal timeseries: %w", err)
			}

			// Convert prompb.TimeSeries to writev2.TimeSeries
			v2ts := convertTimeSeriesToV2(tspb, metadata, metadataCache, symbolTable)
			ts = append(ts, v2ts)
			resetTs(tspb)
			tspbPool.Put(tspb)
			continue
		}

		// We do not expect to see metadata datums, as they should be loaded into the cache before they reach the shard writing PRWv2
		return nil, fmt.Errorf("unknown data type %T", t)
	}

	req := &writev2.Request{
		Symbols:    symbolTable.Symbols(),
		Timeseries: ts,
	}

	if len(input) < req.Size() {
		input = make([]byte, req.Size())
	}

	_, err := req.MarshalTo(input)
	return input, err
}

// convertTimeSeriesToV2 converts a prompb.TimeSeries to writev2.TimeSeries using the symbol table
func convertTimeSeriesToV2(ts *prompb.TimeSeries, metadata map[string]writev2.Metadata, metadataCache *metadataCache, symbolTable *writev2.SymbolsTable) writev2.TimeSeries {
	// Convert labels to label references using the symbol table
	labelsRefs := make([]uint32, 0, len(ts.Labels)*2)
	metricFamilyName := ""
	for _, label := range ts.Labels {
		nameRef := symbolTable.Symbolize(label.Name)
		valueRef := symbolTable.Symbolize(label.Value)
		labelsRefs = append(labelsRefs, nameRef, valueRef)

		if label.Name == "__name__" {
			metricFamilyName = label.Value
		}
	}

	v2ts := writev2.TimeSeries{
		LabelsRefs: labelsRefs,
		Metadata: writev2.Metadata{
			Type:    writev2.Metadata_METRIC_TYPE_UNSPECIFIED,
			HelpRef: 0,
			UnitRef: 0,
		},
	}

	if metricFamilyName != "" {
		// Check for already symbolized metadata, otherwise symbolize if it is in the payload
		if translated, exists := metadata[metricFamilyName]; exists {
			v2ts.Metadata = translated
		} else if md, exists := metadataCache.GetIfNotSent(metricFamilyName); exists {
			v2ts.Metadata = writev2.Metadata{
				Type:    writev2.Metadata_MetricType(md.Type),
				HelpRef: symbolTable.Symbolize(md.Help),
				UnitRef: symbolTable.Symbolize(md.Unit),
			}
			metadata[metricFamilyName] = v2ts.Metadata
		}
	}

	// Convert samples
	if len(ts.Samples) > 0 {
		v2ts.Samples = make([]writev2.Sample, len(ts.Samples))
		for i, sample := range ts.Samples {
			v2ts.Samples[i] = writev2.Sample{
				Value:     sample.Value,
				Timestamp: sample.Timestamp,
			}
		}
	}

	// Convert histograms
	if len(ts.Histograms) > 0 {
		v2ts.Histograms = make([]writev2.Histogram, len(ts.Histograms))
		for i, hist := range ts.Histograms {
			v2ts.Histograms[i] = convertHistogramToV2(&hist)
		}
	}

	// Convert exemplars
	if len(ts.Exemplars) > 0 {
		v2ts.Exemplars = make([]writev2.Exemplar, len(ts.Exemplars))
		for i, exemplar := range ts.Exemplars {
			exemplarLabelsRefs := make([]uint32, 0, len(exemplar.Labels)*2)
			for _, label := range exemplar.Labels {
				nameRef := symbolTable.Symbolize(label.Name)
				valueRef := symbolTable.Symbolize(label.Value)
				exemplarLabelsRefs = append(exemplarLabelsRefs, nameRef, valueRef)
			}

			v2ts.Exemplars[i] = writev2.Exemplar{
				LabelsRefs: exemplarLabelsRefs,
				Value:      exemplar.Value,
				Timestamp:  exemplar.Timestamp,
			}
		}
	}

	return v2ts
}

// convertHistogramToV2 converts a prompb.Histogram to writev2.Histogram
func convertHistogramToV2(hist *prompb.Histogram) writev2.Histogram {
	v2hist := writev2.Histogram{
		Sum:            hist.Sum,
		Schema:         hist.Schema,
		ZeroThreshold:  hist.ZeroThreshold,
		NegativeSpans:  make([]writev2.BucketSpan, len(hist.NegativeSpans)),
		NegativeDeltas: hist.NegativeDeltas,
		NegativeCounts: hist.NegativeCounts,
		PositiveSpans:  make([]writev2.BucketSpan, len(hist.PositiveSpans)),
		PositiveDeltas: hist.PositiveDeltas,
		PositiveCounts: hist.PositiveCounts,
		ResetHint:      writev2.Histogram_ResetHint(hist.ResetHint),
		Timestamp:      hist.Timestamp,
	}

	if hist.Count != nil {
		switch count := hist.Count.(type) {
		case *prompb.Histogram_CountInt:
			v2hist.Count = &writev2.Histogram_CountInt{
				CountInt: count.CountInt,
			}
		case *prompb.Histogram_CountFloat:
			v2hist.Count = &writev2.Histogram_CountFloat{
				CountFloat: count.CountFloat,
			}
		}
	}

	if hist.ZeroCount != nil {
		switch zeroCount := hist.ZeroCount.(type) {
		case *prompb.Histogram_ZeroCountInt:
			v2hist.ZeroCount = &writev2.Histogram_ZeroCountInt{
				ZeroCountInt: zeroCount.ZeroCountInt,
			}
		case *prompb.Histogram_ZeroCountFloat:
			v2hist.ZeroCount = &writev2.Histogram_ZeroCountFloat{
				ZeroCountFloat: zeroCount.ZeroCountFloat,
			}
		}
	}

	// Convert spans
	for i, span := range hist.NegativeSpans {
		v2hist.NegativeSpans[i] = writev2.BucketSpan{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	for i, span := range hist.PositiveSpans {
		v2hist.PositiveSpans[i] = writev2.BucketSpan{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	return v2hist
}
