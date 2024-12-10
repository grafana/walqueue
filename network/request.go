//go:build !goexperiment.arenas

package network

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
)

func createWriteRequest(series []*types.TimeSeriesBinary, externalLabels map[string]string) ([]byte, error) {
	wr := &prompb.WriteRequest{
		// We know BatchCount is the most we will ever send.
		Timeseries: make([]prompb.TimeSeries, len(series)),
	}

	for i, tsBuf := range series {
		ts := wr.Timeseries[i]
		ts.Labels = make([]prompb.Label, len(tsBuf.Labels))
		for k, v := range tsBuf.Labels {
			ts.Labels[k].Name = v.Name
			ts.Labels[k].Value = v.Value
		}
		ts.Histograms = make([]prompb.Histogram, 1)
		if tsBuf.Histograms.Histogram != nil {
			ts.Histograms = ts.Histograms[:1]
			ts.Histograms[0] = tsBuf.Histograms.Histogram.ToPromHistogram()
		}
		if tsBuf.Histograms.FloatHistogram != nil {
			ts.Histograms = ts.Histograms[:1]
			ts.Histograms[0] = tsBuf.Histograms.FloatHistogram.ToPromFloatHistogram()
		}

		if tsBuf.Histograms.Histogram == nil && tsBuf.Histograms.FloatHistogram == nil {
			ts.Histograms = ts.Histograms[:0]
		}

		// Encode the external labels inside if needed.
		for k, v := range externalLabels {
			found := false
			for j, lbl := range ts.Labels {
				if lbl.Name == k {
					ts.Labels[j].Value = v
					found = true
					break
				}
			}
			if !found {
				ts.Labels = append(ts.Labels, prompb.Label{
					Name:  k,
					Value: v,
				})
			}
		}

		ts.Samples = make([]prompb.Sample, 1)

		ts.Samples[0].Value = tsBuf.Value
		ts.Samples[0].Timestamp = tsBuf.TS
		wr.Timeseries[i] = ts
	}
	return wr.Marshal()
}

func createWriteRequestMetadata(l log.Logger, series []*types.TimeSeriesBinary) ([]byte, error) {
	wr := &prompb.WriteRequest{}
	// Metadata is rarely sent so having this being less than optimal is fine.
	wr.Metadata = make([]prompb.MetricMetadata, 0)
	for _, ts := range series {
		mt, valid := toMetadata(ts)
		if !valid {
			level.Error(l).Log("msg", "invalid metadata was found", "labels", ts.Labels.String())
			continue
		}
		wr.Metadata = append(wr.Metadata, mt)
	}
	return wr.Marshal()
}
