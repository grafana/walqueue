package network

import (
	"github.com/prometheus/prometheus/prompb"
	"net/http"
	"time"

	"github.com/grafana/walqueue/types"
)

// recordStats determines what values to send to the stats function. This allows for any
// number of metrics/signals libraries to be used. Prometheus, OTel, and any other.
func recordStats(series []prompb.TimeSeries, meta []prompb.MetricMetadata, isMeta bool, stats func(s types.NetworkStats), r sendResult, bytesSent int) {
	seriesCount := getSeriesCount(series)
	histogramCount := getHistogramCount(series)
	metadataCount := len(meta)
	switch {
	case r.networkError:
		stats(types.NetworkStats{
			Series: types.CategoryStats{
				NetworkSamplesFailed: seriesCount,
			},
			Histogram: types.CategoryStats{
				NetworkSamplesFailed: histogramCount,
			},
			Metadata: types.CategoryStats{
				NetworkSamplesFailed: metadataCount,
			},
		})
	case r.successful:
		// Need to grab the newest series.
		var newestTS int64
		for _, ts := range series {
			if len(ts.Samples) > 0 {
				if ts.Samples[0].Timestamp > newestTS {
					newestTS = ts.Samples[0].Timestamp
				}
			}
		}
		var sampleBytesSent int
		var metaBytesSent int
		// Each loop is explicitly a normal signal or metadata sender.
		if isMeta {
			metaBytesSent = bytesSent
		} else {
			sampleBytesSent = bytesSent
		}
		stats(types.NetworkStats{
			Series: types.CategoryStats{
				SeriesSent: seriesCount,
			},
			Histogram: types.CategoryStats{
				SeriesSent: histogramCount,
			},
			Metadata: types.CategoryStats{
				SeriesSent: metadataCount,
			},
			MetadataBytes:          metaBytesSent,
			SeriesBytes:            sampleBytesSent,
			NewestTimestampSeconds: time.UnixMilli(newestTS).Unix(),
		})
	case r.statusCode == http.StatusTooManyRequests:
		stats(types.NetworkStats{
			Series: types.CategoryStats{
				RetriedSamples:    seriesCount,
				RetriedSamples429: seriesCount,
			},
			Histogram: types.CategoryStats{
				RetriedSamples:    histogramCount,
				RetriedSamples429: histogramCount,
			},
			Metadata: types.CategoryStats{
				RetriedSamples:    metadataCount,
				RetriedSamples429: metadataCount,
			},
		})
	case r.statusCode/100 == 5:
		stats(types.NetworkStats{
			Series: types.CategoryStats{
				RetriedSamples5XX: seriesCount,
			},
			Histogram: types.CategoryStats{
				RetriedSamples5XX: histogramCount,
			},
			Metadata: types.CategoryStats{
				RetriedSamples: metadataCount,
			},
		})
	case r.statusCode != 200:
		stats(types.NetworkStats{
			Series: types.CategoryStats{
				FailedSamples: seriesCount,
			},
			Histogram: types.CategoryStats{
				FailedSamples: histogramCount,
			},
			Metadata: types.CategoryStats{
				FailedSamples: metadataCount,
			},
		})
	}

}

func getSeriesCount(tss []prompb.TimeSeries) int {
	cnt := 0
	for _, ts := range tss {
		if len(ts.Samples) > 0 {
			cnt++
		}
	}
	return cnt
}

func getHistogramCount(tss []prompb.TimeSeries) int {
	cnt := 0
	for _, ts := range tss {
		cnt = cnt + len(ts.Histograms)
	}
	return cnt
}
