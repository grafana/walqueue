package network

import (
	"net/http"
	"time"

	"github.com/grafana/walqueue/types"
)

// recordStats determines what values to send to the stats function. This allows for any
// number of metrics/signals libraries to be used. Prometheus, OTel, and any other.
func recordStats(seriesCount, histogramCount, metadataCount int, newestTS int64, isMeta bool, stats func(s types.NetworkStats), r sendResult, bytesSent int) {

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
			SendDuration: r.duration,
		})
	case r.successful:
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
			SendDuration:           r.duration,
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
			SendDuration: r.duration,
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
			SendDuration: r.duration,
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
			SendDuration: r.duration,
		})
	}

}

func getSeriesCount[T types.Datum](tss []T) int {
	cnt := 0
	for _, ts := range tss {
		mm, valid := interface{}(ts).(types.MetricDatum)
		if !valid {
			continue
		}
		if !mm.IsHistogram() {
			cnt++
		}
	}
	return cnt
}

func getHistogramCount[T types.Datum](tss []T) int {
	cnt := 0
	for _, ts := range tss {
		mm, valid := interface{}(ts).(types.MetricDatum)
		if !valid {
			continue
		}
		if mm.IsHistogram() {
			cnt++
		}
	}
	return cnt
}

func getMetaDataCount[T types.Datum](tss []T) int {
	cnt := 0
	for _, ts := range tss {
		_, valid := interface{}(ts).(types.MetadataDatum)
		if !valid {
			continue
		}
		cnt++
	}
	return cnt
}
