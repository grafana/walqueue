package network

import (
	"net/http"
	"time"

	"github.com/grafana/walqueue/types"
)

// recordStats determines what values to send to the stats function. This allows for any
// number of metrics/signals libraries to be used. Prometheus, OTel, and any other.
func recordStats[T types.Datum](series []T, isMeta bool, stats func(s types.NetworkStats), r sendResult, bytesSent int) {
	seriesCount := getSeriesCount(series)
	histogramCount := getHistogramCount(series)
	metadataCount := getMetaDataCount(series)

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
			mm, valid := interface{}(ts).(types.MetricDatum)
			if !valid {
				continue
			}
			if mm.TimeStampMS() > newestTS {
				newestTS = mm.TimeStampMS()
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
