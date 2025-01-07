package serialization

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type metric struct {
	l  labels.Labels
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

type metadata struct {
	name  string
	unit  string
	help  string
	pType string
}
