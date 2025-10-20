package serialization_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/walqueue/serialization"
	"github.com/grafana/walqueue/types"
)

type testSender struct {
	sent []types.PrometheusMetric
}

func (ts *testSender) SendMetrics(ctx context.Context, metrics []*types.PrometheusMetric, externalLabels labels.Labels) error {
	for _, m := range metrics {
		// Shallow copy since the appender is going to clear the incoming value
		ts.sent = append(ts.sent, *m)
	}
	return nil
}

func (ts *testSender) SendMetadata(ctx context.Context, name string, unit string, help string, pType string) error {
	panic("Not implemented")
}

func TestAppenderMaintainsAppendedOrder(t *testing.T) {
	sender := &testSender{}
	app := serialization.NewAppender(t.Context(), time.Hour, sender, labels.EmptyLabels(), log.NewNopLogger())

	for i := range 10 {
		_, err := app.Append(storage.SeriesRef(i), labels.FromStrings(strconv.Itoa(i), "bar"), time.Now().UnixMilli(), float64(i))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())
	require.Len(t, sender.sent, 10)

	for i := range 10 {
		// We cannot access sender.sent[i].L labels directly using the stringlabels API
		// instead, we can iterate over the single element and assign l.Name to the name variable
		var name string
		sender.sent[i].L.Range(func(l labels.Label) { name = l.Name })
		assert.Equal(t, strconv.Itoa(i), name)
	}
}
