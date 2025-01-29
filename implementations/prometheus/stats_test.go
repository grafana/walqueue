package prometheus

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/grafana/walqueue/types"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestDriftSerializer(t *testing.T) {
	ps := NewStats("test", "test", prom.NewRegistry())
	ps.UpdateSerializer(types.SerializerStats{
		SeriesStored:           1,
		MetadataStored:         1,
		Errors:                 0,
		NewestTimestampSeconds: time.Now().Unix(),
		TTLDropped:             0,
	})
	dt := &dto.Metric{}
	err := ps.TimestampDriftSeconds.Write(dt)
	require.NoError(t, err)
	require.Equal(t, float64(0), dt.Gauge.GetValue())
}

func TestDriftNetwork(t *testing.T) {
	ps := NewStats("test", "test", prom.NewRegistry())
	ps.UpdateNetwork(types.NetworkStats{
		NewestTimestampSeconds: time.Now().Unix(),
	})
	dt := &dto.Metric{}
	err := ps.TimestampDriftSeconds.Write(dt)
	require.NoError(t, err)
	require.Equal(t, float64(0), dt.Gauge.GetValue())
}
