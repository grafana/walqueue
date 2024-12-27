package v1

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/grafana/walqueue/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabels(t *testing.T) {
	lblsMap := make(map[string]string)
	unique := make(map[string]struct{})
	for i := 0; i < 1_000; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := randString()
		lblsMap[k] = v
		unique[k] = struct{}{}
		unique[v] = struct{}{}
	}

	metrics := make([]*types.Metric, 1)
	metrics[0] = &types.Metric{}

	metrics[0].Labels = labels.FromMap(lblsMap)

	serializer := GetSerializer()
	var newMetrics *types.Metrics
	var err error
	err = serializer.Serialize(&types.Metrics{M: metrics}, nil, func(buf []byte) {
		newMetrics, _, err = serializer.Deserialize(buf)
		require.NoError(t, err)
	})
	require.NoError(t, err)
	series1 := newMetrics.M[0]
	series2 := metrics[0]
	require.Len(t, series2.Labels, len(series1.Labels))
	// Ensure we were able to convert back and forth properly.
	for i, lbl := range series2.Labels {
		require.Equal(t, lbl.Name, series1.Labels[i].Name)
		require.Equal(t, lbl.Value, series1.Labels[i].Value)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	// 2024-12-26 BenchmarkDeserialize-20    	     226	   5447446 ns/op
	metrics := make([]*types.Metric, 0)
	for k := 0; k < 1_000; k++ {
		lblsMap := make(map[string]string)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key_%d", j)
			v := randString()
			lblsMap[key] = v
		}
		m := &types.Metric{}
		m.Labels = labels.FromMap(lblsMap)
		metrics = append(metrics, m)
	}
	for i := 0; i < b.N; i++ {
		sg := GetSerializer()
		var newMetrics *types.Metrics
		var err error
		err = sg.Serialize(&types.Metrics{M: metrics}, nil, func(buf []byte) {
			newMetrics, _, err = sg.Deserialize(buf)
			if err != nil {
				b.Fatal(err)
			}
		})
		if err != nil {
			b.Fatal(err)
		}
		if err != nil {
			panic(err.Error())
		}
		types.PutMetricSliceIntoPool(newMetrics.M)
	}
}

func TestBackwardsCompatability(t *testing.T) {
	buf, err := os.ReadFile("v1.bin")
	require.NoError(t, err)
	sg := GetSerializer()
	metrics, meta, err := sg.Deserialize(buf)
	require.NoError(t, err)
	require.Len(t, metrics.M, 1_000)
	require.Len(t, meta.M, 0)
	for _, m := range metrics.M {
		require.Len(t, m.Labels, 10)
		for _, lbl := range m.Labels {
			require.True(t, strings.HasPrefix(lbl.Name, "key"))
		}
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString() string {
	b := make([]rune, rand.Intn(20))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
