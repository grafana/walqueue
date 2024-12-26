package v1

import (
	"fmt"
	"math/rand"
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
	var newMetrics []*types.Metric
	var err error
	err = serializer.Serialize(metrics, nil, func(buf []byte) {
		newMetrics, _, err = serializer.Deserialize(buf)
		require.NoError(t, err)
	})
	require.NoError(t, err)
	series1 := newMetrics[0]
	series2 := metrics[0]
	require.Len(t, series2.Labels, len(series1.Labels))
	// Ensure we were able to convert back and forth properly.
	for i, lbl := range series2.Labels {
		require.Equal(t, lbl.Name, series1.Labels[i].Name)
		require.Equal(t, lbl.Value, series1.Labels[i].Value)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	// 2024-12-17 BenchmarkDeserialize-24    	     631	   1806962 ns/op
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
		var newMetrics []*types.Metric
		var err error
		err = sg.Serialize(metrics, nil, func(buf []byte) {

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
		types.PutMetricsIntoPool(newMetrics)
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
