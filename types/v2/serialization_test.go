package v2

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
	err := serializer.Serialize(metrics, nil, func(b []byte) {
		var err error
		newMetrics, _, err = serializer.Deserialize(b)
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
	// NOTE there can be a fair amount of variance in these, +-200 is easily observable.
	// cpu: 13th Gen Intel(R) Core(TM) i7-13700K
	// 2024-12-17 BenchmarkDeserialize-24    	     909	   1234031 ns/op after some optimization on pools
	// 2024-12-17 BenchmarkDeserialize-24    	    1308	    858204 ns/op further optimizations on allocs
	// 2023-12-17 BenchmarkDeserialize-24    	    1508	    811829 ns/op after switching to swiss map
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
	var err error
	for i := 0; i < b.N; i++ {
		sg := GetSerializer()
		var newMetrics []*types.Metric
		var newMeta []*types.Metric
		err = sg.Serialize(metrics, nil, func(buf []byte) {
			var err error
			newMetrics, newMeta, err = sg.Deserialize(buf)
			if err != nil {
				panic(err.Error())
			}
		})
		if err != nil {
			b.Fatal(err)
		}
		types.PutMetricsIntoPool(newMetrics)
		types.PutMetricsIntoPool(newMeta)
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

func TestBenchmarkDeserialize(t *testing.T) {
	type args struct {
		b *testing.B
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			BenchmarkDeserialize(tt.args.b)
		})
	}
}

func Test_randString(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := randString(); got != tt.want {
				t.Errorf("randString() = %v, want %v", got, tt.want)
			}
		})
	}
}
