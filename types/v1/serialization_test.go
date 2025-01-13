package v1

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

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

	lbls := labels.FromMap(lblsMap)

	serializer := GetSerializer()
	var err error
	err = serializer.AddPrometheusMetric(time.Now().UnixMilli(), rand.Float64(), lbls, nil, nil, nil)
	require.NoError(t, err)
	var bb []byte
	err = serializer.Marshal(func(_ map[string]string, bytes []byte) error {
		bb = make([]byte, len(bytes))
		copy(bb, bytes)
		return nil
	})
	require.NoError(t, err)
	deserial := GetSerializer()
	items, err := deserial.Unmarshal(nil, bb)
	require.NoError(t, err)
	pm := prompb.TimeSeries{}
	err = pm.Unmarshal(items[0].Bytes())
	require.NoError(t, err)

	require.Len(t, lbls, len(pm.Labels))
	// Ensure we were able to convert back and forth properly.
	for i, lbl := range pm.Labels {
		require.Equal(t, lbl.Name, lbls[i].Name)
		require.Equal(t, lbl.Value, lbls[i].Value)
	}
}

func TestBackwardsCompatability(t *testing.T) {
	buf, err := os.ReadFile("v1.bin")
	require.NoError(t, err)
	sg := GetSerializer()
	metrics, err := sg.Unmarshal(nil, buf)
	require.NoError(t, err)
	require.Len(t, metrics, 1_000)
	for _, m := range metrics {
		pm := prompb.TimeSeries{}
		err = pm.Unmarshal(m.Bytes())
		require.NoError(t, err)
		require.Len(t, pm.Labels, 10)
		for _, lbl := range pm.Labels {
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
