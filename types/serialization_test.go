package types

import (
	"fmt"
	"math/rand"
	"testing"

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
	sg := &SeriesGroup{
		Series: make([]*TimeSeriesBinary, 1),
	}
	sg.Series[0] = GetTimeSeriesFromPool()
	defer PutTimeSeriesIntoPool(sg.Series[0])
	sg.Series[0].Labels = labels.FromMap(lblsMap)
	strMap := make(map[string]uint32)

	sg.Series[0].FillLabelMapping(strMap)
	stringsSlice := make([]ByteString, len(strMap))
	for k, v := range strMap {
		stringsSlice[v] = ByteString([]byte(k))
	}
	sg.Strings = stringsSlice
	buf, err := sg.MarshalMsg(nil)
	require.NoError(t, err)
	newSg := &SeriesGroup{}
	newSg, _, err = DeserializeToSeriesGroup(newSg, buf)
	require.NoError(t, err)
	series1 := newSg.Series[0]
	series2 := sg.Series[0]
	require.Len(t, series2.Labels, len(series1.Labels))
	// Ensure we were able to convert back and forth properly.
	for i, lbl := range series2.Labels {
		require.Equal(t, lbl.Name, series1.Labels[i].Name)
		require.Equal(t, lbl.Value, series1.Labels[i].Value)
	}
}

func BenchmarkFill(b *testing.B) {
	sg := &SeriesGroup{
		Series: make([]*TimeSeriesBinary, 0),
	}
	for k := 0; k < 1_000; k++ {
		lblsMap := make(map[string]string)
		unique := make(map[string]struct{})
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key_%d", j)
			v := randString()
			lblsMap[key] = v
			unique[key] = struct{}{}
			unique[v] = struct{}{}
		}
		ss := GetTimeSeriesFromPool()
		ss.Labels = labels.FromMap(lblsMap)

		sg.Series = append(sg.Series, ss)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strMap := make(map[string]uint32, 1_000*10)
		for _, ts := range sg.Series {
			ts.FillLabelMapping(strMap)
		}
	}
}

func BenchmarkDeserialize(b *testing.B) {
	sg := &SeriesGroup{
		Series: make([]*TimeSeriesBinary, 0),
	}
	for k := 0; k < 1_000; k++ {
		lblsMap := make(map[string]string)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key_%d", j)
			v := randString()
			lblsMap[key] = v
		}
		ss := GetTimeSeriesFromPool()
		ss.Labels = labels.FromMap(lblsMap)
		sg.Series = append(sg.Series, ss)
	}
	strMapToIndex := make(map[string]uint32, 1_000*10)
	for _, ts := range sg.Series {
		ts.FillLabelMapping(strMapToIndex)
	}
	stringsSlice := make([]ByteString, len(strMapToIndex))
	for stringValue, index := range strMapToIndex {
		stringsSlice[index] = ByteString(stringValue)
	}
	sg.Strings = stringsSlice

	var buf []byte
	buf, err := sg.MarshalMsg(buf)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		_, _, dErr := DeserializeToSeriesGroup(sg, buf)
		if dErr != nil {
			panic(dErr.Error())
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
