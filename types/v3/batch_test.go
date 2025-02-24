package v3

import (
	"strconv"
	"testing"

	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestBatchMarshaling(t *testing.T) {
	tests := []struct {
		name    string
		metrics []*metricspb.ResourceMetrics
		wantErr bool
	}{
		{
			name: "single metric",
			metrics: []*metricspb.ResourceMetrics{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "test-service",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple metrics",
			metrics: []*metricspb.ResourceMetrics{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-1",
									},
								},
							},
						},
					},
				},
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-2",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new batch buffer
			bb := NewBatchBuffer()

			// Add metrics to the buffer
			err := bb.AddMetrics(&metricspb.MetricsData{
				ResourceMetrics: tt.metrics,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Marshal the batch
			var marshaledData []byte
			err = bb.Marshal(func(meta map[string]string, data []byte) error {
				marshaledData = make([]byte, len(data))
				copy(marshaledData, data)
				return nil
			})
			require.NoError(t, err)

			// Create a new batch buffer for unmarshaling
			bb2 := NewBatchBuffer()

			// Unmarshal the data
			items, err := bb2.Unmarshal(map[string]string{
				"record_count": strconv.Itoa(len(tt.metrics)),
			}, marshaledData)
			require.NoError(t, err)

			// Verify the number of items
			require.Equal(t, len(tt.metrics), len(items))

			// Verify each metric
			for i, item := range items {
				require.Equal(t, types.OtelMetricV1, item.Type())
				require.Equal(t, types.OtelFileVersionV3, item.FileFormat())

				// Unmarshal the original metric for comparison
				originalData, err := proto.Marshal(&metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{tt.metrics[i]},
				})
				require.NoError(t, err)

				// Compare the bytes
				require.Equal(t, item.Bytes(), originalData)

				// Verify we can unmarshal the data back into a metric
				var unmarshaled metricspb.MetricsData
				err = proto.Unmarshal(item.Bytes(), &unmarshaled)
				require.NoError(t, err)

				// Compare the unmarshaled metric with the original
				require.True(t, proto.Equal(&metricspb.MetricsData{
					ResourceMetrics: []*metricspb.ResourceMetrics{tt.metrics[i]},
				}, &unmarshaled))
			}
		})
	}
}

func TestBatchMarshalingWithNilMetrics(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddMetrics(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metrics data is nil")
}

func TestBatchMarshalingWithEmptyMetrics(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddMetrics(&metricspb.MetricsData{})
	require.NoError(t, err)

	var marshaledData []byte
	err = bb.Marshal(func(meta map[string]string, data []byte) error {
		marshaledData = make([]byte, len(data))
		copy(marshaledData, data)
		return nil
	})
	require.NoError(t, err)

	bb2 := NewBatchBuffer()
	items, err := bb2.Unmarshal(map[string]string{
		"record_count": "0",
	}, marshaledData)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestBatchMarshalingTraces(t *testing.T) {
	tests := []struct {
		name    string
		traces  []*tracepb.ResourceSpans
		wantErr bool
	}{
		{
			name: "single trace",
			traces: []*tracepb.ResourceSpans{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "test-service",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple traces",
			traces: []*tracepb.ResourceSpans{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-1",
									},
								},
							},
						},
					},
				},
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-2",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := NewBatchBuffer()

			err := bb.AddTraces(&tracepb.TracesData{
				ResourceSpans: tt.traces,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			var marshaledData []byte
			err = bb.Marshal(func(meta map[string]string, data []byte) error {
				marshaledData = make([]byte, len(data))
				copy(marshaledData, data)
				return nil
			})
			require.NoError(t, err)

			bb2 := NewBatchBuffer()
			items, err := bb2.Unmarshal(map[string]string{
				"record_count": strconv.Itoa(len(tt.traces)),
			}, marshaledData)
			require.NoError(t, err)

			require.Equal(t, len(tt.traces), len(items))

			for i, item := range items {
				require.Equal(t, types.OtelTraceV1, item.Type())
				require.Equal(t, types.OtelFileVersionV3, item.FileFormat())

				originalData, err := proto.Marshal(&tracepb.TracesData{
					ResourceSpans: []*tracepb.ResourceSpans{tt.traces[i]},
				})
				require.NoError(t, err)

				require.Equal(t, item.Bytes(), originalData)

				var unmarshaled tracepb.TracesData
				err = proto.Unmarshal(item.Bytes(), &unmarshaled)
				require.NoError(t, err)

				require.True(t, proto.Equal(&tracepb.TracesData{
					ResourceSpans: []*tracepb.ResourceSpans{tt.traces[i]},
				}, &unmarshaled))
			}
		})
	}
}

func TestBatchMarshalingLogs(t *testing.T) {
	tests := []struct {
		name    string
		logs    []*logspb.ResourceLogs
		wantErr bool
	}{
		{
			name: "single log",
			logs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "test-service",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple logs",
			logs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-1",
									},
								},
							},
						},
					},
				},
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "service-2",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := NewBatchBuffer()

			err := bb.AddLogs(&logspb.LogsData{
				ResourceLogs: tt.logs,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			var marshaledData []byte
			err = bb.Marshal(func(meta map[string]string, data []byte) error {
				marshaledData = make([]byte, len(data))
				copy(marshaledData, data)
				return nil
			})
			require.NoError(t, err)

			bb2 := NewBatchBuffer()
			items, err := bb2.Unmarshal(map[string]string{
				"record_count": strconv.Itoa(len(tt.logs)),
			}, marshaledData)
			require.NoError(t, err)

			require.Equal(t, len(tt.logs), len(items))

			for i, item := range items {
				require.Equal(t, types.OtelLogV1, item.Type())
				require.Equal(t, types.OtelFileVersionV3, item.FileFormat())

				originalData, err := proto.Marshal(&logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{tt.logs[i]},
				})
				require.NoError(t, err)

				require.Equal(t, item.Bytes(), originalData)

				var unmarshaled logspb.LogsData
				err = proto.Unmarshal(item.Bytes(), &unmarshaled)
				require.NoError(t, err)

				require.True(t, proto.Equal(&logspb.LogsData{
					ResourceLogs: []*logspb.ResourceLogs{tt.logs[i]},
				}, &unmarshaled))
			}
		})
	}
}

func TestBatchMarshalingWithNilTraces(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddTraces(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "traces data is nil")
}

func TestBatchMarshalingWithNilLogs(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddLogs(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "logs data is nil")
}

func TestBatchMarshalingWithEmptyTraces(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddTraces(&tracepb.TracesData{})
	require.NoError(t, err)

	var marshaledData []byte
	err = bb.Marshal(func(meta map[string]string, data []byte) error {
		marshaledData = make([]byte, len(data))
		copy(marshaledData, data)
		return nil
	})
	require.NoError(t, err)

	bb2 := NewBatchBuffer()
	items, err := bb2.Unmarshal(map[string]string{
		"record_count": "0",
	}, marshaledData)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestBatchMarshalingWithEmptyLogs(t *testing.T) {
	bb := NewBatchBuffer()
	err := bb.AddLogs(&logspb.LogsData{})
	require.NoError(t, err)

	var marshaledData []byte
	err = bb.Marshal(func(meta map[string]string, data []byte) error {
		marshaledData = make([]byte, len(data))
		copy(marshaledData, data)
		return nil
	})
	require.NoError(t, err)

	bb2 := NewBatchBuffer()
	items, err := bb2.Unmarshal(map[string]string{
		"record_count": "0",
	}, marshaledData)
	require.NoError(t, err)
	require.Empty(t, items)
}
