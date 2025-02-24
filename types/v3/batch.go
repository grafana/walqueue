package v3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/grafana/walqueue/types"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// telemetryDatum implements types.Datum for OpenTelemetry data
type telemetryDatum struct {
	data       []byte
	signalType types.Type
	hash       uint64
}

func (d *telemetryDatum) Hash() uint64 {
	return d.hash
}

func (d *telemetryDatum) Bytes() []byte {
	return d.data
}

func (d *telemetryDatum) Type() types.Type {
	return d.signalType
}

func (d *telemetryDatum) FileFormat() types.FileFormat {
	return types.OtelFileVersionV3
}

func (d *telemetryDatum) Free() {
	d.data = nil
	d.hash = 0
}

// BatchBuffer manages buffering of telemetry data
type BatchBuffer struct {
	mu           sync.Mutex
	currentBatch *bytes.Buffer
	totalItems   int
}

// NewBatchBuffer creates a new batch buffer
func NewBatchBuffer() *BatchBuffer {
	return &BatchBuffer{
		currentBatch: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
	}
}

// AddMetrics adds OpenTelemetry metrics to the buffer, creating a separate datum for each ResourceMetrics
func (b *BatchBuffer) AddMetrics(metrics *metricspb.MetricsData) error {
	if metrics == nil {
		return fmt.Errorf("metrics data is nil")
	}

	for _, rm := range metrics.ResourceMetrics {
		// Create a new MetricsData containing just this ResourceMetrics
		singleMetric := &metricspb.MetricsData{
			ResourceMetrics: []*metricspb.ResourceMetrics{rm},
		}

		data, err := proto.Marshal(singleMetric)
		if err != nil {
			return fmt.Errorf("failed to marshal resource metrics: %w", err)
		}

		if err := b.addToBuffer(data, SignalTypeMetrics); err != nil {
			return fmt.Errorf("failed to add resource metrics: %w", err)
		}
	}

	return nil
}

// AddTraces adds OpenTelemetry traces to the buffer, creating a separate datum for each ResourceSpans
func (b *BatchBuffer) AddTraces(traces *tracepb.TracesData) error {
	if traces == nil {
		return fmt.Errorf("traces data is nil")
	}

	for _, rs := range traces.ResourceSpans {
		// Create a new TracesData containing just this ResourceSpans
		singleTrace := &tracepb.TracesData{
			ResourceSpans: []*tracepb.ResourceSpans{rs},
		}

		data, err := proto.Marshal(singleTrace)
		if err != nil {
			return fmt.Errorf("failed to marshal resource spans: %w", err)
		}

		if err := b.addToBuffer(data, SignalTypeTraces); err != nil {
			return fmt.Errorf("failed to add resource spans: %w", err)
		}
	}

	return nil
}

// AddLogs adds OpenTelemetry logs to the buffer, creating a separate datum for each ResourceLogs
func (b *BatchBuffer) AddLogs(logs *logspb.LogsData) error {
	if logs == nil {
		return fmt.Errorf("logs data is nil")
	}

	for _, rl := range logs.ResourceLogs {
		// Create a new LogsData containing just this ResourceLogs
		singleLog := &logspb.LogsData{
			ResourceLogs: []*logspb.ResourceLogs{rl},
		}

		data, err := proto.Marshal(singleLog)
		if err != nil {
			return fmt.Errorf("failed to marshal resource logs: %w", err)
		}

		if err := b.addToBuffer(data, SignalTypeLogs); err != nil {
			return fmt.Errorf("failed to add resource logs: %w", err)
		}
	}

	return nil
}

// addToBuffer adds marshaled data to the buffer with its signal type
func (b *BatchBuffer) addToBuffer(data []byte, signalType SignalType) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Write signal type
	if err := b.currentBatch.WriteByte(byte(signalType)); err != nil {
		return fmt.Errorf("failed to write signal type: %w", err)
	}

	// Write data length
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(data)))
	if _, err := b.currentBatch.Write(lenBytes); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write data
	if _, err := b.currentBatch.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	b.totalItems++
	return nil
}

// Marshal serializes the batch int.buf.Reset()o a byte slice using benc encoding
func (b *BatchBuffer) Marshal(handle func(map[string]string, []byte) error) error {
	defer func() {
		b.currentBatch.Reset()
		b.totalItems = 0
	}()
	buf := b.currentBatch.Bytes()
	meta := make(map[string]string)
	meta["record_count"] = strconv.Itoa(int(b.totalItems))
	return handle(meta, buf)
}

// Unmarshal deserializes the batch from a byte slice using benc encoding
func (b *BatchBuffer) Unmarshal(meta map[string]string, buf []byte) ([]types.Datum, error) {
	count, err := strconv.Atoi(meta["record_count"])
	if err != nil {
		return nil, fmt.Errorf("failed to convert record count to int: %w", err)
	}

	items := make([]types.Datum, 0, count)
	index := 0
	for i := 0; i < count; i++ {
		signalType := SignalType(buf[index])
		index++
		if err != nil {
			return nil, fmt.Errorf("failed to read signal type: %w", err)
		}

		length := binary.BigEndian.Uint32(buf[index : index+4])
		index += 4
		data := buf[index : index+int(length)]
		index += int(length)
		ot := &Otel{}
		err = ot.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}

		var sType types.Type
		switch signalType {
		case SignalTypeMetrics:
			sType = types.OtelMetricV1
		case SignalTypeTraces:
			sType = types.OtelTraceV1
		case SignalTypeLogs:
			sType = types.OtelLogV1
		}
		items = append(items, &telemetryDatum{
			data:       data,
			signalType: sType,
			hash:       ot.Hashvalue,
		})
	}

	return items, nil
}
