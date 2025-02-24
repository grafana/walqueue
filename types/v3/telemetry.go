package v3

import (
	"hash/fnv"
)

// SignalType represents the type of OpenTelemetry signal
type SignalType int8

const (
	SignalTypeMetrics SignalType = iota
	SignalTypeTraces
	SignalTypeLogs
)

// TelemetryDatum is the base interface for all telemetry data
type TelemetryDatum interface {
	// Hash returns a consistent hash for the datum
	Hash() uint64
	// Bytes returns the raw protobuf bytes
	Bytes() []byte
	// Type returns the signal type
	Type() SignalType
}

// RawTelemetry implements TelemetryDatum and holds raw protobuf data
type RawTelemetry struct {
	data       []byte
	signalType SignalType
	hash       uint64
}

// NewRawTelemetry creates a new RawTelemetry instance
func NewRawTelemetry(data []byte, signalType SignalType) *RawTelemetry {
	h := fnv.New64a()
	h.Write(data)
	return &RawTelemetry{
		data:       data,
		signalType: signalType,
		hash:       h.Sum64(),
	}
}

func (r *RawTelemetry) Hash() uint64 {
	return r.hash
}

func (r *RawTelemetry) Bytes() []byte {
	return r.data
}

func (r *RawTelemetry) Type() SignalType {
	return r.signalType
}

// MetricDatum represents OpenTelemetry metric data
type MetricDatum struct {
	*RawTelemetry
}

// NewMetricDatum creates a new MetricDatum instance
func NewMetricDatum(data []byte) *MetricDatum {
	return &MetricDatum{
		RawTelemetry: NewRawTelemetry(data, SignalTypeMetrics),
	}
}

// TraceDatum represents OpenTelemetry trace data
type TraceDatum struct {
	*RawTelemetry
}

// NewTraceDatum creates a new TraceDatum instance
func NewTraceDatum(data []byte) *TraceDatum {
	return &TraceDatum{
		RawTelemetry: NewRawTelemetry(data, SignalTypeTraces),
	}
}

// LogDatum represents OpenTelemetry log data
type LogDatum struct {
	*RawTelemetry
}

// NewLogDatum creates a new LogDatum instance
func NewLogDatum(data []byte) *LogDatum {
	return &LogDatum{
		RawTelemetry: NewRawTelemetry(data, SignalTypeLogs),
	}
}
