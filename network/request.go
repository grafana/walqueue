package network

import (
	"bytes"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/walqueue/types"
)

// generateWriteRequest creates a proto prombpb.WriteRequest from manual bytes. Since
// the data is already serialized and we just need to wrap it in a proto message.
func generateWriteRequest[T any](series []T, input []byte) ([]byte, error) {
	bb := bytes.NewBuffer(input)
	for _, t := range series {
		// This conversion is necessary to test for a specific interface.
		mm, valid := interface{}(t).(types.MetricDatum)
		if valid {
			buf := mm.Bytes()
			size := proto.EncodeVarint(uint64(len(buf)))
			// This is the prompb constant for timeseries
			bb.WriteByte(0xa)
			bb.Write(size)
			bb.Write(buf)
			continue
		}
		md, valid := interface{}(t).(types.MetadataDatum)
		if valid {
			buf := md.Bytes()
			size := proto.EncodeVarint(uint64(len(buf)))
			// This is the prompb constant for metadata
			bb.WriteByte(0x1a)
			bb.Write(size)
			bb.Write(buf)
			continue
		}
		return nil, fmt.Errorf("unknown data type %T", t)
	}

	return bb.Bytes(), nil
}
