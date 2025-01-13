// Code generated by bencgen golang. DO NOT EDIT.
// source: types.benc

package v2

import (
    "github.com/deneonet/benc/std"
    "github.com/deneonet/benc/impl/gen"
)

// Struct - Metric
type Metric struct {
    Timestampmsvalue int64
    Hashvalue uint64
    IsHistogramvalue bool
    Buf []byte
}

// Reserved Ids - metric
var metricRIds = []uint16{}

// Size - metric
func (metric *Metric) Size() int {
    return metric.size(0)
}

// Nested Size - metric
func (metric *Metric) size(id uint16) (s int) {
    s += bstd.SizeInt64() + 2
    s += bstd.SizeUint64() + 2
    s += bstd.SizeBool() + 2
    s += bstd.SizeBytes(metric.Buf) + 2

    if id > 255 {
        s += 5
        return
    }
    s += 4
    return
}

// SizePlain - metric
func (metric *Metric) SizePlain() (s int) {
    s += bstd.SizeInt64()
    s += bstd.SizeUint64()
    s += bstd.SizeBool()
    s += bstd.SizeBytes(metric.Buf)
    return
}

// Marshal - metric
func (metric *Metric) Marshal(b []byte) {
    metric.marshal(0, b, 0)
}

// Nested Marshal - metric
func (metric *Metric) marshal(tn int, b []byte, id uint16) (n int) {
    n = bgenimpl.MarshalTag(tn, b, bgenimpl.Container, id)
    n = bgenimpl.MarshalTag(n, b, bgenimpl.Fixed64, 1)
    n = bstd.MarshalInt64(n, b, metric.Timestampmsvalue)
    n = bgenimpl.MarshalTag(n, b, bgenimpl.Fixed64, 2)
    n = bstd.MarshalUint64(n, b, metric.Hashvalue)
    n = bgenimpl.MarshalTag(n, b, bgenimpl.Fixed8, 3)
    n = bstd.MarshalBool(n, b, metric.IsHistogramvalue)
    n = bgenimpl.MarshalTag(n, b, bgenimpl.Bytes, 4)
    n = bstd.MarshalBytes(n, b, metric.Buf)

    n += 2
    b[n-2] = 1
    b[n-1] = 1
    return
}

// MarshalPlain - metric
func (metric *Metric) MarshalPlain(tn int, b []byte) (n int) {
    n = tn
    n = bstd.MarshalInt64(n, b, metric.Timestampmsvalue)
    n = bstd.MarshalUint64(n, b, metric.Hashvalue)
    n = bstd.MarshalBool(n, b, metric.IsHistogramvalue)
    n = bstd.MarshalBytes(n, b, metric.Buf)
    return n
}

// Unmarshal - metric
func (metric *Metric) Unmarshal(b []byte) (n int,err error) {
    n, err = metric.unmarshal(0, b, []uint16{}, 0)
    return
}

// Nested Unmarshal - metric
func (metric *Metric) unmarshal(tn int, b []byte, r []uint16, id uint16) (n int, err error) {
    var ok bool
    if n, ok, err = bgenimpl.HandleCompatibility(tn, b, r, id); !ok {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if n, ok, err = bgenimpl.HandleCompatibility(n, b, metricRIds, 1); err != nil {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if ok {
        if n, metric.Timestampmsvalue, err = bstd.UnmarshalInt64(n, b); err != nil {
            return
        }
    }
    if n, ok, err = bgenimpl.HandleCompatibility(n, b, metricRIds, 2); err != nil {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if ok {
        if n, metric.Hashvalue, err = bstd.UnmarshalUint64(n, b); err != nil {
            return
        }
    }
    if n, ok, err = bgenimpl.HandleCompatibility(n, b, metricRIds, 3); err != nil {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if ok {
        if n, metric.IsHistogramvalue, err = bstd.UnmarshalBool(n, b); err != nil {
            return
        }
    }
    if n, ok, err = bgenimpl.HandleCompatibility(n, b, metricRIds, 4); err != nil {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if ok {
        if n, metric.Buf, err = bstd.UnmarshalBytes(n, b); err != nil {
            return
        }
    }
    n += 2
    return
}

// UnmarshalPlain - metric
func (metric *Metric) UnmarshalPlain(tn int, b []byte) (n int, err error) {
    n = tn
    if n, metric.Timestampmsvalue, err = bstd.UnmarshalInt64(n, b); err != nil {
        return
    }
    if n, metric.Hashvalue, err = bstd.UnmarshalUint64(n, b); err != nil {
        return
    }
    if n, metric.IsHistogramvalue, err = bstd.UnmarshalBool(n, b); err != nil {
        return
    }
    if n, metric.Buf, err = bstd.UnmarshalBytes(n, b); err != nil {
        return
    }
    return
}

// Struct - Metadata
type Metadata struct {
    Buf []byte
}

// Reserved Ids - metadata
var metadataRIds = []uint16{}

// Size - metadata
func (metadata *Metadata) Size() int {
    return metadata.size(0)
}

// Nested Size - metadata
func (metadata *Metadata) size(id uint16) (s int) {
    s += bstd.SizeBytes(metadata.Buf) + 2

    if id > 255 {
        s += 5
        return
    }
    s += 4
    return
}

// SizePlain - metadata
func (metadata *Metadata) SizePlain() (s int) {
    s += bstd.SizeBytes(metadata.Buf)
    return
}

// Marshal - metadata
func (metadata *Metadata) Marshal(b []byte) {
    metadata.marshal(0, b, 0)
}

// Nested Marshal - metadata
func (metadata *Metadata) marshal(tn int, b []byte, id uint16) (n int) {
    n = bgenimpl.MarshalTag(tn, b, bgenimpl.Container, id)
    n = bgenimpl.MarshalTag(n, b, bgenimpl.Bytes, 1)
    n = bstd.MarshalBytes(n, b, metadata.Buf)

    n += 2
    b[n-2] = 1
    b[n-1] = 1
    return
}

// MarshalPlain - metadata
func (metadata *Metadata) MarshalPlain(tn int, b []byte) (n int) {
    n = tn
    n = bstd.MarshalBytes(n, b, metadata.Buf)
    return n
}

// Unmarshal - metadata
func (metadata *Metadata) Unmarshal(b []byte) (n int,err error) {
    n, err = metadata.unmarshal(0, b, []uint16{}, 0)
    return
}

// Nested Unmarshal - metadata
func (metadata *Metadata) unmarshal(tn int, b []byte, r []uint16, id uint16) (n int, err error) {
    var ok bool
    if n, ok, err = bgenimpl.HandleCompatibility(tn, b, r, id); !ok {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if n, ok, err = bgenimpl.HandleCompatibility(n, b, metadataRIds, 1); err != nil {
        if err == bgenimpl.ErrEof {
            return n, nil
        }
        return
    }
    if ok {
        if n, metadata.Buf, err = bstd.UnmarshalBytes(n, b); err != nil {
            return
        }
    }
    n += 2
    return
}

// UnmarshalPlain - metadata
func (metadata *Metadata) UnmarshalPlain(tn int, b []byte) (n int, err error) {
    n = tn
    if n, metadata.Buf, err = bstd.UnmarshalBytes(n, b); err != nil {
        return
    }
    return
}

