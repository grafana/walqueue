package v3

import (
	"github.com/tinylib/msgp/msgp"
	"unsafe"
)

func (v *ByteString) UnmarshalMsg(bts []byte) (o []byte, err error) {
	*v, o, err = msgp.ReadStringZC(bts)
	return o, err
}

func (v *ByteString) MarshalMsg(bts []byte) (o []byte, err error) {
	buf := msgp.AppendString(bts, v.String())
	return buf, nil
}

func (v *ByteString) Msgsize() int {
	return msgp.StringPrefixSize + len(*v)
}

func (v *ByteString) DecodeMsg(dc *msgp.Reader) (err error) {
	s, err := dc.ReadStringAsBytes(nil)
	if err != nil {
		return err
	}
	*v = s
	return nil
}

func (v *ByteString) EncodeMsg(en *msgp.Writer) (err error) {
	return en.WriteString(v.String())
}

func (v ByteString) String() string {
	if len([]byte(v)) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len([]byte(v)))
}
