package encoder

import "fmt"

// ProtoMarshaler is the interface implemented by types that can marshal themselves into valid Protobuf.
type ProtoMarshaler interface {
	MarshalProto() ([]byte, error)
}

// Unmarshaler is the interface implemented by types that can unmarshal a Protobuf description of themselves.
type ProtoUnmarshaler interface {
	UnmarshalProto([]byte) error
}

// Marshal returns the Protobuf encoding of v.
func ProtoMarshal(v ProtoMarshaler) ([]byte, error) {
	return v.MarshalProto()
}

// Unmarshal parses the encoded Protobuf data and stores the result in the value pointed to by v.
//
// TODO: If v is nil or not a pointer, Unmarshal returns an error.
func ProtoUnmarshal(data []byte, v ProtoUnmarshaler) error {
	return v.UnmarshalProto(data)
}

// Implements Codec interface.
type ProtoEncoder struct{}

func (ProtoEncoder) Marshal(v any) ([]byte, error) {
	m, ok := v.(ProtoMarshaler)
	if !ok {
		return nil, fmt.Errorf("encoder: value does not implement ProtoMarshaler")
	}
	return ProtoMarshal(m)
}

func (ProtoEncoder) Unmarshal(data []byte, out any) error {
	u, ok := out.(ProtoUnmarshaler)
	if !ok {
		return fmt.Errorf("encoder: target does not implement ProtoUnmarshaler")
	}
	return ProtoUnmarshal(data, u)
}
