// Earthly-specific protobuf types for the Export RPC.
// These are hand-written to match the proto definitions in gateway.proto
// that were added by Earthly. They implement the legacy proto.Marshaler
// and proto.Unmarshaler interfaces so that gRPC can serialize them.

package moby_buildkit_v1_frontend

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// ExportRequest is an earthly-specific message for the Export RPC.
type ExportRequest struct {
	Refs     *RefMap           `protobuf:"bytes,1,opt,name=refs,proto3" json:"refs,omitempty"`
	Metadata map[string][]byte `protobuf:"bytes,2,rep,name=metadata,proto3" json:"metadata,omitempty"`
}

func (m *ExportRequest) Reset()         { *m = ExportRequest{} }
func (m *ExportRequest) String() string { return fmt.Sprintf("%+v", *m) }
func (m *ExportRequest) ProtoMessage()  {}

func (m *ExportRequest) GetRefs() *RefMap {
	if m != nil {
		return m.Refs
	}
	return nil
}

func (m *ExportRequest) GetMetadata() map[string][]byte {
	if m != nil {
		return m.Metadata
	}
	return nil
}

func (m *ExportRequest) Marshal() ([]byte, error) {
	return m.MarshalVT()
}

func (m *ExportRequest) Unmarshal(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *ExportRequest) MarshalVT() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte

	// Field 1: RefMap refs
	if m.Refs != nil {
		refsData, err := m.Refs.MarshalVT()
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, refsData)
	}

	// Field 2: map<string, bytes> metadata
	for k, v := range m.Metadata {
		// Each map entry is a submessage with key=1, value=2
		var entry []byte
		entry = protowire.AppendTag(entry, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, k)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendBytes(entry, v)

		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, entry)
	}

	return b, nil
}

func (m *ExportRequest) UnmarshalVT(data []byte) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		switch num {
		case 1: // refs
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for ExportRequest.refs")
			}
			val, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			m.Refs = &RefMap{}
			if err := m.Refs.UnmarshalVT(val); err != nil {
				return err
			}

		case 2: // metadata map entry
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for ExportRequest.metadata")
			}
			val, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]

			if m.Metadata == nil {
				m.Metadata = make(map[string][]byte)
			}

			var key string
			var value []byte
			for len(val) > 0 {
				fnum, _, fn := protowire.ConsumeTag(val)
				if fn < 0 {
					return protowire.ParseError(fn)
				}
				val = val[fn:]
				switch fnum {
				case 1: // key
					s, sn := protowire.ConsumeString(val)
					if sn < 0 {
						return protowire.ParseError(sn)
					}
					key = s
					val = val[sn:]
				case 2: // value
					bv, bn := protowire.ConsumeBytes(val)
					if bn < 0 {
						return protowire.ParseError(bn)
					}
					value = append([]byte(nil), bv...)
					val = val[bn:]
				default:
					_, _, skipN := protowire.ConsumeField(val)
					if skipN < 0 {
						return protowire.ParseError(skipN)
					}
					val = val[skipN:]
				}
			}
			m.Metadata[key] = value

		default:
			_, _, skipN := protowire.ConsumeField(data)
			if skipN < 0 {
				return protowire.ParseError(skipN)
			}
			data = data[skipN:]
		}
	}
	return nil
}

// ExportResponse is an earthly-specific message for the Export RPC.
type ExportResponse struct{}

func (m *ExportResponse) Reset()         { *m = ExportResponse{} }
func (m *ExportResponse) String() string { return "ExportResponse{}" }
func (m *ExportResponse) ProtoMessage()  {}

func (m *ExportResponse) Marshal() ([]byte, error) {
	return nil, nil
}

func (m *ExportResponse) Unmarshal(data []byte) error {
	// Skip all fields
	for len(data) > 0 {
		_, _, skipN := protowire.ConsumeField(data)
		if skipN < 0 {
			return protowire.ParseError(skipN)
		}
		data = data[skipN:]
	}
	return nil
}

func (m *ExportResponse) MarshalVT() ([]byte, error) {
	return nil, nil
}

func (m *ExportResponse) UnmarshalVT(data []byte) error {
	return m.Unmarshal(data)
}
