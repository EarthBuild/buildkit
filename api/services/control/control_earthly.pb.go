// Earthly-specific protobuf types for the Control service.
// These are hand-written to match the proto definitions in control.proto
// that were added by Earthly. They implement the proto.Marshaler and
// proto.Unmarshaler interfaces so that gRPC can serialize them.

package moby_buildkit_v1

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// --- ShutdownIfIdleRequest ---

type ShutdownIfIdleRequest struct{}

func (m *ShutdownIfIdleRequest) Reset()         { *m = ShutdownIfIdleRequest{} }
func (m *ShutdownIfIdleRequest) String() string { return "ShutdownIfIdleRequest{}" }
func (m *ShutdownIfIdleRequest) ProtoMessage()  {}

func (m *ShutdownIfIdleRequest) Marshal() ([]byte, error)   { return nil, nil }
func (m *ShutdownIfIdleRequest) MarshalVT() ([]byte, error) { return nil, nil }
func (m *ShutdownIfIdleRequest) Unmarshal(data []byte) error {
	return skipAllFields(data)
}
func (m *ShutdownIfIdleRequest) UnmarshalVT(data []byte) error { return m.Unmarshal(data) }

// --- ShutdownIfIdleResponse ---

type ShutdownIfIdleResponse struct {
	WillShutdown bool   `protobuf:"varint,1,opt,name=willShutdown,proto3" json:"willShutdown,omitempty"`
	NumSessions  uint64 `protobuf:"varint,2,opt,name=numSessions,proto3" json:"numSessions,omitempty"`
}

func (m *ShutdownIfIdleResponse) Reset()         { *m = ShutdownIfIdleResponse{} }
func (m *ShutdownIfIdleResponse) String() string { return fmt.Sprintf("%+v", *m) }
func (m *ShutdownIfIdleResponse) ProtoMessage()  {}

func (m *ShutdownIfIdleResponse) GetWillShutdown() bool {
	if m != nil {
		return m.WillShutdown
	}
	return false
}

func (m *ShutdownIfIdleResponse) GetNumSessions() uint64 {
	if m != nil {
		return m.NumSessions
	}
	return 0
}

func (m *ShutdownIfIdleResponse) Marshal() ([]byte, error) { return m.MarshalVT() }
func (m *ShutdownIfIdleResponse) Unmarshal(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *ShutdownIfIdleResponse) MarshalVT() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	if m.WillShutdown {
		b = protowire.AppendTag(b, 1, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	if m.NumSessions != 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, m.NumSessions)
	}
	return b, nil
}

func (m *ShutdownIfIdleResponse) UnmarshalVT(data []byte) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			if typ != protowire.VarintType {
				return fmt.Errorf("proto: wrong wire type for ShutdownIfIdleResponse.willShutdown")
			}
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			data = data[vn:]
			m.WillShutdown = v != 0
		case 2:
			if typ != protowire.VarintType {
				return fmt.Errorf("proto: wrong wire type for ShutdownIfIdleResponse.numSessions")
			}
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			data = data[vn:]
			m.NumSessions = v
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return nil
}

// --- ReserveRequest ---

type ReserveRequest struct{}

func (m *ReserveRequest) Reset()         { *m = ReserveRequest{} }
func (m *ReserveRequest) String() string { return "ReserveRequest{}" }
func (m *ReserveRequest) ProtoMessage()  {}

func (m *ReserveRequest) Marshal() ([]byte, error)         { return nil, nil }
func (m *ReserveRequest) MarshalVT() ([]byte, error)       { return nil, nil }
func (m *ReserveRequest) Unmarshal(data []byte) error       { return skipAllFields(data) }
func (m *ReserveRequest) UnmarshalVT(data []byte) error     { return m.Unmarshal(data) }

// --- ReserveResponse ---

type ReserveResponse struct{}

func (m *ReserveResponse) Reset()         { *m = ReserveResponse{} }
func (m *ReserveResponse) String() string { return "ReserveResponse{}" }
func (m *ReserveResponse) ProtoMessage()  {}

func (m *ReserveResponse) Marshal() ([]byte, error)         { return nil, nil }
func (m *ReserveResponse) MarshalVT() ([]byte, error)       { return nil, nil }
func (m *ReserveResponse) Unmarshal(data []byte) error       { return skipAllFields(data) }
func (m *ReserveResponse) UnmarshalVT(data []byte) error     { return m.Unmarshal(data) }

// --- SessionHistoryRequest ---

type SessionHistoryRequest struct{}

func (m *SessionHistoryRequest) Reset()         { *m = SessionHistoryRequest{} }
func (m *SessionHistoryRequest) String() string { return "SessionHistoryRequest{}" }
func (m *SessionHistoryRequest) ProtoMessage()  {}

func (m *SessionHistoryRequest) Marshal() ([]byte, error)     { return nil, nil }
func (m *SessionHistoryRequest) MarshalVT() ([]byte, error)   { return nil, nil }
func (m *SessionHistoryRequest) Unmarshal(data []byte) error   { return skipAllFields(data) }
func (m *SessionHistoryRequest) UnmarshalVT(data []byte) error { return m.Unmarshal(data) }

// --- SessionHistoryResponse ---

type SessionHistoryResponse struct {
	History []*SessionHistoryResponse_History `protobuf:"bytes,1,rep,name=history,proto3" json:"history,omitempty"`
}

func (m *SessionHistoryResponse) Reset()         { *m = SessionHistoryResponse{} }
func (m *SessionHistoryResponse) String() string { return fmt.Sprintf("%+v", *m) }
func (m *SessionHistoryResponse) ProtoMessage()  {}

func (m *SessionHistoryResponse) GetHistory() []*SessionHistoryResponse_History {
	if m != nil {
		return m.History
	}
	return nil
}

func (m *SessionHistoryResponse) Marshal() ([]byte, error) { return m.MarshalVT() }
func (m *SessionHistoryResponse) Unmarshal(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SessionHistoryResponse) MarshalVT() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	for _, h := range m.History {
		hData, err := h.MarshalVT()
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, hData)
	}
	return b, nil
}

func (m *SessionHistoryResponse) UnmarshalVT(data []byte) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for SessionHistoryResponse.history")
			}
			val, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			data = data[vn:]
			h := &SessionHistoryResponse_History{}
			if err := h.UnmarshalVT(val); err != nil {
				return err
			}
			m.History = append(m.History, h)
		default:
			cn := protowire.ConsumeFieldValue(num, typ, data)
			if cn < 0 {
				return protowire.ParseError(cn)
			}
			data = data[cn:]
		}
	}
	return nil
}

// --- SessionHistoryResponse_History ---

type SessionHistoryResponse_History struct {
	SessionID string     `protobuf:"bytes,1,opt,name=sessionID,proto3" json:"sessionID,omitempty"`
	Start     *time.Time `protobuf:"bytes,2,opt,name=start,proto3,stdtime" json:"start,omitempty"`
	End       *time.Time `protobuf:"bytes,3,opt,name=end,proto3,stdtime" json:"end,omitempty"`
}

func (m *SessionHistoryResponse_History) Reset() { *m = SessionHistoryResponse_History{} }
func (m *SessionHistoryResponse_History) String() string {
	return fmt.Sprintf("%+v", *m)
}
func (m *SessionHistoryResponse_History) ProtoMessage() {}

func (m *SessionHistoryResponse_History) GetSessionID() string {
	if m != nil {
		return m.SessionID
	}
	return ""
}

func (m *SessionHistoryResponse_History) GetStart() *time.Time {
	if m != nil {
		return m.Start
	}
	return nil
}

func (m *SessionHistoryResponse_History) GetEnd() *time.Time {
	if m != nil {
		return m.End
	}
	return nil
}

func (m *SessionHistoryResponse_History) Marshal() ([]byte, error) { return m.MarshalVT() }
func (m *SessionHistoryResponse_History) Unmarshal(data []byte) error {
	return m.UnmarshalVT(data)
}

func (m *SessionHistoryResponse_History) MarshalVT() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	var b []byte
	if m.SessionID != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.SessionID)
	}
	if m.Start != nil {
		ts := timestamppb.New(*m.Start)
		tsData, err := marshalTimestamp(ts)
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, tsData)
	}
	if m.End != nil {
		ts := timestamppb.New(*m.End)
		tsData, err := marshalTimestamp(ts)
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, tsData)
	}
	return b, nil
}

func (m *SessionHistoryResponse_History) UnmarshalVT(data []byte) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for SessionHistoryResponse_History.sessionID")
			}
			s, sn := protowire.ConsumeString(data)
			if sn < 0 {
				return protowire.ParseError(sn)
			}
			data = data[sn:]
			m.SessionID = s
		case 2:
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for SessionHistoryResponse_History.start")
			}
			val, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			data = data[vn:]
			ts, err := unmarshalTimestamp(val)
			if err != nil {
				return err
			}
			t := ts.AsTime()
			m.Start = &t
		case 3:
			if typ != protowire.BytesType {
				return fmt.Errorf("proto: wrong wire type for SessionHistoryResponse_History.end")
			}
			val, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			data = data[vn:]
			ts, err := unmarshalTimestamp(val)
			if err != nil {
				return err
			}
			t := ts.AsTime()
			m.End = &t
		default:
			cn := protowire.ConsumeFieldValue(num, typ, data)
			if cn < 0 {
				return protowire.ParseError(cn)
			}
			data = data[cn:]
		}
	}
	return nil
}

// --- helpers ---

func skipAllFields(data []byte) error {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		cn := protowire.ConsumeFieldValue(num, typ, data)
		if cn < 0 {
			return protowire.ParseError(cn)
		}
		data = data[cn:]
	}
	return nil
}

func marshalTimestamp(ts *timestamppb.Timestamp) ([]byte, error) {
	if ts == nil {
		return nil, nil
	}
	var b []byte
	if ts.Seconds != 0 {
		b = protowire.AppendTag(b, 1, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(ts.Seconds))
	}
	if ts.Nanos != 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(ts.Nanos))
	}
	return b, nil
}

func unmarshalTimestamp(data []byte) (*timestamppb.Timestamp, error) {
	ts := &timestamppb.Timestamp{}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		switch num {
		case 1:
			if typ != protowire.VarintType {
				return nil, fmt.Errorf("proto: wrong wire type for Timestamp.seconds")
			}
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return nil, protowire.ParseError(vn)
			}
			data = data[vn:]
			ts.Seconds = int64(v)
		case 2:
			if typ != protowire.VarintType {
				return nil, fmt.Errorf("proto: wrong wire type for Timestamp.nanos")
			}
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return nil, protowire.ParseError(vn)
			}
			data = data[vn:]
			ts.Nanos = int32(v)
		default:
			cn := protowire.ConsumeFieldValue(num, typ, data)
			if cn < 0 {
				return nil, protowire.ParseError(cn)
			}
			data = data[cn:]
		}
	}
	return ts, nil
}
