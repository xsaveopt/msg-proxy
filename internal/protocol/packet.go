package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const MaxPayloadBytes = 2900

const (
	TypeConnect = "connect"
	TypeData    = "data"
	TypeDataAck = "dack"
	TypeClose   = "close"
	TypeAck     = "ack"
	TypeError   = "error"
)

type Packet struct {
	SessionID string `json:"s"`
	Seq       uint32 `json:"q"`
	Type      string `json:"t"`
	Target    string `json:"r,omitempty"`
	Payload   string `json:"p,omitempty"`
}

var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	zstdDecoder, _ = zstd.NewReader(nil)
)

func Encode(p *Packet) (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("marshal packet: %w", err)
	}
	return string(raw), nil
}

func Decode(s string) (*Packet, error) {
	var p Packet
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return nil, fmt.Errorf("unmarshal packet: %w", err)
	}
	return &p, nil
}

func EncodePayload(data []byte) string {
	compressed := zstdEncoder.EncodeAll(data, nil)
	return base64.StdEncoding.EncodeToString(compressed)
}

func DecodePayload(s string) ([]byte, error) {
	compressed, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("base64 decode payload: %w", err)
	}
	raw, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress payload: %w", err)
	}
	return raw, nil
}

func SplitData(data []byte) [][]byte {
	var chunks [][]byte
	for len(data) > 0 {
		n := MaxPayloadBytes
		if n > len(data) {
			n = len(data)
		}
		chunks = append(chunks, data[:n])
		data = data[n:]
	}
	return chunks
}
