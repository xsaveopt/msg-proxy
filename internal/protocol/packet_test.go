package protocol

import (
	"bytes"
	"math"
	"math/rand"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	original := &Packet{
		SessionID: "test-session-123",
		Seq:       42,
		Type:      TypeData,
		Payload:   EncodePayload([]byte("hello world")),
	}

	encoded, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encoded == "" {
		t.Fatal("Encode returned empty string")
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.SessionID != original.SessionID {
		t.Errorf("SessionID: got %q, want %q", decoded.SessionID, original.SessionID)
	}
	if decoded.Seq != original.Seq {
		t.Errorf("Seq: got %d, want %d", decoded.Seq, original.Seq)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type: got %q, want %q", decoded.Type, original.Type)
	}
}

func TestEncodePayloadRoundtrip(t *testing.T) {
	data := []byte("some raw bytes for testing \x00\x01\x02\xff")
	encoded := EncodePayload(data)
	decoded, err := DecodePayload(encoded)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if !bytes.Equal(data, decoded) {
		t.Errorf("payload mismatch: got %v, want %v", decoded, data)
	}
}

func TestSplitData(t *testing.T) {
	small := bytes.Repeat([]byte("a"), 100)
	chunks := SplitData(small)
	if len(chunks) != 1 {
		t.Errorf("small: expected 1 chunk, got %d", len(chunks))
	}

	exact := bytes.Repeat([]byte("b"), MaxPayloadBytes)
	chunks = SplitData(exact)
	if len(chunks) != 1 {
		t.Errorf("exact: expected 1 chunk, got %d", len(chunks))
	}

	plus1 := bytes.Repeat([]byte("c"), MaxPayloadBytes+1)
	chunks = SplitData(plus1)
	if len(chunks) != 2 {
		t.Errorf("plus1: expected 2 chunks, got %d", len(chunks))
	}
	if len(chunks[0]) != MaxPayloadBytes {
		t.Errorf("plus1 first chunk: expected %d bytes, got %d", MaxPayloadBytes, len(chunks[0]))
	}
	if len(chunks[1]) != 1 {
		t.Errorf("plus1 second chunk: expected 1 byte, got %d", len(chunks[1]))
	}

	chunks = SplitData(nil)
	if len(chunks) != 0 {
		t.Errorf("nil: expected 0 chunks, got %d", len(chunks))
	}
}

func TestDecodeInvalid(t *testing.T) {
	_, err := Decode("not valid json!!!")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}

	_, err = Decode("42")
	if err == nil {
		t.Error("expected error for non-object JSON")
	}
}

func TestConnectPacket(t *testing.T) {
	p := &Packet{
		SessionID: "abc",
		Seq:       1,
		Type:      TypeConnect,
		Target:    "example.com:80",
	}
	encoded, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for _, ch := range encoded {
		if ch > 127 {
			t.Errorf("encoded contains non-ASCII char: %q", ch)
		}
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.SessionID != p.SessionID || decoded.Target != p.Target || decoded.Type != p.Type {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, p)
	}
	_ = strings.ToUpper
}

const telegramMessageLimit = 4096

func TestWorstCaseDataPacketFitsTelegramMessage(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20; i++ {
		chunk := make([]byte, MaxPayloadBytes)
		if _, err := rng.Read(chunk); err != nil {
			t.Fatalf("generate chunk: %v", err)
		}
		encoded, err := Encode(&Packet{
			SessionID: "ffffffff-ffff-ffff-ffff-ffffffffffff",
			Seq:       math.MaxUint32,
			Type:      TypeData,
			Payload:   EncodePayload(chunk),
		})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if len(encoded) >= telegramMessageLimit {
			t.Fatalf("incompressible %d-byte chunk encodes to %d chars, want under %d", MaxPayloadBytes, len(encoded), telegramMessageLimit)
		}
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	cases := map[string]struct {
		input string
		want  string
	}{
		"invalid base64":      {input: "***", want: "base64"},
		"base64 but not zstd": {input: "aGVsbG8gd29ybGQ=", want: "zstd"},
		"truncated zstd":      {input: EncodePayload(bytes.Repeat([]byte("abc"), 100))[:12], want: "zstd"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodePayload(tc.input)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodePayloadEmpty(t *testing.T) {
	data, err := DecodePayload(EncodePayload(nil))
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("got %d bytes, want 0", len(data))
	}
}
