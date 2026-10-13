package wire

import "testing"

func TestEnvelopeRoundTrip(t *testing.T) {
	b := MarshalEnvelope("demoMsg", []byte{1, 2, 3})
	env, err := UnmarshalEnvelope(b)
	if err != nil || env.TypeName != "demoMsg" || len(env.Payload) != 3 {
		t.Fatalf("env=%+v err=%v", env, err)
	}
}

func TestEnvelopeMalformed(t *testing.T) {
	if _, err := UnmarshalEnvelope([]byte{0x63 /*count=6 但无字段*/}); err == nil {
		t.Fatal("count mismatch must error (trap 8)")
	}
}
