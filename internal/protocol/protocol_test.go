package protocol

import "testing"

func TestMessageTypeValuesMatchWireSpec(t *testing.T) {
	// 附录 B：数值即契约。
	cases := map[MessageType]uint8{
		MsgRequest: 1, MsgResponse: 2, MsgOneWay: 3, MsgError: 4,
		MsgMigrationPackage: 9, MsgPing: 10, MsgPong: 11,
		MsgHandshake: 12, MsgHandshakeAck: 13, MsgRetire: 14,
	}
	for m, v := range cases {
		if uint8(m) != v {
			t.Fatalf("MessageType %d = %d, want %d", m, uint8(m), v)
		}
	}
}

func TestMessageFlagBitsDisjoint(t *testing.T) {
	all := []MessageFlag{FlagHasStatus, FlagHasMessagerHash, FlagHasMethodHash,
		FlagHasSourceNodeID, FlagHasTargetUUID, FlagHasCausalContext, FlagForwarded}
	var acc MessageFlag
	for _, f := range all {
		if acc&f != 0 {
			t.Fatalf("flag bits overlap at %b", f)
		}
		acc |= f
	}
}

func TestIsControlPlane(t *testing.T) {
	for _, m := range []MessageType{MsgHandshake, MsgHandshakeAck, MsgPing, MsgPong, MsgRetire} {
		if !IsControlPlane(m) {
			t.Fatalf("%d must be control plane", m)
		}
	}
	for _, m := range []MessageType{MsgRequest, MsgResponse, MsgOneWay, MsgError, MsgMigrationPackage} {
		if IsControlPlane(m) {
			t.Fatalf("%d must not be control plane", m)
		}
	}
}
