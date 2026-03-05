package mqadmin

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestEncodeDecodeFrameRoundTrip(t *testing.T) {
	cmd := &remotingCommand{
		Code:      requestCodeGetAllTopicListFromNameSrv,
		Language:  languageGo,
		Version:   remotingVersion,
		Opaque:    42,
		Flag:      0,
		Remark:    "ok",
		ExtFields: map[string]string{"topic": "T"},
		Body:      []byte(`{"k":"v"}`),
	}

	packet, err := encodeFrame(cmd)
	if err != nil {
		t.Fatalf("encodeFrame failed: %v", err)
	}
	if len(packet) < 8 {
		t.Fatalf("encoded packet too short: %d", len(packet))
	}

	frameLen := int(binary.BigEndian.Uint32(packet[:4]))
	if frameLen != len(packet)-4 {
		t.Fatalf("frameLen mismatch, got %d want %d", frameLen, len(packet)-4)
	}

	decoded, err := decodeFrame(packet[4:])
	if err != nil {
		t.Fatalf("decodeFrame failed: %v", err)
	}
	if decoded.Code != cmd.Code || decoded.Opaque != cmd.Opaque || decoded.ExtFields["topic"] != "T" {
		t.Fatalf("decoded command mismatch: %+v", decoded)
	}
	if string(decoded.Body) != string(cmd.Body) {
		t.Fatalf("decoded body mismatch: got %q want %q", string(decoded.Body), string(cmd.Body))
	}
}

func TestPropertiesCodec(t *testing.T) {
	in := map[string]string{"b": "2", "a": "1"}
	bytes := propertiesToBytes(in)
	out := bytesToProperties(bytes)
	if len(out) != 2 || out["a"] != "1" || out["b"] != "2" {
		t.Fatalf("properties codec mismatch: %+v", out)
	}
}

func TestInvokeSyncWithRetryStopsOnRemotingError(t *testing.T) {
	addr, calls, stop := startMockServer(t, func(req *remotingCommand, n int32) *remotingCommand {
		return &remotingCommand{Code: 17, Remark: "TOPIC_NOT_EXIST", Opaque: req.Opaque}
	})
	defer stop()

	_, err := invokeSyncWithRetry(context.Background(), addr, false, 2*time.Second, newCommand(requestCodeGetAllTopicListFromNameSrv, nil), nil, 3, 1*time.Millisecond)
	if err == nil {
		t.Fatal("expected remoting error, got nil")
	}
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("expected single attempt on remoting error, got %d", got)
	}
}

func TestInvokeSyncWithRetryRetriesOnIOError(t *testing.T) {
	addr, calls, stop := startMockServer(t, func(req *remotingCommand, n int32) *remotingCommand {
		if n == 1 {
			return nil
		}
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"ok":true}`)}
	})
	defer stop()

	resp, err := invokeSyncWithRetry(context.Background(), addr, false, 2*time.Second, newCommand(requestCodeGetAllTopicListFromNameSrv, nil), nil, 2, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if resp == nil || string(resp.Body) != `{"ok":true}` {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if got := atomic.LoadInt32(calls); got < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", got)
	}
}

func startMockServer(t *testing.T, handler func(req *remotingCommand, call int32) *remotingCommand) (string, *int32, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	var calls int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				n := atomic.AddInt32(&calls, 1)
				lenBuf := make([]byte, 4)
				if _, err := io.ReadFull(c, lenBuf); err != nil {
					return
				}
				frameLen := int(binary.BigEndian.Uint32(lenBuf))
				frame := make([]byte, frameLen)
				if _, err := io.ReadFull(c, frame); err != nil {
					return
				}
				req, err := decodeFrame(frame)
				if err != nil {
					return
				}
				resp := handler(req, n)
				if resp == nil {
					return
				}
				packet, err := encodeFrame(resp)
				if err != nil {
					return
				}
				_, _ = c.Write(packet)
			}(conn)
		}
	}()

	stop := func() {
		_ = ln.Close()
		<-done
	}
	return ln.Addr().String(), &calls, stop
}
