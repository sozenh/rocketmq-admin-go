package mqadmin

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func runCompareMessageCases(t *testing.T, h *compareHarness) {
	t.Helper()

	msgKey := fmt.Sprintf("k_%d", time.Now().UnixNano())
	body := fmt.Sprintf("mqadmin-go-msg-%d", time.Now().UnixNano())

	sendRet, err := h.cli.SendMessage(h.ctx, SendMessageRequest{
		BrokerAddr: h.goBroker0,
		Topic:      h.topicA,
		Body:       body,
		Keys:       msgKey,
		Tag:        "TAGA",
		WaitStore:  true,
	})
	if err != nil {
		t.Fatalf("go sendMessage failed: %v", err)
	}

	if _, err := h.cli.QueryMsgByKey(h.ctx, h.topicA, msgKey, 32, 0, time.Now().UnixMilli()); err != nil {
		t.Fatalf("go queryMsgByKey failed: %v", err)
	}
	if out, err := h.runJava("queryMsgByKey", "-n", h.javaNamesrv, "-t", h.topicA, "-k", msgKey, "-m", "32"); err != nil {
		t.Fatalf("java queryMsgByKey failed: %v, out=%s", err, out)
	}

	msgID := ""
	if v, ok := sendRet["msgId"].(string); ok {
		msgID = strings.TrimSpace(v)
	}
	if msgID == "" {
		if v, ok := sendRet["offsetMsgId"].(string); ok {
			msgID = strings.TrimSpace(v)
		}
	}
	if msgID != "" {
		if _, err := h.cli.QueryMsgByID(h.ctx, h.goBroker0, msgID); err != nil {
			t.Fatalf("go queryMsgById failed: %v", err)
		}
		if out, err := h.runJava("queryMsgById", "-n", h.javaNamesrv, "-t", h.topicA, "-i", msgID); err != nil {
			t.Fatalf("java queryMsgById failed: %v, out=%s", err, out)
		}
	}
}
