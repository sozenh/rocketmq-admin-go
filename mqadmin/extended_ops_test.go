package mqadmin

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetConsumeModeValidation(t *testing.T) {
	c := &client{}
	ctx := context.Background()
	if err := c.SetConsumeMode(ctx, SetConsumeModeRequest{}); err != errEmptyBrokerAddr {
		t.Fatalf("expected errEmptyBrokerAddr, got %v", err)
	}
	if err := c.SetConsumeMode(ctx, SetConsumeModeRequest{BrokerAddr: "b"}); err != errEmptyTopic {
		t.Fatalf("expected errEmptyTopic, got %v", err)
	}
	if err := c.SetConsumeMode(ctx, SetConsumeModeRequest{BrokerAddr: "b", Topic: "t"}); err != errEmptyGroup {
		t.Fatalf("expected errEmptyGroup, got %v", err)
	}
	if err := c.SetConsumeMode(ctx, SetConsumeModeRequest{BrokerAddr: "b", Topic: "t", Group: "g"}); err != errEmptyMode {
		t.Fatalf("expected errEmptyMode, got %v", err)
	}
	if err := c.SetConsumeMode(ctx, SetConsumeModeRequest{BrokerAddr: "b", Topic: "t", Group: "g", Mode: "push"}); err != errUnsupportedMode {
		t.Fatalf("expected errUnsupportedMode, got %v", err)
	}
}

func TestUpdateStaticTopicValidation(t *testing.T) {
	c := &client{}
	ctx := context.Background()
	if err := c.UpdateStaticTopic(ctx, StaticTopicRequest{}); err != errEmptyBrokerAddr {
		t.Fatalf("expected errEmptyBrokerAddr, got %v", err)
	}
	if err := c.UpdateStaticTopic(ctx, StaticTopicRequest{BrokerAddr: "b"}); err != errEmptyTopic {
		t.Fatalf("expected errEmptyTopic, got %v", err)
	}
}

func TestCopyValidation(t *testing.T) {
	c := &client{}
	ctx := context.Background()
	if err := c.CopyUsers(ctx, "", "b", ""); err != errEmptyBrokerAddr {
		t.Fatalf("expected errEmptyBrokerAddr for CopyUsers, got %v", err)
	}
	if err := c.CopyAcls(ctx, "a", "", ""); err != errEmptyBrokerAddr {
		t.Fatalf("expected errEmptyBrokerAddr for CopyAcls, got %v", err)
	}
}

func TestUpdateOrderConfUsesKVRequests(t *testing.T) {
	var lastCode int32
	var lastExt atomic.Value
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		atomic.StoreInt32(&lastCode, int32(req.Code))
		lastExt.Store(req.ExtFields)
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
	})
	defer stop()

	c := &client{nameServer: []string{addr}, timeout: 2 * time.Second}
	ctx := context.Background()

	if err := c.UpdateOrderConf(ctx, "T", "broker-a:4"); err != nil {
		t.Fatalf("UpdateOrderConf put failed: %v", err)
	}
	if atomic.LoadInt32(&lastCode) != requestCodePutKVConfig {
		t.Fatalf("expected put kv code, got %d", atomic.LoadInt32(&lastCode))
	}
	ext, _ := lastExt.Load().(map[string]string)
	if ext["namespace"] != namespaceOrderTopicConfig || ext["key"] != "T" || ext["value"] != "broker-a:4" {
		t.Fatalf("unexpected ext fields: %+v", ext)
	}

	if err := c.UpdateOrderConf(ctx, "T", ""); err != nil {
		t.Fatalf("UpdateOrderConf delete failed: %v", err)
	}
	if atomic.LoadInt32(&lastCode) != requestCodeDeleteKVConfig {
		t.Fatalf("expected delete kv code, got %d", atomic.LoadInt32(&lastCode))
	}
	ext, _ = lastExt.Load().(map[string]string)
	if ext["namespace"] != namespaceOrderTopicConfig || ext["key"] != "T" {
		t.Fatalf("unexpected delete ext fields: %+v", ext)
	}
}

func TestAllocateMQByAveragely(t *testing.T) {
	var addr string
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeGetRouteInfoByTopic:
			body := []byte(`{"queueDatas":[{"brokerName":"broker-a","writeQueueNums":3,"perm":6},{"brokerName":"broker-b","writeQueueNums":2,"perm":6}],"brokerDatas":[{"cluster":"c0","brokerName":"broker-a","brokerAddrs":{"0":"` + addr + `"}},{"cluster":"c0","brokerName":"broker-b","brokerAddrs":{"0":"` + addr + `"}}]}`)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: body}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	})
	defer stop()

	c := &client{nameServer: []string{addr}, timeout: 2 * time.Second}
	ctx := context.Background()

	queues, err := c.AllocateMQ(ctx, "T", "c2", []string{"c3", "c1", "c2"})
	if err != nil {
		t.Fatalf("AllocateMQ failed: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("expected 2 allocated queues, got %d", len(queues))
	}
	if queues[0].BrokerName != "broker-a" || queues[0].QueueID != 1 {
		t.Fatalf("unexpected first queue: %+v", queues[0])
	}
	if queues[1].BrokerName != "broker-b" || queues[1].QueueID != 1 {
		t.Fatalf("unexpected second queue: %+v", queues[1])
	}
}

func TestStatsAllSingleTopic(t *testing.T) {
	var addr string
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeGetRouteInfoByTopic:
			body := []byte(`{"queueDatas":[{"brokerName":"broker-a","writeQueueNums":1,"perm":6}],"brokerDatas":[{"cluster":"c0","brokerName":"broker-a","brokerAddrs":{"0":"` + addr + `"}}]}`)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: body}
		case requestCodeGetTopicStatsInfo:
			payload := map[string]any{"offsetTable": map[string]any{"mq": map[string]any{"minOffset": 0, "maxOffset": 10}}}
			body, _ := json.Marshal(payload)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: body}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	})
	defer stop()

	c := &client{nameServer: []string{addr}, timeout: 2 * time.Second}
	ctx := context.Background()

	out, err := c.StatsAll(ctx, "T")
	if err != nil {
		t.Fatalf("StatsAll failed: %v", err)
	}
	if out["topicCount"].(int) != 1 {
		t.Fatalf("expected topicCount=1, got %+v", out["topicCount"])
	}
	topics, ok := out["topics"].(map[string]any)
	if !ok || topics["T"] == nil {
		t.Fatalf("missing topic stats: %+v", out)
	}
}

func TestLiteCommandsUseExpectedRequestCodes(t *testing.T) {
	var seen []int
	var addr string
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		seen = append(seen, req.Code)
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"ok":true}`)}
	})
	defer stop()

	c := &client{timeout: 2 * time.Second}
	ctx := context.Background()
	req := LiteTopicRequest{BrokerAddr: addr, ParentTopic: "p", LiteTopic: "l", Group: "g", ClientID: "c1", TopK: 10, MaxCount: 5}

	if _, err := c.GetBrokerLiteInfo(ctx, addr); err != nil {
		t.Fatalf("GetBrokerLiteInfo failed: %v", err)
	}
	if _, err := c.GetParentTopicInfo(ctx, addr, "p"); err != nil {
		t.Fatalf("GetParentTopicInfo failed: %v", err)
	}
	if _, err := c.GetLiteTopicInfo(ctx, req); err != nil {
		t.Fatalf("GetLiteTopicInfo failed: %v", err)
	}
	if _, err := c.GetLiteClientInfo(ctx, req); err != nil {
		t.Fatalf("GetLiteClientInfo failed: %v", err)
	}
	if _, err := c.GetLiteGroupInfo(ctx, req); err != nil {
		t.Fatalf("GetLiteGroupInfo failed: %v", err)
	}
	if err := c.TriggerLiteDispatch(ctx, req); err != nil {
		t.Fatalf("TriggerLiteDispatch failed: %v", err)
	}

	expected := []int{
		requestCodeGetBrokerLiteInfo,
		requestCodeGetParentTopicInfo,
		requestCodeGetLiteTopicInfo,
		requestCodeGetLiteClientInfo,
		requestCodeGetLiteGroupInfo,
		requestCodeTriggerLiteDispatch,
	}
	if len(seen) != len(expected) {
		t.Fatalf("unexpected request count: got=%d want=%d", len(seen), len(expected))
	}
	for i := range expected {
		if seen[i] != expected[i] {
			t.Fatalf("request code mismatch at %d: got=%d want=%d", i, seen[i], expected[i])
		}
	}
}
