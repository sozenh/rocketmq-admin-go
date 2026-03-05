package mqadmin

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestControllerOpsRequestCodes(t *testing.T) {
	var mu sync.Mutex
	seen := []int{}
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		mu.Lock()
		seen = append(seen, req.Code)
		mu.Unlock()
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"ok":true}`)}
	})
	defer stop()

	c := &client{timeout: 2 * time.Second}
	ctx := context.Background()

	if err := c.AddBroker(ctx, addr, BrokerMembershipRequest{ClusterName: "c", BrokerName: "b", BrokerID: 1, BrokerAddr: "127.0.0.1:10911"}); err != nil {
		t.Fatalf("AddBroker failed: %v", err)
	}
	if err := c.RemoveBroker(ctx, addr, BrokerMembershipRequest{ClusterName: "c", BrokerName: "b", BrokerID: 1, BrokerAddr: "127.0.0.1:10911"}); err != nil {
		t.Fatalf("RemoveBroker failed: %v", err)
	}
	if _, err := c.GetControllerMetaData(ctx, addr); err != nil {
		t.Fatalf("GetControllerMetaData failed: %v", err)
	}
	if _, err := c.GetControllerConfig(ctx, addr); err != nil {
		t.Fatalf("GetControllerConfig failed: %v", err)
	}
	if err := c.UpdateControllerConfig(ctx, addr, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("UpdateControllerConfig failed: %v", err)
	}
	if _, err := c.ReElectMaster(ctx, addr, ElectMasterRequest{ClusterName: "c", BrokerName: "b", BrokerID: 1}); err != nil {
		t.Fatalf("ReElectMaster failed: %v", err)
	}
	if _, err := c.CleanControllerBrokerMeta(ctx, addr, CleanBrokerDataRequest{ClusterName: "c", BrokerName: "b", BrokerID: 1}); err != nil {
		t.Fatalf("CleanControllerBrokerMeta failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	expected := []int{
		requestCodeAddBroker,
		requestCodeRemoveBroker,
		requestCodeControllerGetMetadataInfo,
		requestCodeGetControllerConfig,
		requestCodeUpdateControllerConfig,
		requestCodeControllerGetMetadataInfo,
		requestCodeControllerElectMaster,
		requestCodeControllerGetMetadataInfo,
		requestCodeCleanBrokerData,
	}
	if len(seen) != len(expected) {
		t.Fatalf("unexpected call count: got=%d want=%d", len(seen), len(expected))
	}
	for i := range expected {
		if seen[i] != expected[i] {
			t.Fatalf("unexpected code at %d: got=%d want=%d", i, seen[i], expected[i])
		}
	}
}

func TestBrokerMaintenanceRequestCodes(t *testing.T) {
	var mu sync.Mutex
	seen := []int{}
	addr, _, stop := startMockServer(t, func(req *remotingCommand, _ int32) *remotingCommand {
		mu.Lock()
		seen = append(seen, req.Code)
		mu.Unlock()
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"ok":true}`)}
	})
	defer stop()

	c := &client{timeout: 2 * time.Second, nameServer: []string{addr}}
	ctx := context.Background()

	if _, err := c.BrokerConsumeStats(ctx, addr, false, 3000); err != nil {
		t.Fatalf("BrokerConsumeStats failed: %v", err)
	}
	if err := c.CleanExpiredCQ(ctx, addr); err != nil {
		t.Fatalf("CleanExpiredCQ failed: %v", err)
	}
	if err := c.DeleteExpiredCommitLog(ctx, addr); err != nil {
		t.Fatalf("DeleteExpiredCommitLog failed: %v", err)
	}
	if err := c.CleanUnusedTopic(ctx, addr); err != nil {
		t.Fatalf("CleanUnusedTopic failed: %v", err)
	}
	if _, err := c.GetColdDataFlowCtrInfo(ctx, addr); err != nil {
		t.Fatalf("GetColdDataFlowCtrInfo failed: %v", err)
	}
	if err := c.UpdateColdDataFlowCtrGroupConfig(ctx, addr, "g", "10"); err != nil {
		t.Fatalf("UpdateColdDataFlowCtrGroupConfig failed: %v", err)
	}
	if err := c.RemoveColdDataFlowCtrGroupConfig(ctx, addr, "g"); err != nil {
		t.Fatalf("RemoveColdDataFlowCtrGroupConfig failed: %v", err)
	}
	if err := c.SetCommitLogReadAheadMode(ctx, addr, "RANDOM"); err != nil {
		t.Fatalf("SetCommitLogReadAheadMode failed: %v", err)
	}
	if err := c.SwitchTimerEngine(ctx, addr); err != nil {
		t.Fatalf("SwitchTimerEngine failed: %v", err)
	}
	if _, err := c.HAStatus(ctx, addr); err != nil {
		t.Fatalf("HAStatus failed: %v", err)
	}
	if _, err := c.GetBrokerEpoch(ctx, addr); err != nil {
		t.Fatalf("GetBrokerEpoch failed: %v", err)
	}
	if _, err := c.QueryConsumeQueue(ctx, addr, "T", 0, 0, 1, ""); err != nil {
		t.Fatalf("QueryConsumeQueue failed: %v", err)
	}
	if _, err := c.CheckRocksdbCqWriteProgress(ctx, addr, "T", false); err != nil {
		t.Fatalf("CheckRocksdbCqWriteProgress failed: %v", err)
	}
	if _, err := c.RocksDBConfigToJSON(ctx, addr, "topics", true); err != nil {
		t.Fatalf("RocksDBConfigToJSON failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 14 {
		t.Fatalf("expected at least 14 requests, got %d", len(seen))
	}
}
