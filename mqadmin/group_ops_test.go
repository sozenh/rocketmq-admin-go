//go:build integration

package mqadmin

import (
	"fmt"
	"strings"
	"testing"
)

func runCompareGroupCases(t *testing.T, h *compareHarness) {
	t.Helper()

	h.assertCrossMutate(t, "updateSubGroup",
		func() error {
			if err := h.cli.UpdateSubGroup(h.ctx, h.goBroker0, SubscriptionGroupConfig{GroupName: h.groupA, ConsumeEnable: true, RetryQueueNums: 1}); err != nil {
				return err
			}
			return h.cli.UpdateSubGroup(h.ctx, h.goBroker1, SubscriptionGroupConfig{GroupName: h.groupA, ConsumeEnable: true, RetryQueueNums: 1})
		},
		func() (string, error) {
			return h.runJava("getConsumerConfig", "-n", h.javaNamesrv, "-g", h.groupA)
		},
		func() (string, error) {
			return h.runJava("updateSubGroup", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-g", h.groupB)
		},
		func() error {
			groups, err := h.cli.GetAllSubscriptionGroup(h.ctx, h.goBroker0)
			if err != nil {
				return err
			}
			if _, ok := groups.SubscriptionGroupTable[h.groupB]; !ok {
				return fmt.Errorf("group %s not found after java updateSubGroup", h.groupB)
			}
			return nil
		},
	)

	h.assertBoth(t, "updateSubGroupList", func() error {
		return h.cli.UpdateSubGroupList(h.ctx, h.goBroker0, []SubscriptionGroupConfig{{GroupName: h.groupB}, {GroupName: h.groupC}})
	}, func() (string, error) {
		shell := fmt.Sprintf("cat >/tmp/groups_%s.json <<'EOF'\n[{\"groupName\":\"%s\"},{\"groupName\":\"%s\"}]\nEOF\n%s updateSubGroupList -n %s -b %s -f /tmp/groups_%s.json", h.topicA, h.groupB, h.groupC, h.mqadminPath, h.javaNamesrv, h.broker0Svc, h.topicA)
		return h.runJavaShell(shell)
	})

	h.assertBoth(t, "deleteSubGroup", func() error {
		return h.cli.DeleteSubscriptionGroup(h.ctx, DeleteSubscriptionGroupRequest{BrokerAddr: h.goBroker0, GroupName: h.groupC, RemoveOffset: true})
	}, func() (string, error) {
		return h.runJava("deleteSubGroup", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-g", h.groupB, "-r", "true")
	})

	h.assertBoth(t, "setConsumeMode", func() error {
		return h.cli.SetConsumeMode(h.ctx, SetConsumeModeRequest{BrokerAddr: h.goBroker0, Topic: h.topicA, Group: h.groupA, Mode: "PULL"})
	}, func() (string, error) {
		return h.runJava("setConsumeMode", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-g", h.groupA, "-t", h.topicA, "-m", "PULL")
	})

	if _, err := h.cli.GetConsumerConfig(h.ctx, h.groupA); err != nil {
		t.Fatalf("go getConsumerConfig failed: %v", err)
	}
	if out, err := h.runJava("getConsumerConfig", "-n", h.javaNamesrv, "-g", h.groupA); err != nil {
		t.Fatalf("java getConsumerConfig failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ConsumerProgress(h.ctx, h.groupA, h.topicA); err != nil {
		t.Fatalf("go consumerProgress failed: %v", err)
	}
	if out, err := h.runJava("consumerProgress", "-n", h.javaNamesrv, "-g", h.groupA, "-t", h.topicA); err != nil {
		t.Fatalf("java consumerProgress failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ConsumerStatus(h.ctx, h.topicA, h.groupA, ""); err != nil {
		t.Fatalf("go consumerStatus failed: %v", err)
	}
	if out, err := h.runJava("consumerStatus", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-g", h.groupA); err != nil {
		if !strings.Contains(strings.ToLower(out), "not online") && !strings.Contains(strings.ToLower(out), "not found") {
			t.Fatalf("java consumerStatus failed: %v, out=%s", err, out)
		}
	}

	if _, err := h.cli.StartMonitoring(h.ctx, h.groupA, h.topicA); err != nil {
		t.Fatalf("go startMonitoring failed: %v", err)
	}
}
