package mqadmin

import (
	"fmt"
	"strings"
	"testing"
)

func runCompareConfigCases(t *testing.T, h *compareHarness) {
	t.Helper()

	if _, err := h.cli.BrokerStatus(h.ctx, h.goBroker0); err != nil {
		t.Fatalf("go brokerStatus failed: %v", err)
	}
	if out, err := h.runJava("brokerStatus", "-n", h.javaNamesrv, "-b", h.broker0Svc); err != nil {
		t.Fatalf("java brokerStatus failed: %v, out=%s", err, out)
	}

	h.assertCrossMutate(t, "updateBrokerConfig",
		func() error {
			return h.cli.UpdateBrokerConfig(h.ctx, h.goBroker0, map[string]string{"autoCreateTopicEnable": "true"})
		},
		func() (string, error) {
			out, err := h.runJava("getBrokerConfig", "-n", h.javaNamesrv, "-b", h.broker0Svc)
			if err != nil {
				return out, err
			}
			if !strings.Contains(out, "autoCreateTopicEnable") {
				return out, fmt.Errorf("java getBrokerConfig missing autoCreateTopicEnable")
			}
			return out, nil
		},
		func() (string, error) {
			return h.runJava("updateBrokerConfig", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-k", "autoCreateTopicEnable", "-v", "false")
		},
		func() error {
			cfg, err := h.cli.GetBrokerConfig(h.ctx, h.goBroker0)
			if err != nil {
				return err
			}
			if cfg["autoCreateTopicEnable"] != "false" {
				return fmt.Errorf("autoCreateTopicEnable=%s expected false", cfg["autoCreateTopicEnable"])
			}
			return nil
		},
	)

	if _, err := h.cli.GetBrokerConfig(h.ctx, h.goBroker0); err != nil {
		t.Fatalf("go getBrokerConfig failed: %v", err)
	}
	if out, err := h.runJava("getBrokerConfig", "-n", h.javaNamesrv, "-b", h.broker0Svc); err != nil {
		t.Fatalf("java getBrokerConfig failed: %v, out=%s", err, out)
	}

	h.assertBoth(t, "updateKvConfig", func() error {
		return h.cli.UpdateKVConfig(h.ctx, KVConfig{Namespace: h.kvNS, Key: h.kvKey, Value: "v1"})
	}, func() (string, error) {
		return h.runJava("updateKvConfig", "-n", h.javaNamesrv, "-s", h.kvNS, "-k", h.kvKey, "-v", "v1")
	})

	h.assertBoth(t, "deleteKvConfig", func() error {
		return h.cli.DeleteKVConfig(h.ctx, h.kvNS, h.kvKey)
	}, func() (string, error) {
		return h.runJava("deleteKvConfig", "-n", h.javaNamesrv, "-s", h.kvNS, "-k", h.kvKey)
	})

	if _, err := h.cli.GetNamesrvConfig(h.ctx, []string{h.goNS}); err != nil {
		t.Fatalf("go getNamesrvConfig failed: %v", err)
	}
	if out, err := h.runJava("getNamesrvConfig", "-n", h.javaNamesrv); err != nil {
		t.Fatalf("java getNamesrvConfig failed: %v, out=%s", err, out)
	}

	h.assertBoth(t, "updateNamesrvConfig", func() error {
		return h.cli.UpdateNamesrvConfig(h.ctx, []string{h.goNS}, map[string]string{"orderMessageEnable": "false"})
	}, func() (string, error) {
		return h.runJava("updateNamesrvConfig", "-n", h.javaNamesrv, "-k", "orderMessageEnable", "-v", "false")
	})

	h.assertBoth(t, "wipeWritePerm", func() error {
		return h.cli.WipeWritePerm(h.ctx, "rocketmq-hhhhhzhen-0", []string{h.goNS})
	}, func() (string, error) {
		return h.runJava("wipeWritePerm", "-n", h.javaNamesrv, "-b", "rocketmq-hhhhhzhen-0")
	})

	h.assertBoth(t, "addWritePerm", func() error {
		return h.cli.AddWritePerm(h.ctx, "rocketmq-hhhhhzhen-0", []string{h.goNS})
	}, func() (string, error) {
		return h.runJava("addWritePerm", "-n", h.javaNamesrv, "-b", "rocketmq-hhhhhzhen-0")
	})

	if _, err := h.cli.ClusterList(h.ctx); err != nil {
		t.Fatalf("go clusterList failed: %v", err)
	}
	if out, err := h.runJava("clusterList", "-n", h.javaNamesrv); err != nil {
		t.Fatalf("java clusterList failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ClusterSendMsgRT(h.ctx, h.javaCluster); err != nil {
		t.Fatalf("go clusterSendMsgRT failed: %v", err)
	}

	if _, err := h.cli.StatsAll(h.ctx, h.topicA); err != nil {
		t.Fatalf("go statsAll failed: %v", err)
	}
	if out, err := h.runJava("statsAll", "-n", h.javaNamesrv, "-t", h.topicA); err != nil {
		t.Fatalf("java statsAll failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.BrokerConsumeStats(h.ctx, h.goBroker0, false, 3000); err != nil {
		t.Fatalf("go brokerConsumeStats failed: %v", err)
	}
	if out, err := h.runJava("brokerConsumeStats", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-o", "false", "-t", "3000"); err != nil {
		t.Fatalf("java brokerConsumeStats failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ProducerInfo(h.ctx, h.goBroker0); err != nil {
		t.Fatalf("go producer failed: %v", err)
	}
	if out, err := h.runJava("producer", "-n", h.javaNamesrv, "-b", h.broker0Svc); err != nil {
		t.Fatalf("java producer failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.SendMessage(h.ctx, SendMessageRequest{BrokerAddr: h.goBroker0, Topic: h.topicA, Body: "mqadmin-go-align-check", WaitStore: true}); err != nil {
		t.Fatalf("go sendMessage failed: %v", err)
	}
	if _, err := h.cli.QueryConsumeQueue(h.ctx, h.goBroker0, h.topicA, 0, 0, 1, h.groupA); err != nil {
		t.Fatalf("go queryConsumeQueue failed: %v", err)
	}
	if out, err := h.runJava("queryCq", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-t", h.topicA, "-q", "0", "-i", "0", "-c", "1", "-g", h.groupA); err != nil {
		t.Fatalf("java queryCq failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.HAStatus(h.ctx, h.goBroker0); err != nil {
		t.Logf("go haStatus skipped: %v", err)
	}
	if out, err := h.runJava("haStatus", "-n", h.javaNamesrv, "-b", h.broker0Svc); err != nil {
		t.Logf("java haStatus skipped: %v, out=%s", err, out)
	}

	if _, err := h.cli.GetColdDataFlowCtrInfo(h.ctx, h.goBroker0); err != nil {
		t.Logf("go getColdDataFlowCtrInfo skipped: %v", err)
	}
	if out, err := h.runJava("getColdDataFlowCtrInfo", "-n", h.javaNamesrv, "-b", h.broker0Svc); err != nil {
		t.Logf("java getColdDataFlowCtrInfo skipped: %v, out=%s", err, out)
	}
}
