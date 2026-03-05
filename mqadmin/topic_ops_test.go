//go:build integration

package mqadmin

import (
	"fmt"
	"strings"
	"testing"
)

func runCompareTopicCases(t *testing.T, h *compareHarness) {
	t.Helper()

	h.assertCrossMutate(t, "updateTopic",
		func() error {
			if err := h.cli.UpdateTopic(h.ctx, CreateTopicRequest{Topic: h.topicA, BrokerAddr: h.goBroker0, ReadQueueNums: 4, WriteQueueNums: 4}); err != nil {
				return err
			}
			return h.cli.UpdateTopic(h.ctx, CreateTopicRequest{Topic: h.topicA, BrokerAddr: h.goBroker1, ReadQueueNums: 4, WriteQueueNums: 4})
		},
		func() (string, error) {
			return h.runJava("topicRoute", "-n", h.javaNamesrv, "-t", h.topicA)
		},
		func() (string, error) {
			return h.runJava("updateTopic", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-t", h.topicA, "-r", "8", "-w", "8")
		},
		func() error {
			_, err := h.cli.TopicRoute(h.ctx, h.topicA)
			return err
		},
	)

	h.assertBoth(t, "updateTopicList", func() error {
		return h.cli.UpdateTopicList(h.ctx, h.goBroker0, []CreateTopicRequest{{Topic: h.topicB, ReadQueueNums: 4, WriteQueueNums: 4}, {Topic: h.topicC, ReadQueueNums: 4, WriteQueueNums: 4}})
	}, func() (string, error) {
		shell := fmt.Sprintf("cat >/tmp/topics_%s.json <<'EOF'\n[{\"topicName\":\"%s\",\"readQueueNums\":4,\"writeQueueNums\":4,\"perm\":6,\"topicFilterType\":\"SINGLE_TAG\"},{\"topicName\":\"%s\",\"readQueueNums\":4,\"writeQueueNums\":4,\"perm\":6,\"topicFilterType\":\"SINGLE_TAG\"}]\nEOF\n%s updateTopicList -n %s -b %s -f /tmp/topics_%s.json", h.topicA, h.topicB, h.topicC, h.mqadminPath, h.javaNamesrv, h.broker0Svc, h.topicA)
		return h.runJavaShell(shell)
	})

	h.assertBoth(t, "updateTopicPerm", func() error {
		return h.cli.UpdateTopicPerm(h.ctx, UpdateTopicPermRequest{Topic: h.topicA, Perm: 6})
	}, func() (string, error) {
		return h.runJava("updateTopicPerm", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-t", h.topicA, "-p", "6")
	})

	h.assertBoth(t, "updateOrderConf", func() error {
		return h.cli.UpdateOrderConf(h.ctx, h.topicA, "rocketmq-hhhhhzhen-0:4")
	}, func() (string, error) {
		return h.runJava("updateOrderConf", "-n", h.javaNamesrv, "-t", h.topicA, "-m", "put", "-v", "rocketmq-hhhhhzhen-0:4")
	})

	h.assertBoth(t, "allocateMQ", func() error {
		_, err := h.cli.AllocateMQ(h.ctx, h.topicA, "cid-1", []string{"cid-1", "cid-2"})
		return err
	}, func() (string, error) {
		return h.runJava("allocateMQ", "-n", h.javaNamesrv, "-t", h.topicA, "-i", "cid-1,cid-2")
	})

	goList, err := h.cli.TopicList(h.ctx)
	if err != nil {
		t.Fatalf("go topicList failed: %v", err)
	}
	javaTopicListOut, err := h.runJava("topicList", "-n", h.javaNamesrv)
	if err != nil {
		t.Fatalf("java topicList failed: %v, out=%s", err, javaTopicListOut)
	}
	if !contains(goList.TopicList, h.topicA) || !strings.Contains(javaTopicListOut, h.topicA) {
		t.Fatalf("topicList mismatch: goHas=%v javaOut=%s", contains(goList.TopicList, h.topicA), javaTopicListOut)
	}

	if _, err := h.cli.TopicRoute(h.ctx, h.topicA); err != nil {
		t.Fatalf("go topicRoute failed: %v", err)
	}
	if out, err := h.runJava("topicRoute", "-n", h.javaNamesrv, "-t", h.topicA); err != nil {
		t.Fatalf("java topicRoute failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.TopicStatus(h.ctx, h.topicA); err != nil {
		t.Fatalf("go topicStatus failed: %v", err)
	}
	if out, err := h.runJava("topicStatus", "-n", h.javaNamesrv, "-t", h.topicA); err != nil {
		t.Fatalf("java topicStatus failed: %v, out=%s", err, out)
	}

	clusters, err := h.cli.TopicCluster(h.ctx, h.topicA)
	if err != nil {
		t.Fatalf("go topicClusterList failed: %v", err)
	}
	javaClusterOut, err := h.runJava("topicClusterList", "-n", h.javaNamesrv, "-t", h.topicA)
	if err != nil {
		t.Fatalf("java topicClusterList failed: %v, out=%s", err, javaClusterOut)
	}
	if len(clusters) == 0 {
		t.Fatalf("go topicCluster empty")
	}
}
