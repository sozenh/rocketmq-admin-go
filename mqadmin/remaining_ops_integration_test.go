//go:build integration

package mqadmin

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func runCompareRemainingCases(t *testing.T, h *compareHarness) {
	t.Helper()

	h.assertBoth(t, "resetMasterFlushOffset", func() error {
		return h.cli.ResetMasterFlushOffset(h.ctx, h.goBroker0, 0)
	}, func() (string, error) {
		if !h.javaCommandExists("resetMasterFlushOffset") {
			return "", nil
		}
		return h.runJava("resetMasterFlushOffset", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-o", "0")
	})

	h.assertBoth(t, "cleanExpiredCQ", func() error {
		return h.cli.CleanExpiredCQ(h.ctx, h.goBroker0)
	}, func() (string, error) {
		if !h.javaCommandExists("cleanExpiredCQ") {
			return "", nil
		}
		return h.runJava("cleanExpiredCQ", "-n", h.javaNamesrv, "-b", h.broker0Svc)
	})

	h.assertBoth(t, "deleteExpiredCommitLog", func() error {
		return h.cli.DeleteExpiredCommitLog(h.ctx, h.goBroker0)
	}, func() (string, error) {
		if !h.javaCommandExists("deleteExpiredCommitLog") {
			return "", nil
		}
		return h.runJava("deleteExpiredCommitLog", "-n", h.javaNamesrv, "-b", h.broker0Svc)
	})

	h.assertBoth(t, "cleanUnusedTopic", func() error {
		return h.cli.CleanUnusedTopic(h.ctx, h.goBroker0)
	}, func() (string, error) {
		if !h.javaCommandExists("cleanUnusedTopic") {
			return "", nil
		}
		return h.runJava("cleanUnusedTopic", "-n", h.javaNamesrv, "-b", h.broker0Svc)
	})

	h.assertBoth(t, "checkRocksdbCqWriteProgress", func() error {
		_, err := h.cli.CheckRocksdbCqWriteProgress(h.ctx, h.goBroker0, h.topicA, false)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("checkRocksdbCqWriteProgress") {
			return "", nil
		}
		return h.runJava("checkRocksdbCqWriteProgress", "-n", h.javaNamesrv, "-c", h.javaCluster, "-t", h.topicA)
	})

	h.assertBoth(t, "rocksDBConfigToJson", func() error {
		_, err := h.cli.RocksDBConfigToJSON(h.ctx, h.goBroker0, "topics", true)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("rocksDBConfigToJson") {
			return "", nil
		}
		return h.runJava("rocksDBConfigToJson", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-t", "topics", "-j", "true")
	})

	h.assertBoth(t, "exportMetadata", func() error {
		_, err := h.cli.ExportMetadata(h.ctx)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("exportMetadata") {
			return "", nil
		}
		return h.runJava("exportMetadata", "-n", h.javaNamesrv, "-c", h.javaCluster, "-f", "/tmp/export-metadata.json")
	})

	h.assertBoth(t, "exportConfigs", func() error {
		_, err := h.cli.ExportConfigs(h.ctx)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("exportConfigs") {
			return "", nil
		}
		return h.runJava("exportConfigs", "-n", h.javaNamesrv, "-c", h.javaCluster, "-f", "/tmp/export-configs.json")
	})

	h.assertBoth(t, "exportMetrics", func() error {
		_, err := h.cli.ExportMetrics(h.ctx)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("exportMetrics") {
			return "", nil
		}
		return h.runJava("exportMetrics", "-n", h.javaNamesrv, "-c", h.javaCluster, "-f", "/tmp/export-metrics.json")
	})

	h.assertBoth(t, "exportPopRecord", func() error {
		_, err := h.cli.ExportPopRecord(h.ctx, h.goBroker0)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("exportPopRecord") {
			return "", nil
		}
		return h.runJava("exportPopRecord", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-d", "true")
	})

	assertDualOutcome(t, "producerConnection",
		func() error {
			_, err := h.cli.ProducerConnection(h.ctx, "mqadmin-go", h.topicA)
			return err
		},
		func() (string, error) {
			if !h.javaCommandExists("producerConnection") {
				return "", nil
			}
			return h.runJava("producerConnection", "-n", h.javaNamesrv, "-g", "mqadmin-go", "-t", h.topicA)
		},
	)

	assertDualOutcome(t, "consumerConnection",
		func() error {
			_, err := h.cli.ConsumerConnection(h.ctx, h.groupA)
			return err
		},
		func() (string, error) {
			if !h.javaCommandExists("consumerConnection") {
				return "", nil
			}
			return h.runJava("consumerConnection", "-n", h.javaNamesrv, "-g", h.groupA, "-b", h.broker0Svc)
		},
	)

	h.assertBoth(t, "sendMsgStatus", func() error {
		_, err := h.cli.SendMsgStatus(h.ctx, h.goBroker0, h.topicA, 5)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("sendMsgStatus") {
			return "", nil
		}
		return h.runJava("sendMsgStatus", "-n", h.javaNamesrv, "-b", h.brokerName0, "-c", "5", "-s", "64")
	})

	msgKey := fmt.Sprintf("remain_%d", time.Now().UnixNano())
	sendRet, err := h.cli.SendMessage(h.ctx, SendMessageRequest{BrokerAddr: h.goBroker0, Topic: h.topicA, Body: "remaining-case", Keys: msgKey, Tag: "TAGB", WaitStore: true})
	if err != nil {
		t.Fatalf("go send message for remaining cases failed: %v", err)
	}

	h.assertBoth(t, "queryMsgByUniqueKey", func() error {
		_, err := h.cli.QueryMsgByUniqueKey(h.ctx, h.topicA, msgKey)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("queryMsgByKey") {
			return "", nil
		}
		return h.runJava("queryMsgByKey", "-n", h.javaNamesrv, "-t", h.topicA, "-k", msgKey)
	})

	h.assertBoth(t, "queryMsgByOffset", func() error {
		_, err := h.cli.QueryMsgByOffset(h.ctx, h.topicA, 0, 0)
		return err
	}, func() (string, error) {
		if !h.javaCommandExists("queryMsgByOffset") {
			return "", nil
		}
		return h.runJava("queryMsgByOffset", "-n", h.javaNamesrv, "-b", h.brokerName0, "-t", h.topicA, "-i", "0", "-o", "0")
	})

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
		h.assertBoth(t, "printMessage", func() error {
			_, err := h.cli.PrintMessage(h.ctx, h.goBroker0, msgID)
			return err
		}, func() (string, error) {
			if !h.javaCommandExists("queryMsgById") {
				return "", nil
			}
			return h.runJava("queryMsgById", "-n", h.javaNamesrv, "-t", h.topicA, "-i", msgID)
		})
	}
}

func assertDualOutcome(t *testing.T, name string, goFn func() error, javaFn func() (string, error)) {
	t.Helper()
	gErr := goFn()
	out, jErr := javaFn()
	jFailed := jErr != nil || javaOutputFailed(out)
	if gErr == nil && !jFailed {
		return
	}
	if gErr != nil && jFailed {
		gmsg := strings.ToLower(gErr.Error())
		jmsg := strings.ToLower(out)
		if strings.Contains(gmsg, "not online") || strings.Contains(gmsg, "not found") || strings.Contains(gmsg, "not exist") || strings.Contains(jmsg, "not online") || strings.Contains(jmsg, "not found") || strings.Contains(jmsg, "not exist") {
			return
		}
	}
	if gErr != nil {
		t.Fatalf("%s go failed: %v, javaErr=%v out=%s", name, gErr, jErr, out)
	}
	t.Fatalf("%s java failed: %v, out=%s", name, jErr, out)
}

func javaOutputFailed(out string) bool {
	l := strings.ToLower(out)
	return strings.Contains(l, "subcommandexception") || strings.Contains(l, "command failed") || strings.Contains(l, "exception:")
}
