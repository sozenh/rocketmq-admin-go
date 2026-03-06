package mqadmin

import (
	"testing"
	"time"
)

func runCompareOffsetCases(t *testing.T, h *compareHarness) {
	t.Helper()

	h.assertBoth(t, "resetOffsetByTime", func() error {
		_, err := h.cli.ResetOffsetByTime(h.ctx, ResetOffsetRequest{Topic: h.topicA, Group: h.groupA, Timestamp: time.Now().UnixMilli(), Force: true})
		return err
	}, func() (string, error) {
		return h.runJava("resetOffsetByTime", "-n", h.javaNamesrv, "-g", h.groupA, "-t", h.topicA, "-s", "now", "-f", "true")
	})

	h.assertBoth(t, "skipAccumulatedMessage", func() error {
		_, err := h.cli.SkipAccumulation(h.ctx, h.topicA, h.groupA, "")
		return err
	}, func() (string, error) {
		return h.runJava("skipAccumulatedMessage", "-n", h.javaNamesrv, "-g", h.groupA, "-t", h.topicA, "-f", "true")
	})

	h.assertBoth(t, "cloneGroupOffset", func() error {
		return h.cli.CloneGroupOffset(h.ctx, CloneGroupOffsetRequest{SrcGroup: h.groupA, DestGroup: h.groupB, Topic: h.topicA, Offline: true})
	}, func() (string, error) {
		return h.runJava("cloneGroupOffset", "-n", h.javaNamesrv, "-s", h.groupA, "-d", h.groupB, "-t", h.topicA, "-o", "true")
	})
}
