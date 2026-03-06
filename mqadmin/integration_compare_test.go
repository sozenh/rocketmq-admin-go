package mqadmin

import "testing"

func TestIntegrationCompareWithJavaMQAdmin(t *testing.T) {
	h := newCompareHarness(t)
	runCompareTopicCases(t, h)
	runCompareGroupCases(t, h)
	runCompareOffsetCases(t, h)
	runCompareMessageCases(t, h)
	runCompareConfigCases(t, h)
	runCompareRemainingCases(t, h)
}
