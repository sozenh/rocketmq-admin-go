package mqadmin

import "testing"

func TestDecodeRouteDataWithNumericKeys(t *testing.T) {
	body := []byte(`{"brokerDatas":[{"brokerAddrs":{0:"10.0.0.1:10911",2:"10.0.0.2:10911"},"brokerName":"b0","cluster":"c0"}]}`)
	route, err := decodeRouteData(body)
	if err != nil {
		t.Fatalf("decodeRouteData failed: %v", err)
	}
	if len(route.BrokerDatas) != 1 || route.BrokerDatas[0].BrokerAddrs["0"] == "" {
		t.Fatalf("unexpected decoded route: %+v", route)
	}
}

func TestDecodeResetOffsetBodyWithObjectMapKey(t *testing.T) {
	body := []byte(`{"offsetTable":{{"brokerName":"RaftNode00","queueId":0,"topic":"topicB"}:11110}}`)
	m := decodeResetOffsetBody(body)
	offsetTable, ok := m["offsetTable"].(map[string]any)
	if !ok || len(offsetTable) != 1 {
		t.Fatalf("unexpected offsetTable: %#v", m)
	}
}
