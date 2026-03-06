package mqadmin

import (
	"context"
	"encoding/json"
)

type ClusterBrokerTable struct {
	Cluster            string            `json:"cluster"`
	BrokerName         string            `json:"brokerName"`
	BrokerAddrs        map[string]string `json:"brokerAddrs"`
	EnableActingMaster bool              `json:"enableActingMaster"`
}

type ClusterInfo struct {
	ClusterAddrTable map[string][]string           `json:"clusterAddrTable"`
	BrokerAddrTable  map[string]ClusterBrokerTable `json:"brokerAddrTable"`
}

type ClusterSendMsgRTResult struct {
	Cluster string       `json:"cluster"`
	Note    string       `json:"note"`
	Data    *ClusterInfo `json:"data"`
}

func (c *client) ClusterList(ctx context.Context) (*ClusterInfo, error) {
	resp, err := c.invokeNameServer(ctx, newCommand(requestCodeGetBrokerClusterInfo, nil))
	if err != nil {
		return nil, err
	}
	var clusterInfo ClusterInfo
	err = json.Unmarshal(JavaFastJsonConvert(resp.Body), &clusterInfo)
	if err != nil {
		return nil, err
	}
	return &clusterInfo, nil
}

func (c *client) ClusterSendMsgRT(ctx context.Context, clusterName string) (*ClusterSendMsgRTResult, error) {
	info, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	return &ClusterSendMsgRTResult{
		Cluster: clusterName,
		Note:    "clusterSendMsgRT in mqadmin-go currently provides broker runtime snapshot, not active message benchmark",
		Data:    info,
	}, nil
}
