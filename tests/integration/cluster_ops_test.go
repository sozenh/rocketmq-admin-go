package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/sozenh/rocketmq-admin-go/mqadmin"
)

func TestClusterList(t *testing.T) {
	accessKey := getenvDefault("RMQ_ACCESS_KEY", "rocketAdmin")
	secretKey := getenvDefault("RMQ_SECRET_KEY", "27LDO7PLS9KT5PE2EA3SSBLVXWV3")
	if accessKey == "" || secretKey == "" {
		t.Skip("skip: RMQ_ACCESS_KEY/RMQ_SECRET_KEY are required for real-environment cluster ops test")
	}

	ns := getenvDefault("RMQ_NS", "0.0.0.0:19876")

	cli, err := New(Options{
		NameServer:     []string{ns},
		Credentials:    Credentials{AccessKey: accessKey, SecretKey: secretKey},
		TimeoutMs:      8000,
		Retry:          1,
		RetryBackoffMs: 200,
	})
	if err != nil {
		t.Fatalf("new admin failed: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterInfo, err := cli.ClusterList(ctx)
	if err != nil {
		t.Fatalf("ClusterList failed: %v", err)
	}
	if clusterInfo == nil || len(clusterInfo.BrokerAddrTable) == 0 {
		t.Fatalf("ClusterList returned empty broker table: %+v", clusterInfo)
	}

	fmt.Println(clusterInfo)
}
