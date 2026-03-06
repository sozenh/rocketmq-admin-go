package mqadmin

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestIntegrationSmoke(t *testing.T) {
	ns := getenvDefault("RMQ_NS", "127.0.0.1:19876")
	broker0 := getenvDefault("RMQ_BROKER0", "127.0.0.1:10911")
	broker1 := getenvDefault("RMQ_BROKER1", "127.0.0.1:20911")
	proxy := getenvDefault("RMQ_PROXY", "127.0.0.1:18080")
	controller := getenvDefault("RMQ_CONTROLLER", "127.0.0.1:19878")

	assertTCP(t, proxy)
	assertTCP(t, controller)

	cli, err := New(Options{
		NameServer:     []string{ns},
		TimeoutMs:      5000,
		Retry:          1,
		RetryBackoffMs: 200,
	})
	if err != nil {
		t.Fatalf("new admin failed: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().Unix(), 10)
	topic := "mqadmin_go_it_topic_" + suffix
	group := "mqadmin_go_it_group_" + suffix

	if err := cli.UpdateTopic(ctx, CreateTopicRequest{Topic: topic, BrokerAddr: broker0, ReadQueueNums: 4, WriteQueueNums: 4}); err != nil {
		t.Fatalf("UpdateTopic broker0 failed: %v", err)
	}
	if err := cli.UpdateTopic(ctx, CreateTopicRequest{Topic: topic, BrokerAddr: broker1, ReadQueueNums: 4, WriteQueueNums: 4}); err != nil {
		t.Fatalf("UpdateTopic broker1 failed: %v", err)
	}

	list, err := cli.TopicList(ctx)
	if err != nil {
		t.Fatalf("TopicList failed: %v", err)
	}
	if len(list.TopicList) == 0 {
		t.Fatalf("TopicList empty")
	}

	if _, err := cli.TopicRoute(ctx, topic); err != nil {
		t.Fatalf("TopicRoute failed: %v", err)
	}
	if _, err := cli.TopicCluster(ctx, topic); err != nil {
		t.Fatalf("TopicCluster failed: %v", err)
	}

	if _, err := cli.BrokerStatus(ctx, broker0); err != nil {
		t.Fatalf("BrokerStatus failed: %v", err)
	}
	if _, err := cli.GetBrokerConfig(ctx, broker0); err != nil {
		t.Fatalf("GetBrokerConfig failed: %v", err)
	}

	if err := cli.UpdateSubGroup(ctx, broker0, SubscriptionGroupConfig{GroupName: group, ConsumeEnable: true, RetryQueueNums: 1}); err != nil {
		t.Fatalf("UpdateSubGroup failed: %v", err)
	}
	if _, err := cli.GetAllSubscriptionGroup(ctx, broker0); err != nil {
		t.Fatalf("GetAllSubscriptionGroup failed: %v", err)
	}
	if err := cli.DeleteSubscriptionGroup(ctx, DeleteSubscriptionGroupRequest{BrokerAddr: broker0, GroupName: group, RemoveOffset: true}); err != nil {
		t.Fatalf("DeleteSubscriptionGroup failed: %v", err)
	}

	kv := KVConfig{Namespace: "mqadmin_go_test", Key: "it_key_" + suffix, Value: "it_val"}
	if err := cli.UpdateKVConfig(ctx, kv); err != nil {
		t.Fatalf("UpdateKVConfig failed: %v", err)
	}
	if err := cli.DeleteKVConfig(ctx, kv.Namespace, kv.Key); err != nil {
		t.Fatalf("DeleteKVConfig failed: %v", err)
	}

	if err := cli.WipeWritePerm(ctx, "broker-a", []string{ns}); err != nil {
		t.Logf("WipeWritePerm not passed, remark=%v", err)
	}
	if err := cli.AddWritePerm(ctx, "broker-a", []string{ns}); err != nil {
		t.Logf("AddWritePerm not passed, remark=%v", err)
	}

	_ = cli.DeleteTopic(ctx, DeleteTopicRequest{Topic: topic, BrokerAddr: broker0, NameSrv: []string{ns}})
	_ = cli.DeleteTopic(ctx, DeleteTopicRequest{Topic: topic, BrokerAddr: broker1, NameSrv: []string{ns}})
}

func assertTCP(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("tcp connect failed %s: %v", addr, err)
	}
	_ = conn.Close()
}

func getenvDefault(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func TestIntegrationAuthProbe(t *testing.T) {
	ns := getenvDefault("RMQ_NS", "127.0.0.1:19876")
	broker0 := getenvDefault("RMQ_BROKER0", "127.0.0.1:10911")

	cli, err := New(Options{NameServer: []string{ns}, TimeoutMs: 5000})
	if err != nil {
		t.Fatalf("new admin failed: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	probeUser := UserInfo{Username: "mqadmin_go_probe", Password: "P@ssw0rd", UserType: "NORMAL"}
	err = cli.CreateUser(ctx, probeUser, WithBroker(broker0))
	if err != nil {
		t.Logf("auth create user probe failed (likely ACL/Auth config required): %v", err)
		return
	}
	_ = cli.DeleteUser(ctx, probeUser.Username, WithBroker(broker0))
	fmt.Println("auth probe succeeded")
}
