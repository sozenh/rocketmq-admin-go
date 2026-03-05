package mqadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const namespaceOrderTopicConfig = "ORDER_TOPIC_CONFIG"

func (c *client) UpdateOrderConf(ctx context.Context, topic, orderConf string) error {
	if topic == "" {
		return errEmptyTopic
	}
	if orderConf == "" {
		return c.DeleteKVConfig(ctx, namespaceOrderTopicConfig, topic)
	}
	return c.UpdateKVConfig(ctx, KVConfig{Namespace: namespaceOrderTopicConfig, Key: topic, Value: orderConf})
}

func (c *client) AllocateMQ(ctx context.Context, topic, currentCID string, consumerIDs []string) ([]MessageQueue, error) {
	queues, err := c.FetchPublishMessageQueues(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(consumerIDs) == 0 || currentCID == "" {
		return queues, nil
	}
	sorted := append([]string(nil), consumerIDs...)
	sort.Strings(sorted)
	idx := -1
	for i, cid := range sorted {
		if cid == currentCID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("mqadmin: currentCID not in consumer list")
	}
	out := make([]MessageQueue, 0)
	for i, q := range queues {
		if i%len(sorted) == idx {
			out = append(out, q)
		}
	}
	return out, nil
}

func (c *client) UpdateStaticTopic(ctx context.Context, req StaticTopicRequest) error {
	if req.BrokerAddr == "" {
		return errEmptyBrokerAddr
	}
	if req.CreateTopic.Topic == "" {
		return errEmptyTopic
	}
	ct := req.CreateTopic
	if ct.DefaultTopic == "" {
		ct.DefaultTopic = "TBW102"
	}
	if ct.ReadQueueNums <= 0 {
		ct.ReadQueueNums = defaultReadQueueNums
	}
	if ct.WriteQueueNums <= 0 {
		ct.WriteQueueNums = defaultWriteQueueNums
	}
	if ct.Perm <= 0 {
		ct.Perm = defaultPerm
	}
	if ct.TopicFilterType == "" {
		ct.TopicFilterType = defaultTopicFilterType
	}
	ext := toMapString(map[string]any{
		"topic":           ct.Topic,
		"defaultTopic":    ct.DefaultTopic,
		"readQueueNums":   ct.ReadQueueNums,
		"writeQueueNums":  ct.WriteQueueNums,
		"perm":            ct.Perm,
		"topicFilterType": ct.TopicFilterType,
		"topicSysFlag":    ct.TopicSysFlag,
		"order":           ct.Order,
		"force":           req.Force,
	})
	cmd := newCommand(requestCodeUpdateAndCreateStaticTopic, ext)
	if len(req.MappingDetail) > 0 {
		body, err := json.Marshal(req.MappingDetail)
		if err != nil {
			return err
		}
		cmd.Body = body
	}
	_, err := c.invokeBroker(ctx, req.BrokerAddr, cmd)
	return err
}

func (c *client) RemappingStaticTopic(ctx context.Context, req StaticTopicRequest) error {
	return c.UpdateStaticTopic(ctx, req)
}

func (c *client) SetConsumeMode(ctx context.Context, req SetConsumeModeRequest) error {
	if req.BrokerAddr == "" {
		return errEmptyBrokerAddr
	}
	if req.Topic == "" {
		return errEmptyTopic
	}
	if req.Group == "" {
		return errEmptyGroup
	}
	mode := strings.ToUpper(strings.TrimSpace(req.Mode))
	if mode == "" {
		return errEmptyMode
	}
	if mode != "PULL" && mode != "POP" {
		return errUnsupportedMode
	}
	cmd := newCommand(requestCodeSetMessageRequestMode, nil)
	body, err := json.Marshal(map[string]any{
		"topic":            req.Topic,
		"consumerGroup":    req.Group,
		"mode":             mode,
		"popShareQueueNum": req.PopShareQueueNum,
	})
	if err != nil {
		return err
	}
	cmd.Body = body
	_, err = c.invokeBroker(ctx, req.BrokerAddr, cmd)
	return err
}

func (c *client) ConsumerProgress(ctx context.Context, group, topic string) (map[string]any, error) {
	if group == "" {
		return nil, errEmptyGroup
	}
	brokers := []string{}
	if topic != "" {
		route, err := c.getTopicRoute(ctx, topic)
		if err != nil {
			return nil, err
		}
		for _, bd := range route.BrokerDatas {
			if addr := bd.BrokerAddrs["0"]; addr != "" {
				brokers = append(brokers, addr)
			}
		}
	}
	if len(brokers) == 0 {
		clusterInfo, err := c.ClusterList(ctx)
		if err != nil {
			return nil, err
		}
		if table, ok := clusterInfo["brokerAddrTable"].(map[string]any); ok {
			for _, v := range table {
				if m, ok := v.(map[string]any); ok {
					if addrs, ok := m["brokerAddrs"].(map[string]any); ok {
						if a, ok := addrs["0"].(string); ok {
							brokers = append(brokers, a)
						}
					}
				}
			}
		}
	}
	out := map[string]any{}
	for _, brokerAddr := range brokers {
		ext := map[string]any{"consumerGroup": group}
		if topic != "" {
			ext["topic"] = topic
		}
		resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetConsumeStats, toMapString(ext)))
		if err != nil {
			continue
		}
		out[brokerAddr] = decodeResetOffsetBody(resp.Body)
	}
	return out, nil
}

func (c *client) ConsumerStatus(ctx context.Context, topic, group, clientAddr string) (map[string]any, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}
	if group == "" {
		return nil, errEmptyGroup
	}
	route, err := c.getTopicRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, bd := range route.BrokerDatas {
		addr := bd.BrokerAddrs["0"]
		if addr == "" {
			continue
		}
		ext := map[string]any{"topic": topic, "group": group}
		if clientAddr != "" {
			ext["clientAddr"] = clientAddr
		}
		resp, err := c.invokeBroker(ctx, addr, newCommand(requestCodeInvokeBrokerToGetConsumerStatus, toMapString(ext)))
		if err != nil {
			resp, err = c.invokeBroker(ctx, addr, newCommand(requestCodeGetConsumerStatusFromClient, toMapString(ext)))
			if err != nil {
				continue
			}
		}
		out[bd.BrokerName] = decodeResetOffsetBody(resp.Body)
	}
	return out, nil
}

func (c *client) GetConsumerConfig(ctx context.Context, group string) (map[string]any, error) {
	if group == "" {
		return nil, errEmptyGroup
	}
	clusterInfo, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	if table, ok := clusterInfo["brokerAddrTable"].(map[string]any); ok {
		for brokerName, v := range table {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}
			addrs, ok := m["brokerAddrs"].(map[string]any)
			if !ok {
				continue
			}
			addr, _ := addrs["0"].(string)
			if addr == "" {
				continue
			}
			wrapper, err := c.GetAllSubscriptionGroup(ctx, c.rewriteBrokerAddr(addr))
			if err != nil {
				continue
			}
			if cfg, ok := wrapper.SubscriptionGroupTable[group]; ok {
				result[brokerName] = cfg
			}
		}
	}
	return result, nil
}

func (c *client) StartMonitoring(ctx context.Context, group, topic string) (map[string]any, error) {
	return c.ConsumerProgress(ctx, group, topic)
}

func (c *client) ClusterList(ctx context.Context) (map[string]any, error) {
	resp, err := c.invokeNameServer(ctx, newCommand(requestCodeGetBrokerClusterInfo, nil))
	if err != nil {
		return nil, err
	}
	out := decodeResetOffsetBody(resp.Body)
	return out, nil
}

func (c *client) ClusterSendMsgRT(ctx context.Context, clusterName string) (map[string]any, error) {
	info, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"cluster": clusterName,
		"note":    "clusterSendMsgRT in mqadmin-go currently provides broker runtime snapshot, not active message benchmark",
		"data":    info,
	}, nil
}

func (c *client) CopyUsers(ctx context.Context, sourceBroker, targetBroker, username string) error {
	if sourceBroker == "" || targetBroker == "" {
		return errEmptyBrokerAddr
	}
	if username != "" {
		u, err := c.GetUser(ctx, sourceBroker, username)
		if err != nil {
			return err
		}
		if _, err := c.GetUser(ctx, targetBroker, username); err != nil {
			return c.CreateUser(ctx, targetBroker, *u)
		}
		return c.UpdateUser(ctx, targetBroker, *u)
	}
	users, err := c.ListUser(ctx, sourceBroker, "")
	if err != nil {
		return err
	}
	for _, u := range users {
		if _, err := c.GetUser(ctx, targetBroker, u.Username); err != nil {
			if err := c.CreateUser(ctx, targetBroker, u); err != nil {
				return err
			}
			continue
		}
		if err := c.UpdateUser(ctx, targetBroker, u); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) CopyAcls(ctx context.Context, sourceBroker, targetBroker, subject string) error {
	if sourceBroker == "" || targetBroker == "" {
		return errEmptyBrokerAddr
	}
	if subject != "" {
		a, err := c.GetAcl(ctx, sourceBroker, subject)
		if err != nil {
			return err
		}
		if _, err := c.GetAcl(ctx, targetBroker, subject); err != nil {
			return c.CreateAcl(ctx, targetBroker, *a)
		}
		return c.UpdateAcl(ctx, targetBroker, *a)
	}
	acls, err := c.ListAcl(ctx, sourceBroker, "", "")
	if err != nil {
		return err
	}
	for _, a := range acls {
		if _, err := c.GetAcl(ctx, targetBroker, a.Subject); err != nil {
			if err := c.CreateAcl(ctx, targetBroker, a); err != nil {
				return err
			}
			continue
		}
		if err := c.UpdateAcl(ctx, targetBroker, a); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) GetBrokerLiteInfo(ctx context.Context, brokerAddr string) (map[string]any, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetBrokerLiteInfo, nil))
	if err != nil {
		return nil, err
	}
	return decodeResetOffsetBody(resp.Body), nil
}

func (c *client) GetParentTopicInfo(ctx context.Context, brokerAddr, parentTopic string) (map[string]any, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetParentTopicInfo, toMapString(map[string]any{"topic": parentTopic})))
	if err != nil {
		return nil, err
	}
	return decodeResetOffsetBody(resp.Body), nil
}

func (c *client) GetLiteTopicInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error) {
	resp, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeGetLiteTopicInfo, toMapString(map[string]any{"parentTopic": req.ParentTopic, "liteTopic": req.LiteTopic})))
	if err != nil {
		return nil, err
	}
	return decodeResetOffsetBody(resp.Body), nil
}

func (c *client) GetLiteClientInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error) {
	ext := map[string]any{"parentTopic": req.ParentTopic, "group": req.Group, "clientId": req.ClientID}
	if req.MaxCount > 0 {
		ext["maxCount"] = req.MaxCount
	}
	resp, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeGetLiteClientInfo, toMapString(ext)))
	if err != nil {
		return nil, err
	}
	return decodeResetOffsetBody(resp.Body), nil
}

func (c *client) GetLiteGroupInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error) {
	resp, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeGetLiteGroupInfo, toMapString(map[string]any{"group": req.Group, "liteTopic": req.LiteTopic, "topK": req.TopK})))
	if err != nil {
		return nil, err
	}
	return decodeResetOffsetBody(resp.Body), nil
}

func (c *client) TriggerLiteDispatch(ctx context.Context, req LiteTopicRequest) error {
	_, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeTriggerLiteDispatch, toMapString(map[string]any{"group": req.Group, "clientId": req.ClientID})))
	return err
}

func (c *client) StatsAll(ctx context.Context, topic string) (map[string]any, error) {
	topics := []string{}
	if topic != "" {
		topics = append(topics, topic)
	} else {
		list, err := c.TopicList(ctx)
		if err != nil {
			return nil, err
		}
		topics = append(topics, list.TopicList...)
	}
	out := map[string]any{}
	for _, t := range topics {
		status, err := c.TopicStatus(ctx, t)
		if err != nil {
			continue
		}
		out[t] = status
	}
	return map[string]any{"topicCount": len(topics), "topics": out}, nil
}
