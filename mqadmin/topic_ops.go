package mqadmin

import (
	"context"
	"encoding/json"
	"sort"
)

func (c *client) UpdateTopic(ctx context.Context, req CreateTopicRequest) error {
	if req.Topic == "" {
		return errEmptyTopic
	}
	if req.BrokerAddr == "" {
		return errEmptyBrokerAddr
	}
	defaultTopic := req.DefaultTopic
	if defaultTopic == "" {
		defaultTopic = "TBW102"
	}
	readQueueNums := req.ReadQueueNums
	if readQueueNums <= 0 {
		readQueueNums = defaultReadQueueNums
	}
	writeQueueNums := req.WriteQueueNums
	if writeQueueNums <= 0 {
		writeQueueNums = defaultWriteQueueNums
	}
	perm := req.Perm
	if perm <= 0 {
		perm = defaultPerm
	}
	topicFilterType := req.TopicFilterType
	if topicFilterType == "" {
		topicFilterType = defaultTopicFilterType
	}
	topicSysFlag := req.TopicSysFlag
	if topicSysFlag < 0 {
		topicSysFlag = defaultTopicSysFlag
	}
	ext := toMapString(map[string]any{
		"topic":           req.Topic,
		"defaultTopic":    defaultTopic,
		"readQueueNums":   readQueueNums,
		"writeQueueNums":  writeQueueNums,
		"perm":            perm,
		"topicFilterType": topicFilterType,
		"topicSysFlag":    topicSysFlag,
		"order":           req.Order,
	})
	_, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeUpdateAndCreateTopic, ext))
	return err
}

func (c *client) CreateTopic(ctx context.Context, req CreateTopicRequest) error {
	return c.UpdateTopic(ctx, req)
}

func (c *client) UpdateTopicList(ctx context.Context, brokerAddr string, topics []CreateTopicRequest) error {
	for _, topic := range topics {
		t := topic
		if t.BrokerAddr == "" {
			t.BrokerAddr = brokerAddr
		}
		if err := c.UpdateTopic(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) UpdateTopicPerm(ctx context.Context, req UpdateTopicPermRequest) error {
	if req.Topic == "" {
		return errEmptyTopic
	}
	route, err := c.getTopicRoute(ctx, req.Topic)
	if err != nil {
		return err
	}
	for _, qd := range route.QueueDatas {
		addr := ""
		for _, bd := range route.BrokerDatas {
			if bd.BrokerName != qd.BrokerName {
				continue
			}
			addr = bd.BrokerAddrs["0"]
			if addr == "" {
				for _, v := range bd.BrokerAddrs {
					addr = v
					break
				}
			}
			break
		}
		if addr == "" {
			continue
		}
		if err := c.UpdateTopic(ctx, CreateTopicRequest{
			Topic:          req.Topic,
			BrokerAddr:     addr,
			ReadQueueNums:  qd.WriteQueueNums,
			WriteQueueNums: qd.WriteQueueNums,
			Perm:           req.Perm,
			TopicSysFlag:   0,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) DeleteTopic(ctx context.Context, req DeleteTopicRequest) error {
	if req.Topic == "" {
		return errEmptyTopic
	}
	route, err := c.getTopicRoute(ctx, req.Topic)
	if err != nil {
		return err
	}

	brokerAddr := req.BrokerAddr
	if brokerAddr == "" {
		brokerAddr = selectBrokerAddr(route)
		if brokerAddr == "" {
			return errNoBrokerFromRoute
		}
	}

	_, err = c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeDeleteTopicInBroker, toMapString(map[string]any{"topic": req.Topic})))
	if err != nil {
		return err
	}

	nameSrvs := req.NameSrv
	if len(nameSrvs) == 0 {
		nameSrvs = c.nameServer
	}
	if len(nameSrvs) == 0 {
		return errNoNameServerProvided
	}

	ext := map[string]any{"topic": req.Topic}
	if req.Cluster != "" {
		ext["clusterName"] = req.Cluster
	}
	for _, ns := range nameSrvs {
		if _, err := invokeSyncWithRetry(ctx, ns, c.useTLS, c.timeout, newCommand(requestCodeDeleteTopicInNameSrv, toMapString(ext)), c.tlsConfig, c.retry, c.backoff); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	resp, err := c.invokeNameServer(ctx, newCommand(requestCodeGetAllTopicListFromNameSrv, nil))
	if err != nil {
		return nil, err
	}
	result := &TopicList{}
	if err := json.Unmarshal(resp.Body, result); err != nil {
		return nil, err
	}
	if result.TopicDetail == nil {
		result.TopicDetail = map[string]any{}
	}
	sort.Strings(result.TopicList)
	return result, nil
}

func (c *client) TopicList(ctx context.Context) (*TopicList, error) {
	return c.FetchAllTopicList(ctx)
}

func (c *client) FetchPublishMessageQueues(ctx context.Context, topic string) ([]MessageQueue, error) {
	route, err := c.getTopicRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	queues := make([]MessageQueue, 0)
	for _, q := range route.QueueDatas {
		if q.Perm&permWrite == 0 {
			continue
		}
		for i := 0; i < q.WriteQueueNums; i++ {
			queues = append(queues, MessageQueue{Topic: topic, BrokerName: q.BrokerName, QueueID: i})
		}
	}
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].BrokerName == queues[j].BrokerName {
			return queues[i].QueueID < queues[j].QueueID
		}
		return queues[i].BrokerName < queues[j].BrokerName
	})
	return queues, nil
}

func (c *client) FetchClusterList(ctx context.Context, topic string) ([]string, error) {
	route, err := c.getTopicRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(route.BrokerDatas) == 0 {
		return nil, errClusterUnsupported
	}
	set := map[string]struct{}{}
	for _, b := range route.BrokerDatas {
		if b.Cluster != "" {
			set[b.Cluster] = struct{}{}
		}
	}
	clusters := make([]string, 0, len(set))
	for k := range set {
		clusters = append(clusters, k)
	}
	sort.Strings(clusters)
	return clusters, nil
}

func (c *client) TopicCluster(ctx context.Context, topic string) ([]string, error) {
	return c.FetchClusterList(ctx, topic)
}

func (c *client) TopicRoute(ctx context.Context, topic string) (map[string]any, error) {
	route, err := c.getTopicRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	buf, err := json.Marshal(route)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *client) TopicStatus(ctx context.Context, topic string) (map[string]TopicStatsTable, error) {
	route, err := c.getTopicRoute(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[string]TopicStatsTable{}
	for _, bd := range route.BrokerDatas {
		addr := bd.BrokerAddrs["0"]
		if addr == "" {
			for _, v := range bd.BrokerAddrs {
				addr = v
				break
			}
		}
		if addr == "" {
			continue
		}
		resp, err := c.invokeBroker(ctx, addr, newCommand(requestCodeGetTopicStatsInfo, toMapString(map[string]any{"topic": topic})))
		if err != nil {
			return nil, err
		}
		table, err := decodeTopicStatsTable(resp.Body)
		if err != nil {
			return nil, err
		}
		out[bd.BrokerName] = table
	}
	return out, nil
}

func (c *client) getTopicRoute(ctx context.Context, topic string) (*routeData, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}
	resp, err := c.invokeNameServer(ctx, newCommand(requestCodeGetRouteInfoByTopic, toMapString(map[string]any{"topic": topic})))
	if err != nil {
		return nil, err
	}
	return decodeRouteData(resp.Body)
}

func selectBrokerAddr(route *routeData) string {
	for _, b := range route.BrokerDatas {
		if addr := b.BrokerAddrs["0"]; addr != "" {
			return addr
		}
	}
	for _, b := range route.BrokerDatas {
		for _, addr := range b.BrokerAddrs {
			if addr != "" {
				return addr
			}
		}
	}
	return ""
}
