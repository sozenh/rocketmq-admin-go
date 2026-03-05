package mqadmin

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	requestCodeSendMessage = 10
)

func (c *client) AddBroker(ctx context.Context, controllerAddr string, req BrokerMembershipRequest) error {
	_, err := c.invokeAddress(ctx, controllerAddr, requestCodeAddBroker, map[string]any{"configPath": req.ConfigPath}, nil)
	return err
}

func (c *client) RemoveBroker(ctx context.Context, controllerAddr string, req BrokerMembershipRequest) error {
	_, err := c.invokeAddress(ctx, controllerAddr, requestCodeRemoveBroker, map[string]any{
		"brokerClusterName": req.ClusterName,
		"brokerName":        req.BrokerName,
		"brokerId":          req.BrokerID,
	}, nil)
	return err
}

func (c *client) ResetMasterFlushOffset(ctx context.Context, brokerAddr string, offset int64) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeResetMasterFlushOffset, map[string]any{"masterFlushOffset": offset}, nil)
	return err
}

func (c *client) BrokerConsumeStats(ctx context.Context, brokerAddr string, isOrder bool, timeoutMillis int64) (map[string]any, error) {
	_ = timeoutMillis
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeGetBrokerConsumeStats, map[string]any{"isOrder": isOrder}, nil)
}

func (c *client) CleanExpiredCQ(ctx context.Context, brokerAddr string) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeCleanExpiredConsumeQueue, nil, nil)
	return err
}

func (c *client) DeleteExpiredCommitLog(ctx context.Context, brokerAddr string) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeDeleteExpiredCommitLog, nil, nil)
	return err
}

func (c *client) CleanUnusedTopic(ctx context.Context, brokerAddr string) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeCleanUnusedTopic, nil, nil)
	return err
}

func (c *client) SendMsgStatus(ctx context.Context, brokerAddr, topic string, count int) (map[string]any, error) {
	if count <= 0 {
		count = 50
	}
	if topic == "" {
		return nil, errEmptyTopic
	}
	rts := make([]int64, 0, count)
	failures := 0
	for i := 0; i < count; i++ {
		start := time.Now()
		_, err := c.SendMessage(ctx, SendMessageRequest{BrokerAddr: brokerAddr, Topic: topic, Body: "hello jodie", WaitStore: true})
		rts = append(rts, time.Since(start).Milliseconds())
		if err != nil {
			failures++
		}
	}
	avg := int64(0)
	for _, v := range rts {
		avg += v
	}
	if len(rts) > 0 {
		avg /= int64(len(rts))
	}
	out := map[string]any{"count": count, "topic": topic, "avgRTMs": avg, "failures": failures, "rts": rts}
	return out, nil
}

func (c *client) GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) (map[string]any, error) {
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeGetColdDataFlowCtrInfo, nil, nil)
}

func (c *client) UpdateColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr, consumerGroup, threshold string) error {
	if consumerGroup == "" {
		return errEmptyGroup
	}
	if threshold == "" {
		return errEmptyValue
	}
	body := propertiesToBytes(map[string]string{consumerGroup: threshold})
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeUpdateColdDataFlowCtrConfig, nil, body)
	return err
}

func (c *client) RemoveColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr, consumerGroup string) error {
	if consumerGroup == "" {
		return errEmptyGroup
	}
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeRemoveColdDataFlowCtrConfig, nil, []byte(consumerGroup))
	return err
}

func (c *client) SetCommitLogReadAheadMode(ctx context.Context, brokerAddr, mode string) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeSetCommitLogReadMode, map[string]any{"READ_AHEAD_MODE": mode}, nil)
	return err
}

func (c *client) SwitchTimerEngine(ctx context.Context, brokerAddr string) error {
	_, err := c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeSwitchTimerEngine, nil, nil)
	return err
}

func (c *client) HAStatus(ctx context.Context, brokerAddr string) (map[string]any, error) {
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeGetBrokerHAStatus, nil, nil)
}

func (c *client) GetSyncStateSet(ctx context.Context, controllerAddr, brokerName string) (map[string]any, error) {
	meta, err := c.GetControllerMetaData(ctx, controllerAddr)
	if err != nil {
		return nil, err
	}
	leader, _ := meta["controllerLeaderAddress"].(string)
	if leader == "" {
		leader = controllerAddr
	}
	body, err := json.Marshal([]string{brokerName})
	if err != nil {
		return nil, err
	}
	return c.invokeAddress(ctx, leader, requestCodeControllerGetSyncStateData, nil, body)
}

func (c *client) GetBrokerEpoch(ctx context.Context, brokerAddr string) (map[string]any, error) {
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeGetBrokerEpochCache, nil, nil)
}

func (c *client) QueryMsgByID(ctx context.Context, brokerAddr, msgID string) (map[string]any, error) {
	addr := brokerAddr
	offset := int64(-1)
	if parsedAddr, parsedOffset, err := decodeOffsetMsgID(msgID); err == nil {
		offset = parsedOffset
		if addr == "" {
			addr = parsedAddr
		}
	}
	if addr == "" {
		return nil, errEmptyBrokerAddr
	}
	if offset < 0 {
		return nil, fmt.Errorf("mqadmin: invalid message id %s", msgID)
	}
	return c.invokeBrokerDecoded(ctx, addr, requestCodeViewMessageByID, map[string]any{"topic": "", "offset": offset}, nil)
}

func (c *client) QueryMsgByKey(ctx context.Context, topic, key string, maxNum int, beginTimestamp, endTimestamp int64) (map[string]any, error) {
	if maxNum <= 0 {
		maxNum = 32
	}
	if beginTimestamp <= 0 {
		beginTimestamp = 0
	}
	if endTimestamp <= 0 {
		endTimestamp = time.Now().UnixMilli()
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
		data, err := c.invokeBrokerDecoded(ctx, addr, requestCodeQueryMessage, map[string]any{
			"topic":          topic,
			"key":            key,
			"maxNum":         maxNum,
			"beginTimestamp": beginTimestamp,
			"endTimestamp":   endTimestamp,
		}, nil)
		if err != nil {
			continue
		}
		out[bd.BrokerName] = data
	}
	return out, nil
}

func (c *client) QueryMsgByUniqueKey(ctx context.Context, topic, key string) (map[string]any, error) {
	return c.QueryMsgByKey(ctx, topic, key, 32, 0, time.Now().UnixMilli())
}

func (c *client) QueryMsgByOffset(ctx context.Context, topic string, queueID int, offset int64) (map[string]any, error) {
	return c.QueryConsumeQueue(ctx, "", topic, queueID, offset, 1, "")
}

func (c *client) QueryMsgTraceByID(ctx context.Context, topic, msgID string) (map[string]any, error) {
	return c.QueryMsgByUniqueKey(ctx, topic, msgID)
}

func (c *client) PrintMessage(ctx context.Context, brokerAddr, msgID string) (map[string]any, error) {
	return c.QueryMsgByID(ctx, brokerAddr, msgID)
}

func (c *client) PrintMessageByQueue(ctx context.Context, topic string, queueID int, index int64, count int32) (map[string]any, error) {
	return c.QueryConsumeQueue(ctx, "", topic, queueID, index, count, "")
}

func (c *client) SendMessage(ctx context.Context, req SendMessageRequest) (map[string]any, error) {
	if req.BrokerAddr == "" {
		return nil, errEmptyBrokerAddr
	}
	if req.Topic == "" {
		return nil, errEmptyTopic
	}
	if req.Body == "" {
		return nil, errEmptyValue
	}
	props := map[string]string{}
	if req.Tag != "" {
		props["TAGS"] = req.Tag
	}
	if req.Keys != "" {
		props["KEYS"] = req.Keys
	}
	props["WAIT"] = strconv.FormatBool(req.WaitStore)
	return c.invokeBrokerDecoded(ctx, req.BrokerAddr, requestCodeSendMessage, map[string]any{
		"producerGroup":         "mqadmin-go",
		"topic":                 req.Topic,
		"defaultTopic":          "TBW102",
		"defaultTopicQueueNums": 4,
		"queueId":               -1,
		"sysFlag":               0,
		"bornTimestamp":         time.Now().UnixMilli(),
		"flag":                  0,
		"properties":            messagePropertiesToString(props),
		"reconsumeTimes":        0,
		"maxReconsumeTimes":     0,
		"unitMode":              false,
		"batch":                 false,
	}, []byte(req.Body))
}

func (c *client) ConsumeMessage(ctx context.Context, brokerAddr, consumerGroup, topic string, queueID int, offset int64) (map[string]any, error) {
	return c.QueryConsumeQueue(ctx, brokerAddr, topic, queueID, offset, 1, consumerGroup)
}

func (c *client) CheckMsgSendRT(ctx context.Context, brokerAddr, topic string, amount int, msgSize int) (map[string]any, error) {
	if amount <= 0 {
		amount = 10
	}
	if msgSize <= 0 {
		msgSize = 128
	}
	body := make([]byte, msgSize)
	begin := time.Now()
	fail := 0
	for i := 0; i < amount; i++ {
		_, err := c.SendMessage(ctx, SendMessageRequest{BrokerAddr: brokerAddr, Topic: topic, Body: string(body), WaitStore: true})
		if err != nil {
			fail++
		}
	}
	d := time.Since(begin)
	return map[string]any{"amount": amount, "failed": fail, "avgMs": float64(d.Milliseconds()) / float64(amount)}, nil
}

func (c *client) DumpCompactionLog(ctx context.Context, brokerAddr, topic string, queueID int) (map[string]any, error) {
	status, err := c.BrokerStatus(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}
	return map[string]any{"topic": topic, "queueId": queueID, "brokerStatus": status, "note": "dumpCompactionLog summary in mqadmin-go"}, nil
}

func (c *client) ProducerConnection(ctx context.Context, producerGroup, topic string) (map[string]any, error) {
	if topic != "" {
		route, err := c.getTopicRoute(ctx, topic)
		if err == nil {
			for _, bd := range route.BrokerDatas {
				if addr := bd.BrokerAddrs["0"]; addr != "" {
					return c.invokeBrokerDecoded(ctx, addr, requestCodeGetProducerConnectionList, map[string]any{"producerGroup": producerGroup}, nil)
				}
			}
		}
	}
	return nil, errNoBrokerFromRoute
}

func (c *client) ConsumerConnection(ctx context.Context, consumerGroup string) (map[string]any, error) {
	cluster, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	if table, ok := cluster["brokerAddrTable"].(map[string]any); ok {
		for _, v := range table {
			if vm, ok := v.(map[string]any); ok {
				if addrs, ok := vm["brokerAddrs"].(map[string]any); ok {
					if addr, ok := addrs["0"].(string); ok && addr != "" {
						return c.invokeBrokerDecoded(ctx, c.rewriteBrokerAddr(addr), requestCodeGetConsumerConnectionList, map[string]any{"consumerGroup": consumerGroup}, nil)
					}
				}
			}
		}
	}
	return nil, errNoBrokerFromRoute
}

func (c *client) ProducerInfo(ctx context.Context, brokerAddr string) (map[string]any, error) {
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeGetAllProducerInfo, nil, nil)
}

func (c *client) QueryConsumeQueue(ctx context.Context, brokerAddr, topic string, queueID int, index int64, count int32, consumerGroup string) (map[string]any, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}
	if brokerAddr == "" {
		route, err := c.getTopicRoute(ctx, topic)
		if err != nil {
			return nil, err
		}
		brokerAddr = selectBrokerAddr(route)
	}
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeQueryConsumeQueue, map[string]any{
		"topic":         topic,
		"queueId":       queueID,
		"index":         index,
		"count":         count,
		"consumerGroup": consumerGroup,
	}, nil)
}

func (c *client) CheckRocksdbCqWriteProgress(ctx context.Context, brokerAddr string, topic string, checkStoreTime bool) (map[string]any, error) {
	storeTime := int64(0)
	if checkStoreTime {
		storeTime = time.Now().UnixMilli()
	}
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeCheckRocksdbCQWriteProgress, map[string]any{"topic": topic, "checkStoreTime": storeTime}, nil)
}

func (c *client) ExportMetadata(ctx context.Context) (map[string]any, error) {
	clusters, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	topics, err := c.TopicList(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"cluster": clusters, "topics": topics.TopicList}, nil
}

func (c *client) ExportConfigs(ctx context.Context) (map[string]any, error) {
	configs, err := c.GetNamesrvConfig(ctx, nil)
	if err != nil {
		return nil, err
	}
	return map[string]any{"namesrvConfigs": configs}, nil
}

func (c *client) ExportMetrics(ctx context.Context) (map[string]any, error) {
	clusters, err := c.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"metrics": clusters}, nil
}

func (c *client) ExportMetadataInRocksDB(ctx context.Context, brokerAddr, configType string) (map[string]any, error) {
	return c.RocksDBConfigToJSON(ctx, brokerAddr, configType, false)
}

func (c *client) ExportPopRecord(ctx context.Context, brokerAddr string) (map[string]any, error) {
	status, err := c.BrokerStatus(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}
	return map[string]any{"popRecord": status}, nil
}

func (c *client) RocksDBConfigToJSON(ctx context.Context, brokerAddr, configType string, noEscape bool) (map[string]any, error) {
	return c.invokeBrokerDecoded(ctx, brokerAddr, requestCodeExportRocksDBConfigToJSON, map[string]any{"configType": configType, "noEscape": noEscape}, nil)
}

func (c *client) GetControllerMetaData(ctx context.Context, controllerAddr string) (map[string]any, error) {
	return c.invokeAddressWithHeader(ctx, controllerAddr, requestCodeControllerGetMetadataInfo, nil, nil)
}

func (c *client) GetControllerConfig(ctx context.Context, controllerAddr string) (map[string]any, error) {
	return c.invokeAddressWithPropertiesBody(ctx, controllerAddr, requestCodeGetControllerConfig, nil, nil)
}

func (c *client) UpdateControllerConfig(ctx context.Context, controllerAddr string, properties map[string]string) error {
	_, err := c.invokeAddress(ctx, controllerAddr, requestCodeUpdateControllerConfig, nil, propertiesToBytes(properties))
	return err
}

func (c *client) ReElectMaster(ctx context.Context, controllerAddr string, req ElectMasterRequest) (map[string]any, error) {
	meta, err := c.GetControllerMetaData(ctx, controllerAddr)
	if err != nil {
		return nil, err
	}
	leader, _ := meta["controllerLeaderAddress"].(string)
	if leader == "" {
		leader = controllerAddr
	}
	return c.invokeAddressWithHeader(ctx, leader, requestCodeControllerElectMaster, map[string]any{"clusterName": req.ClusterName, "brokerName": req.BrokerName, "brokerId": req.BrokerID, "designateElect": true}, nil)
}

func (c *client) CleanControllerBrokerMeta(ctx context.Context, controllerAddr string, req CleanBrokerDataRequest) (map[string]any, error) {
	meta, err := c.GetControllerMetaData(ctx, controllerAddr)
	if err != nil {
		return nil, err
	}
	leader, _ := meta["controllerLeaderAddress"].(string)
	if leader == "" {
		leader = controllerAddr
	}
	brokerIDs := req.BrokerIDs
	if brokerIDs == "" && req.BrokerID > 0 {
		brokerIDs = strconv.FormatInt(req.BrokerID, 10)
	}
	return c.invokeAddressWithHeader(ctx, leader, requestCodeCleanBrokerData, map[string]any{"clusterName": req.ClusterName, "brokerName": req.BrokerName, "brokerControllerIdsToClean": brokerIDs, "isCleanLivingBroker": req.CleanLiving}, nil)
}

func (c *client) invokeNameServerDecoded(ctx context.Context, code int, ext map[string]any, body []byte) (map[string]any, error) {
	resp, err := c.invokeNameServer(ctx, commandWithBody(code, ext, body))
	if err != nil {
		return nil, err
	}
	return decodeAnyBody(resp.Body), nil
}

func (c *client) invokeBrokerDecoded(ctx context.Context, brokerAddr string, code int, ext map[string]any, body []byte) (map[string]any, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, commandWithBody(code, ext, body))
	if err != nil {
		return nil, err
	}
	return decodeAnyBody(resp.Body), nil
}

func (c *client) invokeAddress(ctx context.Context, addr string, code int, ext map[string]any, body []byte) (map[string]any, error) {
	resp, err := invokeSyncWithRetry(ctx, addr, c.useTLS, c.timeout, commandWithBody(code, ext, body), c.tlsConfig, c.retry, c.backoff)
	if err != nil {
		return nil, err
	}
	return decodeAnyBody(resp.Body), nil
}

func (c *client) invokeAddressWithHeader(ctx context.Context, addr string, code int, ext map[string]any, body []byte) (map[string]any, error) {
	resp, err := invokeSyncWithRetry(ctx, addr, c.useTLS, c.timeout, commandWithBody(code, ext, body), c.tlsConfig, c.retry, c.backoff)
	if err != nil {
		return nil, err
	}
	merged := decodeAnyBody(resp.Body)
	if len(resp.ExtFields) > 0 {
		header := map[string]any{}
		for k, v := range resp.ExtFields {
			header[k] = v
		}
		merged["header"] = header
		for k, v := range header {
			if _, ok := merged[k]; !ok {
				merged[k] = v
			}
		}
	}
	return merged, nil
}

func (c *client) invokeAddressWithPropertiesBody(ctx context.Context, addr string, code int, ext map[string]any, body []byte) (map[string]any, error) {
	resp, err := invokeSyncWithRetry(ctx, addr, c.useTLS, c.timeout, commandWithBody(code, ext, body), c.tlsConfig, c.retry, c.backoff)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"properties": bytesToProperties(resp.Body)}
	if len(resp.ExtFields) > 0 {
		for k, v := range resp.ExtFields {
			out[k] = v
		}
	}
	return out, nil
}

func commandWithBody(code int, ext map[string]any, body []byte) *remotingCommand {
	cmd := newCommand(code, toMapString(ext))
	if len(body) > 0 {
		cmd.Body = body
	}
	return cmd
}

func decodeAnyBody(body []byte) map[string]any {
	if len(body) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err == nil {
		return out
	}
	compat := decodeResetOffsetBody(body)
	if len(compat) > 0 {
		return compat
	}
	return map[string]any{"raw": string(body), "size": strconv.Itoa(len(body))}
}

func decodeOffsetMsgID(msgID string) (string, int64, error) {
	hexMsgID := strings.TrimSpace(msgID)
	if len(hexMsgID) != 32 {
		return "", -1, fmt.Errorf("unexpected msgId length")
	}
	raw, err := hex.DecodeString(hexMsgID)
	if err != nil {
		return "", -1, err
	}
	ip := net.IPv4(raw[0], raw[1], raw[2], raw[3]).String()
	port := int(raw[4])<<24 | int(raw[5])<<16 | int(raw[6])<<8 | int(raw[7])
	offset := int64(0)
	for _, b := range raw[8:16] {
		offset = (offset << 8) | int64(b)
	}
	return fmt.Sprintf("%s:%d", ip, port), offset, nil
}

func messagePropertiesToString(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}
	b := strings.Builder{}
	for k, v := range properties {
		if k == "" {
			continue
		}
		b.WriteString(k)
		b.WriteByte(1)
		b.WriteString(v)
		b.WriteByte(2)
	}
	return b.String()
}
