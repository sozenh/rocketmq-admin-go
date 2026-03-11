package mqadmin

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"time"
)

type Admin interface {
	ClusterList(ctx context.Context) (*ClusterInfo, error)
	ClusterSendMsgRT(ctx context.Context, clusterName string) (*ClusterSendMsgRTResult, error)

	CreateAcl(ctx context.Context, opts ...AclOption) error
	UpdateAcl(ctx context.Context, opts ...AclOption) error
	DeleteAcl(ctx context.Context, subject, resource, policyType string, opts ...ScopeOption) error
	GetAcl(ctx context.Context, subject string, opts ...ScopeOption) (map[string]*ParsedAclInfo, error)
	ListAcl(ctx context.Context, subjectFilter, resourceFilter string, opts ...ScopeOption) (map[string][]ParsedAclInfo, error)
	CopyAcls(ctx context.Context, subject string, source ScopeSelector, target ScopeSelector) error

	CreateUser(ctx context.Context, user UserInfo, opts ...ScopeOption) error
	UpdateUser(ctx context.Context, user UserInfo, opts ...ScopeOption) error
	DeleteUser(ctx context.Context, username string, opts ...ScopeOption) error
	GetUser(ctx context.Context, username string, opts ...ScopeOption) (map[string]*UserInfo, error)
	ListUser(ctx context.Context, filter string, opts ...ScopeOption) (map[string][]UserInfo, error)
	CopyUsers(ctx context.Context, username string, source ScopeSelector, target ScopeSelector) error

	// ACL 1.0
	UpdateAclConfig(ctx context.Context, cfg AclConfigV1, opts ...ScopeOption) error
	DeleteAclConfig(ctx context.Context, accessKey string, opts ...ScopeOption) error
	UpdateGlobalWhiteAddrsConfig(ctx context.Context, addrs []string, opts ...ScopeOption) error
	GetBrokerClusterAclInfo(ctx context.Context, opts ...ScopeOption) (map[string]*AclInfoV1, error)

	UpdateTopic(ctx context.Context, req CreateTopicRequest) error
	UpdateTopicList(ctx context.Context, brokerAddr string, topics []CreateTopicRequest) error
	UpdateTopicPerm(ctx context.Context, req UpdateTopicPermRequest) error
	UpdateOrderConf(ctx context.Context, topic, orderConf string) error
	AllocateMQ(ctx context.Context, topic, currentCID string, consumerIDs []string) ([]MessageQueue, error)
	UpdateStaticTopic(ctx context.Context, req StaticTopicRequest) error
	RemappingStaticTopic(ctx context.Context, req StaticTopicRequest) error
	DeleteTopic(ctx context.Context, req DeleteTopicRequest) error
	UpdateSubGroup(ctx context.Context, brokerAddr string, cfg SubscriptionGroupConfig) error
	UpdateSubGroupList(ctx context.Context, brokerAddr string, cfgs []SubscriptionGroupConfig) error
	DeleteSubscriptionGroup(ctx context.Context, req DeleteSubscriptionGroupRequest) error
	SetConsumeMode(ctx context.Context, req SetConsumeModeRequest) error
	ConsumerProgress(ctx context.Context, group, topic string) (map[string]any, error)
	ConsumerStatus(ctx context.Context, topic, group, clientAddr string) (map[string]any, error)
	GetConsumerConfig(ctx context.Context, group string) (map[string]any, error)
	StartMonitoring(ctx context.Context, group, topic string) (map[string]any, error)
	TopicRoute(ctx context.Context, topic string) (map[string]any, error)
	TopicStatus(ctx context.Context, topic string) (map[string]TopicStatsTable, error)
	TopicCluster(ctx context.Context, topic string) ([]string, error)
	TopicList(ctx context.Context) (*TopicList, error)

	ResetOffsetByTime(ctx context.Context, req ResetOffsetRequest) (map[string]map[string]any, error)
	CloneGroupOffset(ctx context.Context, req CloneGroupOffsetRequest) error
	SkipAccumulation(ctx context.Context, topic, group, cluster string) (map[string]map[string]any, error)

	BrokerStatus(ctx context.Context, brokerAddr string) (map[string]string, error)
	GetBrokerConfig(ctx context.Context, brokerAddr string) (map[string]string, error)
	UpdateBrokerConfig(ctx context.Context, brokerAddr string, properties map[string]string) error
	UpdateKVConfig(ctx context.Context, kv KVConfig) error
	DeleteKVConfig(ctx context.Context, namespace, key string) error
	WipeWritePerm(ctx context.Context, brokerName string, namesrvs []string) error
	AddWritePerm(ctx context.Context, brokerName string, namesrvs []string) error
	GetNamesrvConfig(ctx context.Context, namesrvs []string) (map[string]map[string]string, error)
	UpdateNamesrvConfig(ctx context.Context, namesrvs []string, properties map[string]string) error
	AddBroker(ctx context.Context, controllerAddr string, req BrokerMembershipRequest) error
	RemoveBroker(ctx context.Context, controllerAddr string, req BrokerMembershipRequest) error
	ResetMasterFlushOffset(ctx context.Context, brokerAddr string, offset int64) error
	BrokerConsumeStats(ctx context.Context, brokerAddr string, isOrder bool, timeoutMillis int64) (map[string]any, error)
	CleanExpiredCQ(ctx context.Context, brokerAddr string) error
	DeleteExpiredCommitLog(ctx context.Context, brokerAddr string) error
	CleanUnusedTopic(ctx context.Context, brokerAddr string) error
	SendMsgStatus(ctx context.Context, brokerAddr, topic string, count int) (map[string]any, error)
	GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) (map[string]any, error)
	UpdateColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr, consumerGroup, threshold string) error
	RemoveColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr, consumerGroup string) error
	SetCommitLogReadAheadMode(ctx context.Context, brokerAddr, mode string) error
	SwitchTimerEngine(ctx context.Context, brokerAddr string) error
	HAStatus(ctx context.Context, brokerAddr string) (map[string]any, error)
	GetSyncStateSet(ctx context.Context, controllerAddr, brokerName string) (map[string]any, error)
	GetBrokerEpoch(ctx context.Context, brokerAddr string) (map[string]any, error)
	QueryMsgByID(ctx context.Context, brokerAddr, msgID string) (map[string]any, error)
	QueryMsgByKey(ctx context.Context, topic, key string, maxNum int, beginTimestamp, endTimestamp int64) (map[string]any, error)
	QueryMsgByUniqueKey(ctx context.Context, topic, key string) (map[string]any, error)
	QueryMsgByOffset(ctx context.Context, topic string, queueID int, offset int64) (map[string]any, error)
	QueryMsgTraceByID(ctx context.Context, topic, msgID string) (map[string]any, error)
	PrintMessage(ctx context.Context, brokerAddr, msgID string) (map[string]any, error)
	PrintMessageByQueue(ctx context.Context, topic string, queueID int, index int64, count int32) (map[string]any, error)
	SendMessage(ctx context.Context, req SendMessageRequest) (map[string]any, error)
	ConsumeMessage(ctx context.Context, brokerAddr, consumerGroup, topic string, queueID int, offset int64) (map[string]any, error)
	CheckMsgSendRT(ctx context.Context, brokerAddr, topic string, amount int, msgSize int) (map[string]any, error)
	DumpCompactionLog(ctx context.Context, brokerAddr, topic string, queueID int) (map[string]any, error)
	ProducerConnection(ctx context.Context, producerGroup, topic string) (map[string]any, error)
	ConsumerConnection(ctx context.Context, consumerGroup string) (map[string]any, error)
	ProducerInfo(ctx context.Context, brokerAddr string) (map[string]any, error)
	QueryConsumeQueue(ctx context.Context, brokerAddr, topic string, queueID int, index int64, count int32, consumerGroup string) (map[string]any, error)
	CheckRocksdbCqWriteProgress(ctx context.Context, brokerAddr string, topic string, checkStoreTime bool) (map[string]any, error)
	ExportMetadata(ctx context.Context) (map[string]any, error)
	ExportConfigs(ctx context.Context) (map[string]any, error)
	ExportMetrics(ctx context.Context) (map[string]any, error)
	ExportMetadataInRocksDB(ctx context.Context, brokerAddr, configType string) (map[string]any, error)
	ExportPopRecord(ctx context.Context, brokerAddr string) (map[string]any, error)
	RocksDBConfigToJSON(ctx context.Context, brokerAddr, configType string, noEscape bool) (map[string]any, error)
	GetControllerMetaData(ctx context.Context, controllerAddr string) (map[string]any, error)
	GetControllerConfig(ctx context.Context, controllerAddr string) (map[string]any, error)
	UpdateControllerConfig(ctx context.Context, controllerAddr string, properties map[string]string) error
	ReElectMaster(ctx context.Context, controllerAddr string, req ElectMasterRequest) (map[string]any, error)
	CleanControllerBrokerMeta(ctx context.Context, controllerAddr string, req CleanBrokerDataRequest) (map[string]any, error)

	GetAllSubscriptionGroup(ctx context.Context, brokerAddr string) (*SubscriptionGroupWrapper, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]MessageQueue, error)
	FetchClusterList(ctx context.Context, topic string) ([]string, error)

	StatsAll(ctx context.Context, topic string) (*StatsAllResult, error)
	GetBrokerLiteInfo(ctx context.Context, brokerAddr string) (map[string]any, error)
	GetParentTopicInfo(ctx context.Context, brokerAddr, parentTopic string) (map[string]any, error)
	GetLiteTopicInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error)
	GetLiteClientInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error)
	GetLiteGroupInfo(ctx context.Context, req LiteTopicRequest) (map[string]any, error)
	TriggerLiteDispatch(ctx context.Context, req LiteTopicRequest) error

	CreateTopic(ctx context.Context, req CreateTopicRequest) error
	FetchAllTopicList(ctx context.Context) (*TopicList, error)
	Close() error
}

type client struct {
	nameServer []string
	useTLS     bool
	timeout    time.Duration
	retry      int
	backoff    time.Duration
	tlsConfig  *tls.Config
	creds      Credentials
	nsCursor   uint32
	addrMap    map[string]string
}

func New(opts Options) (Admin, error) {
	if len(opts.NameServer) == 0 {
		return nil, errEmptyNameServer
	}
	timeoutMs := opts.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = defaultTimeout
	}
	retryBackoffMs := opts.RetryBackoffMs
	if retryBackoffMs < 0 {
		retryBackoffMs = 0
	}
	tlsConfig := &tls.Config{ServerName: opts.TLSServerName, InsecureSkipVerify: opts.TLSInsecureSkipVerify}

	return &client{
		nameServer: append([]string(nil), opts.NameServer...),
		useTLS:     opts.UseTLS,
		timeout:    time.Duration(timeoutMs) * time.Millisecond,
		retry:      opts.Retry,
		backoff:    time.Duration(retryBackoffMs) * time.Millisecond,
		tlsConfig:  tlsConfig,
		creds:      opts.Credentials,
		addrMap:    copyAddrMap(opts.BrokerAddrMap),
	}, nil
}

func (c *client) Close() error {
	return nil
}

func (c *client) invokeBroker(ctx context.Context, brokerAddr string, cmd *remotingCommand) (*remotingCommand, error) {
	return invokeSyncWithRetry(ctx, c.rewriteBrokerAddr(brokerAddr), c.useTLS, c.timeout, cmd, c.tlsConfig, c.retry, c.backoff, c.creds)
}

func (c *client) invokeNameServer(ctx context.Context, cmd *remotingCommand) (*remotingCommand, error) {
	if len(c.nameServer) == 0 {
		return nil, errEmptyNameServer
	}
	start := int(atomic.AddUint32(&c.nsCursor, 1)) % len(c.nameServer)
	var lastErr error
	for i := 0; i < len(c.nameServer); i++ {
		idx := (start + i) % len(c.nameServer)
		resp, err := invokeSyncWithRetry(ctx, c.nameServer[idx], c.useTLS, c.timeout, cmd, c.tlsConfig, c.retry, c.backoff, c.creds)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *client) rewriteBrokerAddr(addr string) string {
	if c.addrMap == nil {
		return addr
	}
	if mapped, ok := c.addrMap[addr]; ok && mapped != "" {
		return mapped
	}
	return addr
}

func copyAddrMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type Resolver interface {
	Resolve() []string
}

type passthroughResolver struct {
	addrs []string
}

func NewPassthroughResolver(addrs []string) Resolver {
	return &passthroughResolver{addrs: append([]string(nil), addrs...)}
}

func (r *passthroughResolver) Resolve() []string {
	return append([]string(nil), r.addrs...)
}

type AdminOption func(*Options)

func WithResolver(resolver Resolver) AdminOption {
	return func(opts *Options) {
		if resolver == nil {
			return
		}
		opts.NameServer = resolver.Resolve()
	}
}

func WithCredentials(creds Credentials) AdminOption {
	return func(opts *Options) {
		opts.Credentials = creds
	}
}

func WithNameServer(addrs []string) AdminOption {
	return func(opts *Options) {
		opts.NameServer = append([]string(nil), addrs...)
	}
}

func NewAdmin(adminOpts ...AdminOption) (Admin, error) {
	opts := Options{}
	for _, apply := range adminOpts {
		if apply == nil {
			continue
		}
		apply(&opts)
	}
	return New(opts)
}
