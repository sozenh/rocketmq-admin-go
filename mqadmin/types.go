package mqadmin

import (
	"context"
	"errors"
	"sort"
	"strings"
)

type StatsAllResult struct {
	TopicCount int                                   `json:"topicCount"`
	Topics     map[string]map[string]TopicStatsTable `json:"topics"`
}

type Options struct {
	NameServer            []string
	Credentials           Credentials
	UseTLS                bool
	TLSServerName         string
	TLSInsecureSkipVerify bool
	TimeoutMs             int
	Retry                 int
	RetryBackoffMs        int
	BrokerAddrMap         map[string]string
}

type Credentials struct {
	AccessKey     string
	SecretKey     string
	SecurityToken string
}

func (c Credentials) Enabled() bool {
	return c.AccessKey != "" && c.SecretKey != ""
}

type CreateTopicRequest struct {
	Topic           string
	BrokerAddr      string
	DefaultTopic    string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
}

type DeleteTopicRequest struct {
	Topic      string
	BrokerAddr string
	NameSrv    []string
	Cluster    string
}

type SubscriptionGroupConfig struct {
	GroupName                      string            `json:"groupName"`
	ConsumeEnable                  bool              `json:"consumeEnable,omitempty"`
	ConsumeFromMinEnable           bool              `json:"consumeFromMinEnable,omitempty"`
	ConsumeBroadcastEnable         bool              `json:"consumeBroadcastEnable,omitempty"`
	ConsumeMessageOrderly          bool              `json:"consumeMessageOrderly,omitempty"`
	RetryQueueNums                 int               `json:"retryQueueNums,omitempty"`
	RetryMaxTimes                  int               `json:"retryMaxTimes,omitempty"`
	BrokerID                       int64             `json:"brokerId,omitempty"`
	WhichBrokerWhenConsumeSlowly   int64             `json:"whichBrokerWhenConsumeSlowly,omitempty"`
	NotifyConsumerIdsChangedEnable bool              `json:"notifyConsumerIdsChangedEnable,omitempty"`
	GroupSysFlag                   int               `json:"groupSysFlag,omitempty"`
	ConsumeTimeoutMinute           int               `json:"consumeTimeoutMinute,omitempty"`
	Attributes                     map[string]string `json:"attributes,omitempty"`
}

type DeleteSubscriptionGroupRequest struct {
	BrokerAddr   string
	GroupName    string
	RemoveOffset bool
}

type KVConfig struct {
	Namespace string
	Key       string
	Value     string
}

type SetConsumeModeRequest struct {
	BrokerAddr       string
	Topic            string
	Group            string
	Mode             string
	PopShareQueueNum int
}

type StaticTopicRequest struct {
	BrokerAddr    string
	CreateTopic   CreateTopicRequest
	MappingDetail map[string]any
	Force         bool
}

type LiteTopicRequest struct {
	BrokerAddr  string
	ParentTopic string
	LiteTopic   string
	Group       string
	ClientID    string
	TopK        int
	MaxCount    int
}

type BrokerMembershipRequest struct {
	ClusterName string
	BrokerName  string
	BrokerID    int64
	BrokerAddr  string
	ContainerID string
	ConfigPath  string
}

type SendMessageRequest struct {
	BrokerAddr string
	Topic      string
	Body       string
	Tag        string
	Keys       string
	WaitStore  bool
}

type ElectMasterRequest struct {
	ClusterName string
	BrokerName  string
	BrokerID    int64
}

type CleanBrokerDataRequest struct {
	ClusterName string
	BrokerName  string
	BrokerID    int64
	BrokerIDs   string
	CleanLiving bool
}

type ResetOffsetRequest struct {
	Topic     string
	Group     string
	Timestamp int64
	Force     bool
	Cluster   string
}

type CloneGroupOffsetRequest struct {
	SrcGroup  string
	DestGroup string
	Topic     string
	Offline   bool
}

type UpdateTopicPermRequest struct {
	Topic string
	Perm  int
}

type TopicStatsTable struct {
	OffsetTable map[string]any `json:"offsetTable"`
}

type TopicList struct {
	TopicList   []string       `json:"topicList"`
	BrokerAddr  string         `json:"brokerAddr"`
	TopicDetail map[string]any `json:"-"`
}

type MessageQueue struct {
	Topic      string
	BrokerName string
	QueueID    int
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]any `json:"subscriptionGroupTable"`
	ForbiddenTable         map[string]any `json:"forbiddenTable"`
	DataVersion            map[string]any `json:"dataVersion"`
}

var (
	errEmptyNameServer      = errors.New("mqadmin: nameserver address list is empty")
	errEmptyTopic           = errors.New("mqadmin: topic is required")
	errEmptyBrokerAddr      = errors.New("mqadmin: broker address is required when creating topic")
	errNoBrokerFromRoute    = errors.New("mqadmin: no broker address available in topic route")
	errClusterUnsupported   = errors.New("mqadmin: cluster lookup failed due to empty route data")
	errNoNameServerProvided = errors.New("mqadmin: no nameserver provided for delete topic")
	errEmptyGroup           = errors.New("mqadmin: group is required")
	errEmptySrcGroup        = errors.New("mqadmin: srcGroup is required")
	errEmptyDestGroup       = errors.New("mqadmin: destGroup is required")
	errEmptySubject         = errors.New("mqadmin: subject is required")
	errEmptyUsername        = errors.New("mqadmin: username is required")
	errEmptyNamespace       = errors.New("mqadmin: namespace is required")
	errEmptyKey             = errors.New("mqadmin: key is required")
	errEmptyValue           = errors.New("mqadmin: value is required")
	errEmptyGroupName       = errors.New("mqadmin: groupName is required")
	errEmptyBrokerName      = errors.New("mqadmin: brokerName is required")
	errEmptyMode            = errors.New("mqadmin: consume mode is required")
	errUnsupportedMode      = errors.New("mqadmin: consume mode only supports PULL/POP")
)

type ScopeOption func(*ScopeConfig)

type ScopeConfig struct {
	brokers  []string
	clusters []string
}

func WithBroker(addrs ...string) ScopeOption {
	return func(cfg *ScopeConfig) {
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				cfg.brokers = append(cfg.brokers, addr)
			}
		}
	}
}

func WithCluster(names ...string) ScopeOption {
	return func(cfg *ScopeConfig) {
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name != "" {
				cfg.clusters = append(cfg.clusters, name)
			}
		}
	}
}

func BuildScopeConfig(opts ...ScopeOption) ScopeConfig {
	cfg := ScopeConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func uniqueStrings(in []string) []string {
	set := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, ok := set[v]; ok {
			continue
		}
		set[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (cfg *ScopeConfig) getBrokerAddrs(ctx context.Context, mqadmin Admin, includeSlaves bool) ([]string, error) {
	if len(cfg.brokers) == 0 && len(cfg.clusters) == 0 {
		return nil, errEmptyBrokerAddr
	}

	brokerAddrs := append([]string(nil), cfg.brokers...)

	if len(cfg.clusters) > 0 {
		clusterInfo, err := mqadmin.ClusterList(ctx)
		if err != nil {
			return nil, err
		}
		for _, clusterName := range cfg.clusters {
			brokerNames := clusterInfo.ClusterAddrTable[clusterName]

			for _, brokerName := range brokerNames {
				broker := clusterInfo.BrokerAddrTable[brokerName]
				master := broker.BrokerAddrs["0"]
				if master != "" {
					brokerAddrs = append(brokerAddrs, master)
				}

				if includeSlaves {
					for idx, addr := range broker.BrokerAddrs {
						if addr != "" && idx != "0" {
							brokerAddrs = append(brokerAddrs, addr)
						}
					}
				}
			}
		}
	}

	brokerAddrs = uniqueStrings(brokerAddrs)
	if len(brokerAddrs) == 0 {
		return nil, errNoBrokerFromRoute
	}
	return brokerAddrs, nil
}

type ScopeSelector struct {
	opts []ScopeOption
}

func NewScopeSelector(opts ...ScopeOption) ScopeSelector {
	return ScopeSelector{opts: append([]ScopeOption(nil), opts...)}
}
