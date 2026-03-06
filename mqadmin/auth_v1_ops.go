package mqadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type AclConfigV1 struct {
	AccessKey          string
	SecretKey          string
	WhiteRemoteAddress string
	DefaultTopicPerm   string
	DefaultGroupPerm   string
	Admin              bool
	TopicPerms         []string
	GroupPerms         []string
}

type AclInfoV1 struct {
	GlobalWhiteAddrs   []string            `json:"globalWhiteAddrs"`
	PlainAccessConfigs []PlainAccessConfig `json:"plainAccessConfigs"`
}

type PlainAccessConfig struct {
	Admin              bool     `json:"admin"`
	AccessKey          string   `json:"accessKey"`
	SecretKey          string   `json:"secretKey"`
	WhiteRemoteAddress string   `json:"whiteRemoteAddress"`
	TopicPerms         []string `json:"topicPerms"`
	GroupPerms         []string `json:"groupPerms"`
	DefaultTopicPerm   string   `json:"defaultTopicPerm"`
	DefaultGroupPerm   string   `json:"defaultGroupPerm"`
}

func (c *client) UpdateAclConfig(ctx context.Context, cfg AclConfigV1, opts ...ScopeOption) error {
	bodies := toMapString(map[string]any{
		"accessKey": cfg.AccessKey,
		"secretKey": cfg.SecretKey,
	})
	if cfg.Admin {
		bodies["admin"] = "true"
	}
	if cfg.DefaultTopicPerm != "" {
		bodies["defaultTopicPerm"] = cfg.DefaultTopicPerm
	}
	if cfg.DefaultGroupPerm != "" {
		bodies["defaultGroupPerm"] = cfg.DefaultGroupPerm
	}
	if cfg.WhiteRemoteAddress != "" {
		bodies["whiteRemoteAddress"] = cfg.WhiteRemoteAddress
	}
	if len(cfg.TopicPerms) > 0 {
		bodies["topicPerms"] = strings.Join(cfg.TopicPerms, ",")
	}
	if len(cfg.GroupPerms) > 0 {
		bodies["groupPerms"] = strings.Join(cfg.GroupPerms, ",")
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	for _, brokerAddr := range brokers {
		_, err = c.invokeBroker(ctx, brokerAddr,
			newCommand(requestCodeUpdateAndCreateAclConfig, bodies))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) DeleteAclConfig(ctx context.Context, accessKey string, opts ...ScopeOption) error {
	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	for _, brokerAddr := range brokers {
		_, err = c.invokeBroker(ctx, brokerAddr,
			newCommand(requestCodeDeleteAclConfig, toMapString(map[string]any{"accessKey": accessKey})))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) UpdateGlobalWhiteAddrsConfig(ctx context.Context, addrs []string, opts ...ScopeOption) error {
	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	joined := strings.Join(addrs, ",")
	for _, brokerAddr := range brokers {
		_, err = c.invokeBroker(ctx, brokerAddr,
			newCommand(requestCodeUpdateGlobalWhiteAddrsConfig, toMapString(map[string]any{"globalWhiteAddrs": joined})))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) GetBrokerClusterAclInfo(ctx context.Context, opts ...ScopeOption) (map[string]*AclInfoV1, error) {
	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return nil, fmt.Errorf("resolve brokers failed: %w", err)
	}

	var errs error
	acls := make(map[string]*AclInfoV1, len(brokers))

	for _, brokerAddr := range brokers {
		resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetBrokerClusterAclConfig, nil))
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("get acl from broker %s failed: %w", brokerAddr, err))
			continue
		}
		acl := &AclInfoV1{}
		if len(resp.Body) != 0 {
			err = json.Unmarshal(resp.Body, acl)
			if err != nil {
				return nil, err
			}
		}

		if acl.GlobalWhiteAddrs == nil {
			acl.GlobalWhiteAddrs = []string{}
		}
		if acl.PlainAccessConfigs == nil {
			acl.PlainAccessConfigs = []PlainAccessConfig{}
		}
	}

	return acls, nil
}
