package mqadmin

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

func (c *client) GetBrokerClusterAclInfo(ctx context.Context, brokerAddr string) (*AclInfoV1, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetBrokerClusterAclConfig, nil))
	if err != nil {
		return nil, err
	}
	out := &AclInfoV1{}
	if len(resp.Body) == 0 {
		out.GlobalWhiteAddrs = []string{}
		out.PlainAccessConfigs = []PlainAccessConfig{}
		return out, nil
	}
	if err := json.Unmarshal(resp.Body, out); err != nil {
		return nil, err
	}
	if out.GlobalWhiteAddrs == nil {
		out.GlobalWhiteAddrs = []string{}
	}
	if out.PlainAccessConfigs == nil {
		out.PlainAccessConfigs = []PlainAccessConfig{}
	}
	return out, nil
}

func (c *client) UpdateAclConfig(ctx context.Context, cfg AclConfigV1) error {
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
	brokers := c.allBrokerAddrs(ctx)
	if len(brokers) == 0 {
		return errNoBrokerFromRoute
	}
	for _, brokerAddr := range brokers {
		if _, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeUpdateAndCreateAclConfig, bodies)); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) DeleteAclConfig(ctx context.Context, accessKey string) error {
	brokers := c.allBrokerAddrs(ctx)
	if len(brokers) == 0 {
		return errNoBrokerFromRoute
	}
	for _, brokerAddr := range brokers {
		if _, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeDeleteAclConfig, toMapString(map[string]any{"accessKey": accessKey}))); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) UpdateGlobalWhiteAddrsConfig(ctx context.Context, addrs []string) error {
	joined := strings.Join(addrs, ",")
	brokers := c.allBrokerAddrs(ctx)
	if len(brokers) == 0 {
		return errNoBrokerFromRoute
	}
	for _, brokerAddr := range brokers {
		if _, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeUpdateGlobalWhiteAddrsConfig, toMapString(map[string]any{"globalWhiteAddrs": joined}))); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) allBrokerAddrs(ctx context.Context) []string {
	cluster, err := c.ClusterList(ctx)
	if err != nil {
		return nil
	}
	set := map[string]struct{}{}
	table, ok := cluster["brokerAddrTable"].(map[string]any)
	if !ok {
		return nil
	}
	for _, v := range table {
		bm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		addrs, ok := bm["brokerAddrs"].(map[string]any)
		if !ok {
			continue
		}
		for _, raw := range addrs {
			switch t := raw.(type) {
			case string:
				if t != "" {
					set[t] = struct{}{}
				}
			case float64:
				s := strconv.FormatFloat(t, 'f', -1, 64)
				if s != "" {
					set[s] = struct{}{}
				}
			}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}
