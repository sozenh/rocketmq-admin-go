package mqadmin

import (
	"context"
	"encoding/json"
)

func (c *client) UpdateSubGroup(ctx context.Context, brokerAddr string, cfg SubscriptionGroupConfig) error {
	if brokerAddr == "" {
		return errEmptyBrokerAddr
	}
	if cfg.GroupName == "" {
		return errEmptyGroupName
	}
	cmd := newCommand(requestCodeUpdateAndCreateSubscriptionGroup, nil)
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) UpdateSubGroupList(ctx context.Context, brokerAddr string, cfgs []SubscriptionGroupConfig) error {
	if brokerAddr == "" {
		return errEmptyBrokerAddr
	}
	body := map[string]any{"groupConfigList": cfgs}
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeUpdateAndCreateSubscriptionGroupList, nil)
	cmd.Body = encoded
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) DeleteSubscriptionGroup(ctx context.Context, req DeleteSubscriptionGroupRequest) error {
	if req.BrokerAddr == "" {
		return errEmptyBrokerAddr
	}
	if req.GroupName == "" {
		return errEmptyGroupName
	}
	_, err := c.invokeBroker(ctx, req.BrokerAddr, newCommand(requestCodeDeleteSubscriptionGroup, toMapString(map[string]any{
		"groupName":   req.GroupName,
		"cleanOffset": req.RemoveOffset,
	})))
	return err
}

func (c *client) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string) (*SubscriptionGroupWrapper, error) {
	ext := toMapString(map[string]any{"groupSeq": 0, "maxGroupNum": 1000, "dataVersion": ""})
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetAllSubscriptionGroup, ext))
	if err != nil {
		return nil, err
	}
	out := &SubscriptionGroupWrapper{}
	if err := json.Unmarshal(resp.Body, out); err != nil {
		return nil, err
	}
	if out.SubscriptionGroupTable == nil {
		out.SubscriptionGroupTable = map[string]any{}
	}
	if out.ForbiddenTable == nil {
		out.ForbiddenTable = map[string]any{}
	}
	if out.DataVersion == nil {
		out.DataVersion = map[string]any{}
	}
	return out, nil
}
