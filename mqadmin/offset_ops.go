package mqadmin

import "context"

func (c *client) ResetOffsetByTime(ctx context.Context, req ResetOffsetRequest) (map[string]map[string]any, error) {
	if req.Topic == "" {
		return nil, errEmptyTopic
	}
	if req.Group == "" {
		return nil, errEmptyGroup
	}
	route, err := c.getTopicRoute(ctx, req.Topic)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for _, bd := range route.BrokerDatas {
		if req.Cluster != "" && req.Cluster != bd.Cluster {
			continue
		}
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
		ext := toMapString(map[string]any{
			"group":     req.Group,
			"topic":     req.Topic,
			"queueId":   -1,
			"timestamp": req.Timestamp,
			"isForce":   req.Force,
			"offset":    -1,
		})
		resp, err := c.invokeBroker(ctx, addr, newCommand(requestCodeInvokeBrokerToResetOffset, ext))
		if err != nil {
			return nil, err
		}
		m := map[string]any{}
		if len(resp.Body) > 0 {
			m = decodeResetOffsetBody(resp.Body)
		}
		out[bd.BrokerName] = m
	}
	return out, nil
}

func (c *client) SkipAccumulation(ctx context.Context, topic, group, cluster string) (map[string]map[string]any, error) {
	return c.ResetOffsetByTime(ctx, ResetOffsetRequest{Topic: topic, Group: group, Timestamp: -1, Force: true, Cluster: cluster})
}

func (c *client) CloneGroupOffset(ctx context.Context, req CloneGroupOffsetRequest) error {
	if req.Topic == "" {
		return errEmptyTopic
	}
	if req.SrcGroup == "" {
		return errEmptySrcGroup
	}
	if req.DestGroup == "" {
		return errEmptyDestGroup
	}
	route, err := c.getTopicRoute(ctx, req.Topic)
	if err != nil {
		return err
	}
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
		if _, err := c.invokeBroker(ctx, addr, newCommand(requestCodeCloneGroupOffset, toMapString(map[string]any{
			"srcGroup":  req.SrcGroup,
			"destGroup": req.DestGroup,
			"topic":     req.Topic,
			"offline":   req.Offline,
		}))); err != nil {
			return err
		}
	}
	return nil
}
