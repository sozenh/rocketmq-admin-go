package mqadmin

import (
	"context"
	"encoding/json"
)

func (c *client) BrokerStatus(ctx context.Context, brokerAddr string) (map[string]string, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetBrokerRuntimeInfo, nil))
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	if len(resp.Body) == 0 {
		return out, nil
	}
	wrap := map[string]map[string]string{}
	if err := json.Unmarshal(resp.Body, &wrap); err != nil {
		return nil, err
	}
	if t, ok := wrap["table"]; ok {
		return t, nil
	}
	return out, nil
}

func (c *client) GetBrokerConfig(ctx context.Context, brokerAddr string) (map[string]string, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeGetBrokerConfig, nil))
	if err != nil {
		return nil, err
	}
	return bytesToProperties(resp.Body), nil
}

func (c *client) UpdateBrokerConfig(ctx context.Context, brokerAddr string, properties map[string]string) error {
	cmd := newCommand(requestCodeUpdateBrokerConfig, nil)
	cmd.Body = propertiesToBytes(properties)
	_, err := c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) UpdateKVConfig(ctx context.Context, kv KVConfig) error {
	if kv.Namespace == "" {
		return errEmptyNamespace
	}
	if kv.Key == "" {
		return errEmptyKey
	}
	if kv.Value == "" {
		return errEmptyValue
	}
	cmd := newCommand(requestCodePutKVConfig, toMapString(map[string]any{
		"namespace": kv.Namespace,
		"key":       kv.Key,
		"value":     kv.Value,
	}))
	_, err := c.invokeNameServer(ctx, cmd)
	return err
}

func (c *client) DeleteKVConfig(ctx context.Context, namespace, key string) error {
	if namespace == "" {
		return errEmptyNamespace
	}
	if key == "" {
		return errEmptyKey
	}
	_, err := c.invokeNameServer(ctx, newCommand(requestCodeDeleteKVConfig, toMapString(map[string]any{
		"namespace": namespace,
		"key":       key,
	})))
	return err
}

func (c *client) WipeWritePerm(ctx context.Context, brokerName string, namesrvs []string) error {
	if brokerName == "" {
		return errEmptyBrokerName
	}
	targets := namesrvs
	if len(targets) == 0 {
		targets = c.nameServer
	}
	for _, ns := range targets {
		if _, err := invokeSyncWithRetry(ctx, ns, c.useTLS, c.timeout, newCommand(requestCodeWipeWritePermOfBroker, toMapString(map[string]any{"brokerName": brokerName})), c.tlsConfig, c.retry, c.backoff, c.creds); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) AddWritePerm(ctx context.Context, brokerName string, namesrvs []string) error {
	if brokerName == "" {
		return errEmptyBrokerName
	}
	targets := namesrvs
	if len(targets) == 0 {
		targets = c.nameServer
	}
	for _, ns := range targets {
		if _, err := invokeSyncWithRetry(ctx, ns, c.useTLS, c.timeout, newCommand(requestCodeAddWritePermOfBroker, toMapString(map[string]any{"brokerName": brokerName})), c.tlsConfig, c.retry, c.backoff, c.creds); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) GetNamesrvConfig(ctx context.Context, namesrvs []string) (map[string]map[string]string, error) {
	targets := namesrvs
	if len(targets) == 0 {
		targets = c.nameServer
	}
	out := map[string]map[string]string{}
	for _, ns := range targets {
		resp, err := invokeSyncWithRetry(ctx, ns, c.useTLS, c.timeout, newCommand(requestCodeGetNamesrvConfig, nil), c.tlsConfig, c.retry, c.backoff, c.creds)
		if err != nil {
			return nil, err
		}
		out[ns] = bytesToProperties(resp.Body)
	}
	return out, nil
}

func (c *client) UpdateNamesrvConfig(ctx context.Context, namesrvs []string, properties map[string]string) error {
	targets := namesrvs
	if len(targets) == 0 {
		targets = c.nameServer
	}
	for _, ns := range targets {
		cmd := newCommand(requestCodeUpdateNamesrvConfig, nil)
		cmd.Body = propertiesToBytes(properties)
		if _, err := invokeSyncWithRetry(ctx, ns, c.useTLS, c.timeout, cmd, c.tlsConfig, c.retry, c.backoff, c.creds); err != nil {
			return err
		}
	}
	return nil
}
