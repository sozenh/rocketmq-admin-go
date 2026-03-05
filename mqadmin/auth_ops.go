package mqadmin

import (
	"context"
	"encoding/json"
)

func (c *client) CreateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	if user.Username == "" {
		return errEmptyUsername
	}
	body, err := json.Marshal(user)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthCreateUser, toMapString(map[string]any{"username": user.Username}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) UpdateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	if user.Username == "" {
		return errEmptyUsername
	}
	body, err := json.Marshal(user)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthUpdateUser, toMapString(map[string]any{"username": user.Username}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) DeleteUser(ctx context.Context, brokerAddr, username string) error {
	if username == "" {
		return errEmptyUsername
	}
	_, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthDeleteUser, toMapString(map[string]any{"username": username})))
	return err
}

func (c *client) GetUser(ctx context.Context, brokerAddr, username string) (*UserInfo, error) {
	if username == "" {
		return nil, errEmptyUsername
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthGetUser, toMapString(map[string]any{"username": username})))
	if err != nil {
		return nil, err
	}
	user := &UserInfo{}
	if err := json.Unmarshal(resp.Body, user); err != nil {
		return nil, err
	}
	return user, nil
}

func (c *client) ListUser(ctx context.Context, brokerAddr, filter string) ([]UserInfo, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthListUser, toMapString(map[string]any{"filter": filter})))
	if err != nil {
		return nil, err
	}
	var users []UserInfo
	if len(resp.Body) == 0 {
		return users, nil
	}
	if err := json.Unmarshal(resp.Body, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func (c *client) CreateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	if acl.Subject == "" {
		return errEmptySubject
	}
	body, err := json.Marshal(acl)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthCreateAcl, toMapString(map[string]any{"subject": acl.Subject}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) UpdateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	if acl.Subject == "" {
		return errEmptySubject
	}
	body, err := json.Marshal(acl)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthUpdateAcl, toMapString(map[string]any{"subject": acl.Subject}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) DeleteAcl(ctx context.Context, brokerAddr, subject, resource, policyType string) error {
	if subject == "" {
		return errEmptySubject
	}
	ext := map[string]any{"subject": subject, "resource": resource}
	if policyType != "" {
		ext["policyType"] = policyType
	}
	_, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthDeleteAcl, toMapString(ext)))
	return err
}

func (c *client) GetAcl(ctx context.Context, brokerAddr, subject string) (*AclInfo, error) {
	if subject == "" {
		return nil, errEmptySubject
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthGetAcl, toMapString(map[string]any{"subject": subject})))
	if err != nil {
		return nil, err
	}
	acl := &AclInfo{}
	if err := json.Unmarshal(resp.Body, acl); err != nil {
		return nil, err
	}
	return acl, nil
}

func (c *client) ListAcl(ctx context.Context, brokerAddr, subjectFilter, resourceFilter string) ([]AclInfo, error) {
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthListAcl, toMapString(map[string]any{"subjectFilter": subjectFilter, "resourceFilter": resourceFilter})))
	if err != nil {
		return nil, err
	}
	var acls []AclInfo
	if len(resp.Body) == 0 {
		return acls, nil
	}
	if err := json.Unmarshal(resp.Body, &acls); err != nil {
		return nil, err
	}
	return acls, nil
}
