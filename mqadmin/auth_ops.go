package mqadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type UserInfo struct {
	Username   string `json:"username"`
	Password   string `json:"password,omitempty"`
	UserType   string `json:"userType,omitempty"`
	UserStatus string `json:"userStatus,omitempty"`
}

type AclInfo struct {
	Subject  string       `json:"subject"`
	Policies []PolicyInfo `json:"policies"`
}

type PolicyInfo struct {
	PolicyType string            `json:"policyType,omitempty"`
	Entries    []PolicyEntryInfo `json:"entries"`
}

type PolicyEntryInfo struct {
	Resource  string   `json:"resource"`
	Actions   []string `json:"actions"`
	SourceIps []string `json:"sourceIps"`
	Decision  string   `json:"decision"`
}

func (c *client) CreateUser(ctx context.Context, user UserInfo, opts ...ScopeOption) error {
	if user.Username == "" {
		return errEmptyUsername
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	createdOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.createUserOnBroker(ctx, user, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(createdOn) - 1; i >= 0; i-- {
				if deleteErr := c.deleteUserOnBroker(ctx, user.Username, createdOn[i]); deleteErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", createdOn[i], deleteErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("create user %q on broker %s failed: %w; rollback failed: %v", user.Username, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("create user %q on broker %s failed: %w; rolled back %d broker(s)", user.Username, brokerAddr, err, len(createdOn))
		}
		createdOn = append(createdOn, brokerAddr)
	}
	return nil
}

func (c *client) UpdateUser(ctx context.Context, user UserInfo, opts ...ScopeOption) error {
	if user.Username == "" {
		return errEmptyUsername
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	prevUsers, prevErr := c.GetUser(ctx, user.Username, opts...)
	if prevErr != nil {
		return fmt.Errorf("prefetch current state failed: %w", prevErr)
	}

	updatedOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.updateUserOnBroker(ctx, user, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(updatedOn) - 1; i >= 0; i-- {
				prevUser := prevUsers[updatedOn[i]]
				if prevUser == nil {
					continue
				}
				if revertErr := c.updateUserOnBroker(ctx, *prevUser, updatedOn[i]); revertErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", updatedOn[i], revertErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("update user %q on broker %s failed: %w; rollback failed: %v", user.Username, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("update user %q on broker %s failed: %w; rolled back %d broker(s)", user.Username, brokerAddr, err, len(updatedOn))
		}
		updatedOn = append(updatedOn, brokerAddr)
	}
	return nil
}

func (c *client) DeleteUser(ctx context.Context, username string, opts ...ScopeOption) error {
	if username == "" {
		return errEmptyUsername
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	prevUsers, prevErr := c.GetUser(ctx, username, opts...)
	if prevErr != nil {
		return fmt.Errorf("prefetch current state failed: %w", prevErr)
	}

	deletedOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.deleteUserOnBroker(ctx, username, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(deletedOn) - 1; i >= 0; i-- {
				prevUser := prevUsers[deletedOn[i]]
				if prevUser == nil {
					continue
				}
				if revertErr := c.createUserOnBroker(ctx, *prevUser, deletedOn[i]); revertErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", deletedOn[i], revertErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("delete user %q on broker %s failed: %w; rollback failed: %v", username, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("delete user %q on broker %s failed: %w; rolled back %d broker(s)", username, brokerAddr, err, len(deletedOn))
		}
		deletedOn = append(deletedOn, brokerAddr)
	}
	return nil
}

func (c *client) GetUser(ctx context.Context, username string, opts ...ScopeOption) (map[string]*UserInfo, error) {
	if username == "" {
		return nil, errEmptyUsername
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return nil, fmt.Errorf("resolve brokers failed: %w", err)
	}

	var errs error
	users := make(map[string]*UserInfo, len(brokers))
	for _, brokerAddr := range brokers {
		user, err := c.getUserFromBroker(ctx, username, brokerAddr)
		if err == nil {
			users[brokerAddr] = user
		} else {
			errs = errors.Join(errs, fmt.Errorf("get user %q from broker %s failed: %w", username, brokerAddr, err))
		}
	}

	return users, errs
}

func (c *client) ListUser(ctx context.Context, filter string, opts ...ScopeOption) (map[string][]UserInfo, error) {
	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return nil, fmt.Errorf("resolve brokers failed: %w", err)
	}

	var errs error
	users := make(map[string][]UserInfo, len(brokers))
	for _, brokerAddr := range brokers {
		brokerUsers, err := c.listUserFromBroker(ctx, filter, brokerAddr)
		if err == nil {
			users[brokerAddr] = brokerUsers
		} else {
			errs = errors.Join(errs, fmt.Errorf("list user filter %q from broker %s failed: %w", filter, brokerAddr, err))
		}
	}

	return users, errs
}

func (c *client) CreateAcl(ctx context.Context, acl AclInfo, opts ...ScopeOption) error {
	if acl.Subject == "" {
		return errEmptySubject
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	createdOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.createAclOnBroker(ctx, acl, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(createdOn) - 1; i >= 0; i-- {
				if deleteErr := c.deleteAclOnBroker(ctx, acl.Subject, "", "", createdOn[i]); deleteErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", createdOn[i], deleteErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("create acl %q on broker %s failed: %w; rollback failed: %v", acl.Subject, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("create acl %q on broker %s failed: %w; rolled back %d broker(s)", acl.Subject, brokerAddr, err, len(createdOn))
		}
		createdOn = append(createdOn, brokerAddr)
	}
	return nil
}

func (c *client) UpdateAcl(ctx context.Context, acl AclInfo, opts ...ScopeOption) error {
	if acl.Subject == "" {
		return errEmptySubject
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	prevAcls, prevErr := c.GetAcl(ctx, acl.Subject, opts...)
	if prevErr != nil {
		return fmt.Errorf("prefetch current state failed: %w", prevErr)
	}

	updatedOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.updateAclOnBroker(ctx, acl, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(updatedOn) - 1; i >= 0; i-- {
				prevAcl := prevAcls[updatedOn[i]]
				if prevAcl == nil {
					continue
				}
				if revertErr := c.updateAclOnBroker(ctx, *prevAcl, updatedOn[i]); revertErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", updatedOn[i], revertErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("update acl %q on broker %s failed: %w; rollback failed: %v", acl.Subject, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("update acl %q on broker %s failed: %w; rolled back %d broker(s)", acl.Subject, brokerAddr, err, len(updatedOn))
		}
		updatedOn = append(updatedOn, brokerAddr)
	}
	return nil
}

func (c *client) DeleteAcl(ctx context.Context, subject, resource, policyType string, opts ...ScopeOption) error {
	if subject == "" {
		return errEmptySubject
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return fmt.Errorf("resolve brokers failed: %w", err)
	}

	prevAcls, prevErr := c.GetAcl(ctx, subject, opts...)
	if prevErr != nil {
		return fmt.Errorf("prefetch current state failed: %w", prevErr)
	}

	deletedOn := make([]string, 0, len(brokers))
	for _, brokerAddr := range brokers {
		err = c.deleteAclOnBroker(ctx, subject, resource, policyType, brokerAddr)
		if err != nil {
			var rollbackErr error
			for i := len(deletedOn) - 1; i >= 0; i-- {
				prevAcl := prevAcls[deletedOn[i]]
				if prevAcl == nil {
					continue
				}
				if revertErr := c.createAclOnBroker(ctx, *prevAcl, deletedOn[i]); revertErr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("broker %s: %w", deletedOn[i], revertErr))
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("delete acl %q on broker %s failed: %w; rollback failed: %v", subject, brokerAddr, err, rollbackErr)
			}
			return fmt.Errorf("delete acl %q on broker %s failed: %w; rolled back %d broker(s)", subject, brokerAddr, err, len(deletedOn))
		}
		deletedOn = append(deletedOn, brokerAddr)
	}
	return nil
}

func (c *client) GetAcl(ctx context.Context, subject string, opts ...ScopeOption) (map[string]*AclInfo, error) {
	if subject == "" {
		return nil, errEmptySubject
	}

	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return nil, fmt.Errorf("resolve brokers failed: %w", err)
	}

	var errs error
	acls := make(map[string]*AclInfo, len(brokers))
	for _, brokerAddr := range brokers {
		acl, err := c.getAclFromBroker(ctx, subject, brokerAddr)
		if err == nil {
			acls[brokerAddr] = acl
		} else {
			errs = errors.Join(errs, fmt.Errorf("get acl %q from broker %s failed: %w", subject, brokerAddr, err))
		}
	}

	return acls, errs
}

func (c *client) ListAcl(ctx context.Context, subjectFilter, resourceFilter string, opts ...ScopeOption) (map[string][]AclInfo, error) {
	scopeConfig := BuildScopeConfig(opts...)
	brokers, err := scopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return nil, fmt.Errorf("resolve brokers failed: %w", err)
	}

	var errs error
	acls := make(map[string][]AclInfo, len(brokers))
	for _, brokerAddr := range brokers {
		brokerAcls, err := c.listAclFromBroker(ctx, subjectFilter, resourceFilter, brokerAddr)
		if err == nil {
			acls[brokerAddr] = brokerAcls
		} else {
			errs = errors.Join(errs, fmt.Errorf("list acl from broker %s failed: %w", brokerAddr, err))
		}
	}

	return acls, errs
}

func (c *client) CopyUsers(ctx context.Context, username string, source ScopeSelector, target ScopeSelector) error {

	sourceScopeConfig := BuildScopeConfig(source.opts...)
	sourceBrokers, err := sourceScopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return err
	}

	targetScopeConfig := BuildScopeConfig(target.opts...)
	targetBrokers, err := targetScopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return err
	}

	if username != "" {
		var (
			u       *UserInfo
			lastErr error
		)
		for _, sourceBroker := range sourceBrokers {
			u, err = c.getUserFromBroker(ctx, sourceBroker, username)
			if err != nil {
				lastErr = err
				continue
			}
			if u != nil {
				break
			}
		}
		if u == nil {
			if lastErr != nil {
				return lastErr
			}
			return errNoBrokerFromRoute
		}
		for _, targetBroker := range targetBrokers {
			if _, err := c.getUserFromBroker(ctx, targetBroker, username); err != nil {
				if err := c.createUserOnBroker(ctx, *u, targetBroker); err != nil {
					return err
				}
				continue
			}
			if err := c.updateUserOnBroker(ctx, *u, targetBroker); err != nil {
				return err
			}
		}
		return nil
	}

	userMap := map[string]UserInfo{}
	var lastErr error
	for _, sourceBroker := range sourceBrokers {
		users, listErr := c.listUserFromBroker(ctx, sourceBroker, "")
		if listErr != nil {
			lastErr = listErr
			continue
		}
		for _, u := range users {
			if u.Username != "" {
				userMap[u.Username] = u
			}
		}
	}
	if len(userMap) == 0 {
		if lastErr != nil {
			return lastErr
		}
		return nil
	}
	for _, targetBroker := range targetBrokers {
		for _, u := range userMap {
			if _, err := c.getUserFromBroker(ctx, targetBroker, u.Username); err != nil {
				if err := c.createUserOnBroker(ctx, u, targetBroker); err != nil {
					return err
				}
				continue
			}
			if err := c.updateUserOnBroker(ctx, u, targetBroker); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *client) CopyAcls(ctx context.Context, subject string, source ScopeSelector, target ScopeSelector) error {

	sourceScopeConfig := BuildScopeConfig(source.opts...)
	sourceBrokers, err := sourceScopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return err
	}

	targetScopeConfig := BuildScopeConfig(target.opts...)
	targetBrokers, err := targetScopeConfig.getBrokerAddrs(ctx, c, true)
	if err != nil {
		return err
	}

	if subject != "" {
		var (
			a       *AclInfo
			lastErr error
		)
		for _, sourceBroker := range sourceBrokers {
			a, err = c.getAclFromBroker(ctx, sourceBroker, subject)
			if err != nil {
				lastErr = err
				continue
			}
			if a != nil {
				break
			}
		}
		if a == nil {
			if lastErr != nil {
				return lastErr
			}
			return errNoBrokerFromRoute
		}
		for _, targetBroker := range targetBrokers {
			if _, err := c.getAclFromBroker(ctx, targetBroker, subject); err != nil {
				if err := c.createAclOnBroker(ctx, *a, targetBroker); err != nil {
					return err
				}
				continue
			}
			if err := c.updateAclOnBroker(ctx, *a, targetBroker); err != nil {
				return err
			}
		}
		return nil
	}

	aclMap := map[string]AclInfo{}
	var lastErr error
	for _, sourceBroker := range sourceBrokers {
		acls, listErr := c.listAclFromBroker(ctx, sourceBroker, "", "")
		if listErr != nil {
			lastErr = listErr
			continue
		}
		for _, a := range acls {
			if a.Subject != "" {
				aclMap[a.Subject] = a
			}
		}
	}
	if len(aclMap) == 0 {
		if lastErr != nil {
			return lastErr
		}
		return nil
	}
	for _, targetBroker := range targetBrokers {
		for _, a := range aclMap {
			if _, err := c.getAclFromBroker(ctx, targetBroker, a.Subject); err != nil {
				if err := c.createAclOnBroker(ctx, a, targetBroker); err != nil {
					return err
				}
				continue
			}
			if err := c.updateAclOnBroker(ctx, a, targetBroker); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *client) createUserOnBroker(ctx context.Context, user UserInfo, brokerAddr string) error {
	body, err := json.Marshal(user)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthCreateUser, toMapString(map[string]any{"username": user.Username}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) updateUserOnBroker(ctx context.Context, user UserInfo, brokerAddr string) error {
	body, err := json.Marshal(user)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthUpdateUser, toMapString(map[string]any{"username": user.Username}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) deleteUserOnBroker(ctx context.Context, username, brokerAddr string) error {
	ext := map[string]any{
		"username": username,
	}
	_, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthDeleteUser, toMapString(ext)))
	return err
}

func (c *client) getUserFromBroker(ctx context.Context, username, brokerAddr string) (*UserInfo, error) {
	ext := map[string]any{
		"username": username,
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthGetUser, toMapString(ext)))
	if err != nil {
		return nil, err
	}
	var user *UserInfo
	if resp.Body != nil {
		user = &UserInfo{}
		err = json.Unmarshal(resp.Body, user)
		if err != nil {
			return nil, err
		}
	}

	return user, nil
}

func (c *client) listUserFromBroker(ctx context.Context, filter, brokerAddr string) ([]UserInfo, error) {
	ext := map[string]any{
		"filter": filter,
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthListUser, toMapString(ext)))
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

func (c *client) createAclOnBroker(ctx context.Context, acl AclInfo, brokerAddr string) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthCreateAcl, toMapString(map[string]any{"subject": acl.Subject}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) updateAclOnBroker(ctx context.Context, acl AclInfo, brokerAddr string) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return err
	}
	cmd := newCommand(requestCodeAuthUpdateAcl, toMapString(map[string]any{"subject": acl.Subject}))
	cmd.Body = body
	_, err = c.invokeBroker(ctx, brokerAddr, cmd)
	return err
}

func (c *client) deleteAclOnBroker(ctx context.Context, subject, resource, policyType, brokerAddr string) error {
	ext := map[string]any{
		"subject":  subject,
		"resource": resource,
	}
	if policyType != "" {
		ext["policyType"] = policyType
	}
	_, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthDeleteAcl, toMapString(ext)))
	return err
}

func (c *client) getAclFromBroker(ctx context.Context, subject, brokerAddr string) (*AclInfo, error) {
	ext := map[string]any{
		"subject": subject,
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthGetAcl, toMapString(ext)))
	if err != nil {
		return nil, err
	}
	var acl *AclInfo
	if resp.Body != nil {
		acl = &AclInfo{}
		if err := json.Unmarshal(resp.Body, acl); err != nil {
			return nil, err
		}
	}

	return acl, nil
}

func (c *client) listAclFromBroker(ctx context.Context, subjectFilter, resourceFilter, brokerAddr string) ([]AclInfo, error) {
	ext := map[string]any{
		"subjectFilter":  subjectFilter,
		"resourceFilter": resourceFilter,
	}
	resp, err := c.invokeBroker(ctx, brokerAddr, newCommand(requestCodeAuthListAcl, toMapString(ext)))
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
