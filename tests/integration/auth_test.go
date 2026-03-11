package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sozenh/rocketmq-admin-go/mqadmin"
	. "github.com/sozenh/rocketmq-admin-go/mqadmin"
)

type authIntegrationEnv struct {
	cli    mqadmin.Admin
	ctx    context.Context
	cancel context.CancelFunc
	broker []string
}

func setupAuthIntegrationEnv(t *testing.T) *authIntegrationEnv {
	t.Helper()

	accessKey := getenvDefault("RMQ_ACCESS_KEY", "rocketAdmin")
	secretKey := getenvDefault("RMQ_SECRET_KEY", "27LDO7PLS9KT5PE2EA3SSBLVXWV3")
	if accessKey == "" || secretKey == "" {
		t.Skip("skip: RMQ_ACCESS_KEY/RMQ_SECRET_KEY are required for integration auth tests")
	}

	ns := getenvDefault("RMQ_NS", "0.0.0.0:19876")
	broker00 := getenvDefault("RMQ_BROKER00", "127.0.0.1:10911")
	broker01 := getenvDefault("RMQ_BROKER01", "127.0.0.1:20911")
	broker10 := getenvDefault("RMQ_BROKER10", "127.0.0.1:30911")
	broker11 := getenvDefault("RMQ_BROKER11", "127.0.0.1:40911")

	var brokers = []string{broker00, broker01, broker10, broker11}

	cli, err := mqadmin.New(mqadmin.Options{
		NameServer:     []string{ns},
		Credentials:    mqadmin.Credentials{AccessKey: accessKey, SecretKey: secretKey},
		TimeoutMs:      8000,
		Retry:          1,
		RetryBackoffMs: 200,
	})
	if err != nil {
		t.Fatalf("new admin failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(func() {
		cancel()
		_ = cli.Close()
	})

	return &authIntegrationEnv{cli: cli, ctx: ctx, cancel: cancel, broker: brokers}
}

func uniqueAuthName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func buildACLOptions(username, topic string, brokers []string) []mqadmin.AclOption {
	return []mqadmin.AclOption{
		mqadmin.WithSubjectUser(username),
		mqadmin.WithResourceTopic(topic, []mqadmin.ActionType{mqadmin.ActionTypePub, mqadmin.ActionTypeSub}, mqadmin.DecisionTypeAllow, "127.0.0.1"),
		mqadmin.WithScopeBroker(brokers...),
	}
}

func TestIntegrationAuthCreateUser(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_create_user")
	user := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	t.Cleanup(func() {
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("create_non_existing_user", func(t *testing.T) {
		err := env.cli.CreateUser(env.ctx, user, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("CreateUser failed: %v", err)
		}
	})

	t.Run("create_existing_user", func(t *testing.T) {
		err := env.cli.CreateUser(env.ctx, user, mqadmin.WithBroker(env.broker...))
		if err == nil {
			t.Fatal("expected CreateUser to fail for existing user, got nil")
		}
	})
}

func TestIntegrationAuthUpdateUser(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_update_user")
	baseUser := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	updatedUser := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd2", UserType: UserTypeNormal}
	t.Cleanup(func() {
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("update_non_existing_user", func(t *testing.T) {
		err := env.cli.UpdateUser(env.ctx, baseUser, mqadmin.WithBroker(env.broker...))
		if err == nil {
			t.Fatal("expected UpdateUser to fail for non-existing user, got nil")
		}
	})

	if err := env.cli.CreateUser(env.ctx, baseUser, mqadmin.WithBroker(env.broker...)); err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}

	t.Run("update_existing_user", func(t *testing.T) {
		err := env.cli.UpdateUser(env.ctx, updatedUser, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("UpdateUser failed: %v", err)
		}
	})

}

func TestIntegrationAuthDeleteUser(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_delete_user")
	user := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	if err := env.cli.CreateUser(env.ctx, user, mqadmin.WithBroker(env.broker...)); err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}

	t.Run("delete_existing_user", func(t *testing.T) {
		err := env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("DeleteUser failed: %v", err)
		}
	})

	t.Run("delete_non_existing_user", func(t *testing.T) {
		err := env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("expected DeleteUser to be idempotent for non-existing user, got: %v", err)
		}
	})
}

func TestIntegrationAuthGetUser(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_get_user")
	userInfo := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	err := env.cli.CreateUser(env.ctx, userInfo, mqadmin.WithBroker(env.broker...))
	if err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("get_existing_user", func(t *testing.T) {
		users, err := env.cli.GetUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("GetUser existing failed: %v", err)
		}
		for _, broker := range env.broker {
			if users[broker] == nil || users[broker].Username != username {
				t.Fatalf("GetUser existing unexpected result: %+v", users)
			}
		}
	})

	t.Run("get_non_existing_user", func(t *testing.T) {
		users, err := env.cli.GetUser(env.ctx, uniqueAuthName("mqadmin_it_missing_get_user"), mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("expected GetUser non-existing to return no error, got: %v", err)
		}

		for _, broker := range env.broker {
			if users[broker] != nil {
				t.Fatalf("expected nil user for non-existing username, got: %+v", users[broker])
			}
		}
	})
}

func TestIntegrationAuthListUser(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_list_user")
	userInfo := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	err := env.cli.CreateUser(env.ctx, userInfo, mqadmin.WithBroker(env.broker...))
	if err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("list", func(t *testing.T) {
		_, err := env.cli.ListUser(env.ctx, "", mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("ListUser failed: %v", err)
		}
	})

	t.Run("list_with_matching_filter", func(t *testing.T) {
		usersByBroker, err := env.cli.ListUser(env.ctx, "mqadmin_it_list_user", mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("ListUser failed: %v", err)
		}

		for _, broker := range env.broker {
			foundUser := false
			for _, u := range usersByBroker[broker] {
				if u.Username == username {
					foundUser = true
					break
				}
			}
			if !foundUser {
				t.Fatalf("ListUser missing user %q in broker %s", username, env.broker)
			}
		}
	})

	t.Run("list_with_non_matching_filter", func(t *testing.T) {
		usersByBroker, err := env.cli.ListUser(env.ctx, uniqueAuthName("mqadmin_it_missing_filter"), mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("ListUser non-matching filter failed: %v", err)
		}

		for _, broker := range env.broker {
			foundUser := false
			for _, u := range usersByBroker[broker] {
				if u.Username == username {
					foundUser = true
					break
				}
			}
			if foundUser {
				t.Fatalf("expected no users for non-matching filter, got: %+v", usersByBroker[broker])
			}
		}
	})
}

func TestIntegrationAuthCreateAcl(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_create_acl_user")
	err := env.cli.CreateUser(env.ctx, mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}, mqadmin.WithBroker(env.broker...))
	if err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}

	subject := "User:" + username
	topic := uniqueAuthName("mqadmin_it_create_acl_topic")
	resource := "Topic:" + topic
	t.Cleanup(func() {
		_ = env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...))
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("create_acl_for_existing_user", func(t *testing.T) {
		err := env.cli.CreateAcl(env.ctx, buildACLOptions(username, topic, env.broker)...)
		if err != nil {
			t.Fatalf("CreateAcl failed: %v", err)
		}
	})

	t.Run("create_acl_for_non_existing_user", func(t *testing.T) {
		missingUser := uniqueAuthName("mqadmin_it_missing_subject")
		err := env.cli.CreateAcl(env.ctx, buildACLOptions(missingUser, topic, env.broker)...)
		if err == nil {
			t.Fatal("expected CreateAcl to fail for non-existing subject, got nil")
		}
	})
}

func TestIntegrationAuthUpdateAcl(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_update_acl_user")
	userInfo := mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}
	err := env.cli.CreateUser(env.ctx, userInfo, mqadmin.WithBroker(env.broker...))
	if err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}

	subject := "User:" + username
	topic := uniqueAuthName("mqadmin_it_update_acl_topic")
	resource := "Topic:" + topic
	err = env.cli.CreateAcl(env.ctx, buildACLOptions(username, topic, env.broker)...)
	if err != nil {
		t.Fatalf("pre-create acl failed: %v", err)
	}
	updated := []mqadmin.AclOption{
		mqadmin.WithSubjectUser(username),
		mqadmin.WithResourceTopic(topic, []mqadmin.ActionType{mqadmin.ActionTypeSub}, mqadmin.DecisionTypeAllow, "127.0.0.1"),
		mqadmin.WithScopeBroker(env.broker...),
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...))
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("update_existing_acl", func(t *testing.T) {
		err := env.cli.UpdateAcl(env.ctx, updated...)
		if err != nil {
			t.Fatalf("UpdateAcl failed: %v", err)
		}
	})

	t.Run("update_non_existing_acl", func(t *testing.T) {
		missingUser := uniqueAuthName("mqadmin_it_update_acl_missing_user")
		err := env.cli.UpdateAcl(env.ctx, buildACLOptions(missingUser, topic, env.broker)...)
		if err == nil {
			t.Fatal("expected UpdateAcl to fail for non-existing subject, got nil")
		}
	})
}

func TestIntegrationAuthDeleteAcl(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_delete_acl_user")
	if err := env.cli.CreateUser(env.ctx, mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}, mqadmin.WithBroker(env.broker...)); err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}
	subject := "User:" + username
	topic := uniqueAuthName("mqadmin_it_delete_acl_topic")
	resource := "Topic:" + topic
	if err := env.cli.CreateAcl(env.ctx, buildACLOptions(username, topic, env.broker)...); err != nil {
		t.Fatalf("pre-create acl failed: %v", err)
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...))
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("delete_existing_acl", func(t *testing.T) {
		if err := env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...)); err != nil {
			t.Fatalf("DeleteAcl failed: %v", err)
		}
	})

	t.Run("delete_non_existing_acl", func(t *testing.T) {
		err := env.cli.DeleteAcl(env.ctx, "User:"+uniqueAuthName("mqadmin_it_delete_acl_missing_user"), resource, "", mqadmin.WithBroker(env.broker...))
		if err == nil {
			t.Fatal("expected DeleteAcl to fail for non-existing subject, got nil")
		}
	})
}

func TestIntegrationAuthGetAcl(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_get_acl_user")
	subject := "User:" + username
	topic := uniqueAuthName("mqadmin_it_get_acl_topic")
	resource := "Topic:" + topic
	if err := env.cli.CreateUser(env.ctx, mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}, mqadmin.WithBroker(env.broker...)); err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}
	if err := env.cli.CreateAcl(env.ctx, buildACLOptions(username, topic, env.broker)...); err != nil {
		t.Fatalf("pre-create acl failed: %v", err)
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...))
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("get_existing_acl", func(t *testing.T) {
		acls, err := env.cli.GetAcl(env.ctx, subject, mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("GetAcl existing failed: %v", err)
		}
		for _, broker := range env.broker {
			if acls[broker] == nil || !strings.EqualFold(acls[broker].SubjectType+":"+acls[broker].SubjectName, subject) {
				t.Fatalf("GetAcl existing unexpected result: %+v", acls)
			}
		}

	})

	t.Run("get_non_existing_acl", func(t *testing.T) {
		_, err := env.cli.GetAcl(env.ctx, "User:"+uniqueAuthName("mqadmin_it_missing_get_acl"), mqadmin.WithBroker(env.broker...))
		if err == nil {
			t.Fatal("expected GetAcl to fail for non-existing subject, got nil")
		}
	})
}

func TestIntegrationAuthListAcl(t *testing.T) {
	env := setupAuthIntegrationEnv(t)
	username := uniqueAuthName("mqadmin_it_list_acl_user")
	subject := "User:" + username
	topic := uniqueAuthName("mqadmin_it_list_acl_topic")
	resource := "Topic:" + topic
	if err := env.cli.CreateUser(env.ctx, mqadmin.UserInfo{Username: username, Password: "P@ssw0rd", UserType: UserTypeNormal}, mqadmin.WithBroker(env.broker...)); err != nil {
		t.Fatalf("pre-create user failed: %v", err)
	}
	if err := env.cli.CreateAcl(env.ctx, buildACLOptions(username, topic, env.broker)...); err != nil {
		t.Fatalf("pre-create acl failed: %v", err)
	}
	t.Cleanup(func() {
		_ = env.cli.DeleteAcl(env.ctx, subject, resource, "", mqadmin.WithBroker(env.broker...))
		_ = env.cli.DeleteUser(env.ctx, username, mqadmin.WithBroker(env.broker...))
	})

	t.Run("list_with_matching_subject_filter", func(t *testing.T) {
		aclsByBroker, err := env.cli.ListAcl(env.ctx, subject, "", mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("ListAcl failed: %v", err)
		}
		for _, broker := range env.broker {
			foundACL := false
			for _, a := range aclsByBroker[broker] {
				if strings.EqualFold(a.SubjectType+":"+a.SubjectName, subject) {
					foundACL = true
					break
				}
			}
			if !foundACL {
				t.Fatalf("ListAcl missing subject %q in broker %s", subject, env.broker)
			}
		}

	})

	t.Run("list_with_non_matching_subject_filter", func(t *testing.T) {
		aclsByBroker, err := env.cli.ListAcl(env.ctx, "User:"+uniqueAuthName("mqadmin_it_missing_acl_filter"), "", mqadmin.WithBroker(env.broker...))
		if err != nil {
			t.Fatalf("ListAcl non-matching filter failed: %v", err)
		}

		for _, broker := range env.broker {
			foundACL := false
			for _, a := range aclsByBroker[broker] {
				if strings.EqualFold(a.SubjectType+":"+a.SubjectName, subject) {
					foundACL = true
					break
				}
			}
			if foundACL {
				t.Fatalf("expected no ACLs for non-matching filter, got: %+v", aclsByBroker[broker])
			}
		}
	})
}
