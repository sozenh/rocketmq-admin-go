package mqadmin

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCreateUserRollbackOnBrokerFailure(t *testing.T) {
	var createCalls int32
	var rollbackDeletes int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthCreateUser:
			if atomic.AddInt32(&createCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "create failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthDeleteUser:
			atomic.AddInt32(&rollbackDeletes, 1)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.CreateUser(context.Background(), UserInfo{Username: "rollback-user", Password: "p"}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected create user error")
	}
	if !strings.Contains(err.Error(), `create user "rollback-user" on broker `) {
		t.Fatalf("expected broker failure context, got: %v", err)
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&createCalls) != 2 {
		t.Fatalf("expected exactly 2 create attempts, got %d", atomic.LoadInt32(&createCalls))
	}
	if atomic.LoadInt32(&rollbackDeletes) != 1 {
		t.Fatalf("expected exactly 1 rollback delete, got %d", atomic.LoadInt32(&rollbackDeletes))
	}
}

func TestCreateUserRollbackFailureIsReported(t *testing.T) {
	var createCalls int32
	var rollbackDeletes int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthCreateUser:
			if atomic.AddInt32(&createCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "create failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthDeleteUser:
			atomic.AddInt32(&rollbackDeletes, 1)
			return &remotingCommand{Code: 18, Remark: "delete failed", Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.CreateUser(context.Background(), UserInfo{Username: "rollback-fail-user", Password: "p"}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected create user error")
	}
	if !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("expected rollback failure details, got: %v", err)
	}
	if atomic.LoadInt32(&rollbackDeletes) != 1 {
		t.Fatalf("expected exactly 1 rollback delete attempt, got %d", atomic.LoadInt32(&rollbackDeletes))
	}
}

func TestCreateAclRollbackOnBrokerFailure(t *testing.T) {
	var createCalls int32
	var rollbackDeletes int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthCreateAcl:
			if atomic.AddInt32(&createCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "create failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthDeleteAcl:
			atomic.AddInt32(&rollbackDeletes, 1)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.CreateAcl(context.Background(), AclInfo{Subject: "User:rollback-acl"}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected create acl error")
	}
	if !strings.Contains(err.Error(), `create acl "User:rollback-acl" on broker `) {
		t.Fatalf("expected broker failure context, got: %v", err)
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&createCalls) != 2 {
		t.Fatalf("expected exactly 2 create attempts, got %d", atomic.LoadInt32(&createCalls))
	}
	if atomic.LoadInt32(&rollbackDeletes) != 1 {
		t.Fatalf("expected exactly 1 rollback delete, got %d", atomic.LoadInt32(&rollbackDeletes))
	}
}

func TestCreateAclRollbackFailureIsReported(t *testing.T) {
	var createCalls int32
	var rollbackDeletes int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthCreateAcl:
			if atomic.AddInt32(&createCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "create failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthDeleteAcl:
			atomic.AddInt32(&rollbackDeletes, 1)
			return &remotingCommand{Code: 18, Remark: "delete failed", Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.CreateAcl(context.Background(), AclInfo{Subject: "User:rollback-acl-fail"}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected create acl error")
	}
	if !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("expected rollback failure details, got: %v", err)
	}
	if atomic.LoadInt32(&rollbackDeletes) != 1 {
		t.Fatalf("expected exactly 1 rollback delete attempt, got %d", atomic.LoadInt32(&rollbackDeletes))
	}
}

func TestUpdateUserRollbackOnBrokerFailure(t *testing.T) {
	var updateCalls int32
	var rollbackUpdates int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthGetUser:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"username":"rollback-update-user","password":"old"}`)}
		case requestCodeAuthUpdateUser:
			n := atomic.AddInt32(&updateCalls, 1)
			if n == 2 {
				return &remotingCommand{Code: 17, Remark: "update failed", Opaque: req.Opaque}
			}
			if n == 3 {
				atomic.AddInt32(&rollbackUpdates, 1)
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.UpdateUser(context.Background(), UserInfo{Username: "rollback-update-user", Password: "new"}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected update user error")
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&updateCalls) != 3 {
		t.Fatalf("expected 3 update calls (2 forward + 1 rollback), got %d", atomic.LoadInt32(&updateCalls))
	}
	if atomic.LoadInt32(&rollbackUpdates) != 1 {
		t.Fatalf("expected 1 rollback update, got %d", atomic.LoadInt32(&rollbackUpdates))
	}
}

func TestDeleteUserRollbackOnBrokerFailure(t *testing.T) {
	var deleteCalls int32
	var rollbackCreates int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthGetUser:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"username":"rollback-delete-user","password":"old"}`)}
		case requestCodeAuthDeleteUser:
			if atomic.AddInt32(&deleteCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "delete failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthCreateUser:
			atomic.AddInt32(&rollbackCreates, 1)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.DeleteUser(context.Background(), "rollback-delete-user", WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected delete user error")
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&deleteCalls) != 2 {
		t.Fatalf("expected 2 delete calls, got %d", atomic.LoadInt32(&deleteCalls))
	}
	if atomic.LoadInt32(&rollbackCreates) != 1 {
		t.Fatalf("expected 1 rollback create, got %d", atomic.LoadInt32(&rollbackCreates))
	}
}

func TestUpdateAclRollbackOnBrokerFailure(t *testing.T) {
	var updateCalls int32
	var rollbackUpdates int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthGetAcl:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"subject":"User:rollback-update-acl","policies":[]}`)}
		case requestCodeAuthUpdateAcl:
			n := atomic.AddInt32(&updateCalls, 1)
			if n == 2 {
				return &remotingCommand{Code: 17, Remark: "update failed", Opaque: req.Opaque}
			}
			if n == 3 {
				atomic.AddInt32(&rollbackUpdates, 1)
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.UpdateAcl(context.Background(), AclInfo{Subject: "User:rollback-update-acl", Policies: []PolicyInfo{}}, WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected update acl error")
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&updateCalls) != 3 {
		t.Fatalf("expected 3 update calls (2 forward + 1 rollback), got %d", atomic.LoadInt32(&updateCalls))
	}
	if atomic.LoadInt32(&rollbackUpdates) != 1 {
		t.Fatalf("expected 1 rollback update, got %d", atomic.LoadInt32(&rollbackUpdates))
	}
}

func TestDeleteAclRollbackOnBrokerFailure(t *testing.T) {
	var deleteCalls int32
	var rollbackCreates int32
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		switch req.Code {
		case requestCodeAuthGetAcl:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque, Body: []byte(`{"subject":"User:rollback-delete-acl","policies":[]}`)}
		case requestCodeAuthDeleteAcl:
			if atomic.AddInt32(&deleteCalls, 1) == 2 {
				return &remotingCommand{Code: 17, Remark: "delete failed", Opaque: req.Opaque}
			}
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		case requestCodeAuthCreateAcl:
			atomic.AddInt32(&rollbackCreates, 1)
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		default:
			return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
		}
	}
	b1, _, stop1 := startMockServer(t, handler)
	defer stop1()
	b2, _, stop2 := startMockServer(t, handler)
	defer stop2()

	c := &client{timeout: 2 * time.Second}
	err := c.DeleteAcl(context.Background(), "User:rollback-delete-acl", "Topic:T", "", WithBroker(b1, b2))
	if err == nil {
		t.Fatal("expected delete acl error")
	}
	if !strings.Contains(err.Error(), "rolled back 1 broker(s)") {
		t.Fatalf("expected rollback details, got: %v", err)
	}
	if atomic.LoadInt32(&deleteCalls) != 2 {
		t.Fatalf("expected 2 delete calls, got %d", atomic.LoadInt32(&deleteCalls))
	}
	if atomic.LoadInt32(&rollbackCreates) != 1 {
		t.Fatalf("expected 1 rollback create, got %d", atomic.LoadInt32(&rollbackCreates))
	}
}
