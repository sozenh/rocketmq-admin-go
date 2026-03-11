package mqadmin

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildAclInfoWithUserAndTopic(t *testing.T) {
	acl, scope, err := BuildAclInfo(
		WithSubjectUser("alice"),
		WithResourceTopic("demo-topic", []ActionType{ActionTypePub}, DecisionTypeAllow, "127.0.0.1"),
		WithScopeBroker("127.0.0.1:10911"),
	)
	if err != nil {
		t.Fatalf("BuildAclInfo failed: %v", err)
	}
	if acl.Subject != "User:alice" {
		t.Fatalf("unexpected subject: %s", acl.Subject)
	}
	if len(acl.Policies) != 1 || len(acl.Policies[0].Entries) != 1 {
		t.Fatalf("unexpected policies: %+v", acl.Policies)
	}
	entry := acl.Policies[0].Entries[0]
	if entry.Resource != "Topic:demo-topic" {
		t.Fatalf("unexpected resource: %s", entry.Resource)
	}
	if len(entry.Actions) != 1 || entry.Actions[0] != ActionTypePub {
		t.Fatalf("unexpected actions: %+v", entry.Actions)
	}
	if entry.Decision != DecisionTypeAllow {
		t.Fatalf("unexpected decision: %s", entry.Decision)
	}
	if len(scope) != 1 {
		t.Fatalf("unexpected scope options length: %d", len(scope))
	}
}

func TestBuildAclInfoValidation(t *testing.T) {
	if _, _, err := BuildAclInfo(WithResourceTopic("demo-topic", []ActionType{ActionTypePub}, DecisionTypeAllow)); err == nil {
		t.Fatal("expected missing subject error")
	}
	if _, _, err := BuildAclInfo(WithSubjectUser("alice")); err == nil {
		t.Fatal("expected missing entries error")
	}
	if _, _, err := BuildAclInfo(WithSubjectUser("alice"), WithResourceTopic("", []ActionType{ActionTypePub}, DecisionTypeAllow)); err == nil {
		t.Fatal("expected empty topic error")
	}
}

func TestParseAclInfoSplitByResourceType(t *testing.T) {
	parsed := ParseAclInfo(&AclInfo{
		Subject: "User:alice",
		Policies: []PolicyInfo{
			{Entries: []PolicyEntryInfo{{Resource: "Topic:t1", Actions: []ActionType{ActionTypePub}, Decision: DecisionTypeAllow}}},
			{Entries: []PolicyEntryInfo{{Resource: "Group:g1", Actions: []ActionType{ActionTypeSub}, Decision: DecisionTypeDeny}}},
			{Entries: []PolicyEntryInfo{{Resource: "Custom:r1", Actions: []ActionType{ActionTypeGet}, Decision: DecisionTypeAllow}}},
		},
	})

	if parsed.SubjectType != "User" || parsed.SubjectName != "alice" {
		t.Fatalf("unexpected parsed subject: %+v", parsed)
	}
	if len(parsed.Topics) != 1 || parsed.Topics[0].ResourceName != "t1" {
		t.Fatalf("unexpected topic split: %+v", parsed.Topics)
	}
	if len(parsed.Groups) != 1 || parsed.Groups[0].ResourceName != "g1" {
		t.Fatalf("unexpected group split: %+v", parsed.Groups)
	}
	if len(parsed.Others) != 1 || parsed.Others[0].ResourceType != ResourceType("Custom") {
		t.Fatalf("unexpected others split: %+v", parsed.Others)
	}
}

func TestCreateAclSendsComposedPayload(t *testing.T) {
	var got AclInfo
	handler := func(req *remotingCommand, _ int32) *remotingCommand {
		if req.Code != requestCodeAuthCreateAcl {
			t.Fatalf("unexpected request code: %d", req.Code)
		}
		if err := json.Unmarshal(req.Body, &got); err != nil {
			t.Fatalf("unmarshal request body failed: %v", err)
		}
		return &remotingCommand{Code: responseCodeSuccess, Opaque: req.Opaque}
	}
	b, _, stop := startMockServer(t, handler)
	defer stop()

	c := &client{timeout: 2 * time.Second}
	err := c.CreateAcl(
		context.Background(),
		WithSubjectUser("bob"),
		WithResourceTopic("orders", []ActionType{ActionTypePub, ActionTypeSub}, DecisionTypeAllow, "127.0.0.1"),
		WithScopeBroker(b),
	)
	if err != nil {
		t.Fatalf("CreateAcl failed: %v", err)
	}
	if got.Subject != "User:bob" {
		t.Fatalf("unexpected subject payload: %s", got.Subject)
	}
	if len(got.Policies) != 1 || len(got.Policies[0].Entries) != 1 {
		t.Fatalf("unexpected payload policies: %+v", got.Policies)
	}
	if got.Policies[0].Entries[0].Resource != "Topic:orders" {
		t.Fatalf("unexpected payload resource: %s", got.Policies[0].Entries[0].Resource)
	}
}
