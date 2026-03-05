//go:build integration

package mqadmin

import "testing"

func runCompareAuthCases(t *testing.T, h *compareHarness) {
	t.Helper()

	h.assertCrossMutate(t, "createUser",
		func() error {
			return h.cli.CreateUser(h.ctx, h.goBroker0, UserInfo{Username: h.user1, Password: "P@ssw0rd", UserType: "NORMAL"})
		},
		func() (string, error) {
			return h.runJava("getUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-u", h.user1)
		},
		func() (string, error) {
			return h.runJava("createUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-u", h.user2, "-p", "P@ssw0rd", "-t", "NORMAL")
		},
		func() error {
			_, err := h.cli.GetUser(h.ctx, h.goBroker0, h.user2)
			return err
		},
	)

	h.assertBoth(t, "updateUser", func() error {
		return h.cli.UpdateUser(h.ctx, h.goBroker0, UserInfo{Username: h.user1, Password: "P@ssw0rd2"})
	}, func() (string, error) {
		return h.runJava("updateUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-u", h.user1, "-p", "P@ssw0rd2")
	})

	if _, err := h.cli.GetUser(h.ctx, h.goBroker0, h.user1); err != nil {
		t.Fatalf("go getUser failed: %v", err)
	}
	if out, err := h.runJava("getUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-u", h.user1); err != nil {
		t.Fatalf("java getUser failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ListUser(h.ctx, h.goBroker0, "u_"); err != nil {
		t.Fatalf("go listUser failed: %v", err)
	}
	if out, err := h.runJava("listUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-f", "u_"); err != nil {
		t.Fatalf("java listUser failed: %v, out=%s", err, out)
	}

	h.assertCrossMutate(t, "createAcl",
		func() error {
			return h.cli.CreateAcl(h.ctx, h.goBroker0, AclInfo{Subject: h.subject1, Policies: []PolicyInfo{{Entries: []PolicyEntryInfo{{Resource: "Topic:" + h.topicA, Actions: []string{"PUB", "SUB"}, SourceIps: []string{"127.0.0.1"}, Decision: "ALLOW"}}}}})
		},
		func() (string, error) {
			return h.runJava("getAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", h.subject1)
		},
		func() (string, error) {
			return h.runJava("createAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", h.subject2, "-r", "Topic:"+h.topicA, "-a", "PUB,SUB", "-i", "127.0.0.1", "-d", "ALLOW")
		},
		func() error {
			_, err := h.cli.GetAcl(h.ctx, h.goBroker0, h.subject2)
			return err
		},
	)

	h.assertBoth(t, "updateAcl", func() error {
		return h.cli.UpdateAcl(h.ctx, h.goBroker0, AclInfo{Subject: h.subject1, Policies: []PolicyInfo{{Entries: []PolicyEntryInfo{{Resource: "Topic:" + h.topicA, Actions: []string{"SUB"}, SourceIps: []string{"127.0.0.1"}, Decision: "ALLOW"}}}}})
	}, func() (string, error) {
		return h.runJava("updateAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", h.subject1, "-r", "Topic:"+h.topicA, "-a", "SUB", "-i", "127.0.0.1", "-d", "ALLOW")
	})

	if _, err := h.cli.GetAcl(h.ctx, h.goBroker0, h.subject1); err != nil {
		t.Fatalf("go getAcl failed: %v", err)
	}
	if out, err := h.runJava("getAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", h.subject1); err != nil {
		t.Fatalf("java getAcl failed: %v, out=%s", err, out)
	}

	if _, err := h.cli.ListAcl(h.ctx, h.goBroker0, "User:", ""); err != nil {
		t.Fatalf("go listAcl failed: %v", err)
	}
	if out, err := h.runJava("listAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", "User:"); err != nil {
		t.Fatalf("java listAcl failed: %v, out=%s", err, out)
	}

	h.assertCrossMutate(t, "copyUser",
		func() error {
			return h.cli.CopyUsers(h.ctx, h.goBroker0, h.goBroker1, h.user1)
		},
		func() (string, error) {
			return h.runJava("getUser", "-n", h.javaNamesrv, "-b", h.broker1Svc, "-u", h.user1)
		},
		func() (string, error) {
			return h.runJava("copyUser", "-n", h.javaNamesrv, "-f", h.broker0Svc, "-t", h.broker1Svc, "-u", h.user2)
		},
		func() error {
			_, err := h.cli.GetUser(h.ctx, h.goBroker1, h.user2)
			return err
		},
	)

	h.assertCrossMutate(t, "copyAcl",
		func() error {
			return h.cli.CopyAcls(h.ctx, h.goBroker0, h.goBroker1, h.subject1)
		},
		func() (string, error) {
			return h.runJava("getAcl", "-n", h.javaNamesrv, "-b", h.broker1Svc, "-s", h.subject1)
		},
		func() (string, error) {
			return h.runJava("copyAcl", "-n", h.javaNamesrv, "-f", h.broker0Svc, "-t", h.broker1Svc, "-s", h.subject2)
		},
		func() error {
			_, err := h.cli.GetAcl(h.ctx, h.goBroker1, h.subject2)
			return err
		},
	)

	h.assertBoth(t, "deleteAcl", func() error {
		return h.cli.DeleteAcl(h.ctx, h.goBroker0, h.subject1, "Topic:"+h.topicA, "")
	}, func() (string, error) {
		return h.runJava("deleteAcl", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-s", h.subject2, "-r", "Topic:"+h.topicA)
	})

	h.assertBoth(t, "deleteUser", func() error {
		return h.cli.DeleteUser(h.ctx, h.goBroker0, h.user1)
	}, func() (string, error) {
		return h.runJava("deleteUser", "-n", h.javaNamesrv, "-b", h.broker0Svc, "-u", h.user2)
	})
	_ = h.cli.DeleteAcl(h.ctx, h.goBroker1, h.subject1, "Topic:"+h.topicA, "")
	_ = h.cli.DeleteAcl(h.ctx, h.goBroker1, h.subject2, "Topic:"+h.topicA, "")
	_ = h.cli.DeleteUser(h.ctx, h.goBroker1, h.user1)
	_ = h.cli.DeleteUser(h.ctx, h.goBroker1, h.user2)

	h.assertBoth(t, "deleteTopic", func() error {
		return h.cli.DeleteTopic(h.ctx, DeleteTopicRequest{Topic: h.topicB, BrokerAddr: h.goBroker0, NameSrv: []string{h.goNS}})
	}, func() (string, error) {
		return h.runJava("deleteTopic", "-n", h.javaNamesrv, "-c", h.javaCluster, "-t", h.topicC)
	})
}
