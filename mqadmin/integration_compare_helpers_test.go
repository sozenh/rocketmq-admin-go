//go:build integration

package mqadmin

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type compareHarness struct {
	cli          Admin
	ctx          context.Context
	javaNamesrv  string
	javaCluster  string
	controller   string
	brokerName0  string
	broker0Svc   string
	broker1Svc   string
	mqadminPath  string
	goNS         string
	goBroker0    string
	goBroker1    string
	runJava      func(args ...string) (string, error)
	runJavaShell func(shell string) (string, error)

	topicA   string
	topicB   string
	topicC   string
	groupA   string
	groupB   string
	groupC   string
	user1    string
	user2    string
	subject1 string
	subject2 string
	kvNS     string
	kvKey    string
	cmdExist map[string]bool
}

func newCompareHarness(t *testing.T) *compareHarness {
	t.Helper()

	namespace := getenvDefault("RMQ_K8S_NS", "qfusion-admin")
	pod := getenvDefault("RMQ_JAVA_POD", "rocketmq-hhhhhzhen-0-0-0")
	container := getenvDefault("RMQ_JAVA_CONTAINER", "broker")
	mqadminPath := getenvDefault("RMQ_JAVA_MQADMIN", "/root/rocketmq/bin/mqadmin")
	javaNamesrv := getenvDefault("RMQ_JAVA_NS", "rocketmq-hhhhhzhen-nameserver-client:9876")
	javaCluster := getenvDefault("RMQ_JAVA_CLUSTER", "rocketmq-hhhhhzhen")
	controller := getenvDefault("RMQ_JAVA_CONTROLLER", "")
	brokerName0 := getenvDefault("RMQ_JAVA_BROKER_NAME0", "rocketmq-hhhhhzhen-0")
	broker0Svc := getenvDefault("RMQ_JAVA_BROKER0", "rocketmq-hhhhhzhen-0-0:10911")
	broker1Svc := getenvDefault("RMQ_JAVA_BROKER1", "rocketmq-hhhhhzhen-1-0:10911")

	goNS := getenvDefault("RMQ_NS", "127.0.0.1:19876")
	goBroker0 := getenvDefault("RMQ_BROKER0", "127.0.0.1:10911")
	goBroker1 := getenvDefault("RMQ_BROKER1", "127.0.0.1:20911")
	brokerAddrMap := map[string]string{}
	broker0ServiceName := strings.SplitN(broker0Svc, ":", 2)[0]
	broker1ServiceName := strings.SplitN(broker1Svc, ":", 2)[0]
	if ip, err := getServiceEndpointIP(namespace, broker0ServiceName); err == nil && ip != "" {
		brokerAddrMap[ip+":10911"] = goBroker0
	}
	if ip, err := getServiceEndpointIP(namespace, broker1ServiceName); err == nil && ip != "" {
		brokerAddrMap[ip+":10911"] = goBroker1
	}

	cli, err := New(Options{NameServer: []string{goNS}, TimeoutMs: 7000, Retry: 1, RetryBackoffMs: 200, BrokerAddrMap: brokerAddrMap})
	if err != nil {
		t.Fatalf("new admin failed: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	t.Cleanup(cancel)

	sfx := fmt.Sprintf("%d", time.Now().Unix())
	resolvedPath, err := resolveMQAdminPath(namespace, pod, container, mqadminPath)
	if err != nil {
		t.Fatalf("resolve mqadmin path failed: %v", err)
	}

	runJava := func(args ...string) (string, error) {
		base := []string{"exec", "-n", namespace, pod, "-c", container, "--", resolvedPath}
		base = append(base, args...)
		return runKubectlWithRetry(base)
	}

	runJavaShell := func(shell string) (string, error) {
		return runKubectlWithRetry([]string{"exec", "-n", namespace, pod, "-c", container, "--", "sh", "-lc", shell})
	}

	return &compareHarness{
		cli:          cli,
		ctx:          ctx,
		javaNamesrv:  javaNamesrv,
		javaCluster:  javaCluster,
		controller:   controller,
		brokerName0:  brokerName0,
		broker0Svc:   broker0Svc,
		broker1Svc:   broker1Svc,
		mqadminPath:  resolvedPath,
		goNS:         goNS,
		goBroker0:    goBroker0,
		goBroker1:    goBroker1,
		runJava:      runJava,
		runJavaShell: runJavaShell,
		topicA:       "cmp_topic_a_" + sfx,
		topicB:       "cmp_topic_b_" + sfx,
		topicC:       "cmp_topic_c_" + sfx,
		groupA:       "cmp_group_a_" + sfx,
		groupB:       "cmp_group_b_" + sfx,
		groupC:       "cmp_group_c_" + sfx,
		user1:        "u_" + sfx,
		user2:        "u2_" + sfx,
		subject1:     "User:u_" + sfx,
		subject2:     "User:u2_" + sfx,
		kvNS:         "mqadmin_go_cmp",
		kvKey:        "k_" + sfx,
		cmdExist:     map[string]bool{},
	}
}

func (h *compareHarness) assertBoth(t *testing.T, name string, goFn func() error, javaFn func() (string, error)) {
	t.Helper()
	if err := goFn(); err != nil {
		t.Fatalf("%s go failed: %v", name, err)
	}
	if out, err := javaFn(); err != nil {
		t.Fatalf("%s java failed: %v, out=%s", name, err, out)
	}
}

func (h *compareHarness) assertCrossMutate(
	t *testing.T,
	name string,
	goWrite func() error,
	javaRead func() (string, error),
	javaWrite func() (string, error),
	goRead func() error,
) {
	t.Helper()
	if err := goWrite(); err != nil {
		t.Fatalf("%s go write failed: %v", name, err)
	}
	if out, err := javaRead(); err != nil {
		t.Fatalf("%s java read after go write failed: %v, out=%s", name, err, out)
	}
	if out, err := javaWrite(); err != nil {
		t.Fatalf("%s java write failed: %v, out=%s", name, err, out)
	}
	if err := goRead(); err != nil {
		t.Fatalf("%s go read after java write failed: %v", name, err)
	}
}

func (h *compareHarness) javaCommandExists(cmd string) bool {
	if ok, exists := h.cmdExist[cmd]; exists {
		return ok
	}
	out, err := h.runJava("help", cmd)
	if err != nil && strings.Contains(out, "not exist") {
		h.cmdExist[cmd] = false
		return false
	}
	h.cmdExist[cmd] = true
	return true
}

func sanitizedEnv() []string {
	result := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "http_proxy=") || strings.HasPrefix(e, "https_proxy=") || strings.HasPrefix(e, "HTTP_PROXY=") || strings.HasPrefix(e, "HTTPS_PROXY=") || strings.HasPrefix(e, "no_proxy=") || strings.HasPrefix(e, "NO_PROXY=") {
			continue
		}
		result = append(result, e)
	}
	return result
}

func contains(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func getServiceEndpointIP(namespace, service string) (string, error) {
	cmd := exec.Command("kubectl", "get", "endpoints", "-n", namespace, service, "-o", "jsonpath={.subsets[0].addresses[0].ip}")
	cmd.Env = sanitizedEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func resolveMQAdminPath(namespace, pod, container, preferred string) (string, error) {
	candidates := []string{preferred, "/root/rocketmq/bin/mqadmin", "/opt/rocketmq/bin/mqadmin", "/rocketmq/bin/mqadmin"}
	for _, c := range candidates {
		cmd := exec.Command("kubectl", "exec", "-n", namespace, pod, "-c", container, "--", "sh", "-lc", "[ -x \""+c+"\" ] && echo \""+c+"\"")
		cmd.Env = sanitizedEnv()
		out, err := cmd.CombinedOutput()
		if err != nil {
			continue
		}
		path := strings.TrimSpace(string(out))
		if strings.Contains(path, "\n") {
			parts := strings.Split(path, "\n")
			path = strings.TrimSpace(parts[len(parts)-1])
		}
		if path != "" {
			return path, nil
		}
	}
	return "", fmt.Errorf("mqadmin executable not found in pod %s/%s", namespace, pod)
}

func runKubectlWithRetry(args []string) (string, error) {
	var lastOut string
	var lastErr error
	for i := 0; i < 3; i++ {
		cmd := exec.Command("kubectl", args...)
		cmd.Env = sanitizedEnv()
		out, err := cmd.CombinedOutput()
		lastOut = string(out)
		lastErr = err
		if err == nil {
			return lastOut, nil
		}
		if !strings.Contains(lastOut, "fatal error: concurrent map read and map write") {
			return lastOut, err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return lastOut, lastErr
}
