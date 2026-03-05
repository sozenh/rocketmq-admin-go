# rocketmq-admin-go

`rocketmq-admin-go` is a Go rewrite layer for RocketMQ mqadmin capability, designed to be imported as a module in Go tech stacks.

## Scope

This module focuses on the core admin interaction layer (not Java CLI command parity):

- Topic create/delete/list
- Subscription group query
- Publish queue discovery
- Cluster lookup

Current command coverage tracking is in `docs/command-tracking.md`.
Production readiness checklist is in `docs/production-readiness.md`.

## Dependency Boundary

Only Go standard library is used.

No Java `mqadmin` modules are imported.

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/sozenh/rocketmq-admin-go/mqadmin"
)

func main() {
    cli, err := mqadmin.New(mqadmin.Options{NameServer: []string{"127.0.0.1:9876"}})
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    err = cli.CreateTopic(context.Background(), mqadmin.CreateTopicRequest{Topic: "demo-topic", BrokerAddr: "127.0.0.1:10911"})
    if err != nil {
        log.Fatal(err)
    }
}
```
