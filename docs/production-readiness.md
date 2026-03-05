# mqadmin-go 生产可用性清单

## 已完成

- 纯标准库 remoting 协议实现（无 `rocketmq-client-go/v2` 依赖）。
- 可配置超时、重试次数、重试退避。
- NameServer 多节点容灾调用（轮询起点 + 逐节点尝试）。
- TLS 连接参数暴露（`UseTLS`、`TLSServerName`、`TLSInsecureSkipVerify`）。
- Topic / Offset / 状态配置 / Auth 命令能力实现。

## 待真实集群验证

- 多版本兼容性：4.x / 5.x NameServer + Broker。
- 故障场景：主从切换、网络抖动、半开连接、超时重试行为。
- 安全场景：TLS 证书链校验、双向 TLS、ACL 场景。
- 高并发：并发管理请求下的连接与延迟稳定性。

## 联调建议

1. 单机集群冒烟：Topic/Auth/配置全链路。
2. 双 NameServer + 多 Broker：验证容灾与重试。
3. 注入故障（限速/丢包/断连）：验证错误模型与恢复。
4. 回归脚本：对齐 Java mqadmin 关键命令的输出语义。
