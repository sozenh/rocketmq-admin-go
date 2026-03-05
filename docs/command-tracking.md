# mqadmin-go 设计与命令跟踪

## 设计思路

- 目标：在 Go 技术栈内提供可引入的 mqadmin 能力层。
- 约束：不引入 `rocketmq-client-go/v2`，仅使用 Go 标准库。
- 协议：直接实现 RocketMQ remoting TCP 帧编解码（JSON header + 可选 body）。
- 结构：
  - `mqadmin/protocol.go`：请求码、编解码、同步调用、properties 编解码。
  - `mqadmin/mqadmin.go`：`Admin` 接口与客户端基础能力（New/Close/invoke）。
  - `mqadmin/topic_ops.go`/`group_ops.go`/`offset_ops.go`/`config_ops.go`/`auth_ops.go`/`extended_ops.go`：按类型拆分命令实现。
  - `mqadmin/types.go`：请求/响应结构与错误定义。

## 命令全量跟踪（按 MQAdminStartup 注册）

状态说明：`已实现` 表示已在 `mqadmin-go/mqadmin/Admin` 暴露对应能力；`未实现` 表示尚未落地。

### Topic（12）

| 命令类 | 状态 |
|---|---|
| UpdateTopicSubCommand | 已实现 |
| UpdateTopicListSubCommand | 已实现 |
| DeleteTopicSubCommand | 已实现 |
| UpdateTopicPermSubCommand | 已实现 |
| TopicRouteSubCommand | 已实现 |
| TopicStatusSubCommand | 已实现 |
| TopicClusterSubCommand | 已实现 |
| TopicListSubCommand | 已实现 |
| UpdateOrderConfCommand | 已实现 |
| AllocateMQSubCommand | 已实现 |
| UpdateStaticTopicSubCommand | 已实现 |
| RemappingStaticTopicSubCommand | 已实现 |

### Consumer / Group（8）

| 命令类 | 状态 |
|---|---|
| UpdateSubGroupSubCommand | 已实现 |
| UpdateSubGroupListSubCommand | 已实现 |
| SetConsumeModeSubCommand | 已实现 |
| DeleteSubscriptionGroupCommand | 已实现 |
| ConsumerProgressSubCommand | 已实现 |
| ConsumerStatusSubCommand | 已实现 |
| GetConsumerConfigSubCommand | 已实现 |
| StartMonitoringSubCommand | 已实现 |

### Offset（3）

| 命令类 | 状态 |
|---|---|
| CloneGroupOffsetCommand | 已实现 |
| ResetOffsetByTimeCommand | 已实现 |
| SkipAccumulationSubCommand | 已实现 |

### Broker / Container / HA（19）

| 命令类 | 状态 |
|---|---|
| AddBrokerSubCommand | 已实现 |
| RemoveBrokerSubCommand | 已实现 |
| ResetMasterFlushOffsetSubCommand | 已实现 |
| BrokerStatusSubCommand | 已实现 |
| BrokerConsumeStatsSubCommad | 已实现 |
| UpdateBrokerConfigSubCommand | 已实现 |
| CleanExpiredCQSubCommand | 已实现 |
| DeleteExpiredCommitLogSubCommand | 已实现 |
| CleanUnusedTopicCommand | 已实现 |
| SendMsgStatusCommand | 已实现 |
| GetBrokerConfigCommand | 已实现 |
| GetColdDataFlowCtrInfoSubCommand | 已实现 |
| UpdateColdDataFlowCtrGroupConfigSubCommand | 已实现 |
| RemoveColdDataFlowCtrGroupConfigSubCommand | 已实现 |
| CommitLogSetReadAheadSubCommand | 已实现 |
| SwitchTimerEngineSubCommand | 已实现 |
| HAStatusSubCommand | 已实现 |
| GetSyncStateSetSubCommand | 已实现 |
| GetBrokerEpochSubCommand | 已实现 |

### Message（11）

| 命令类 | 状态 |
|---|---|
| QueryMsgByIdSubCommand | 已实现 |
| QueryMsgByKeySubCommand | 已实现 |
| QueryMsgByUniqueKeySubCommand | 已实现 |
| QueryMsgByOffsetSubCommand | 已实现 |
| QueryMsgTraceByIdSubCommand | 已实现 |
| PrintMessageSubCommand | 已实现 |
| PrintMessageByQueueCommand | 已实现 |
| SendMessageCommand | 已实现 |
| ConsumeMessageCommand | 已实现 |
| CheckMsgSendRTCommand | 已实现 |
| DumpCompactionLogCommand | 已实现 |

### Connection / Producer（3）

| 命令类 | 状态 |
|---|---|
| ProducerConnectionSubCommand | 已实现 |
| ConsumerConnectionSubCommand | 已实现 |
| ProducerSubCommand | 已实现 |

### Cluster（2）

| 命令类 | 状态 |
|---|---|
| ClusterListSubCommand | 已实现 |
| CLusterSendMsgRTCommand | 已实现 |

### NameServer / KV（6）

| 命令类 | 状态 |
|---|---|
| UpdateKvConfigCommand | 已实现 |
| DeleteKvConfigCommand | 已实现 |
| WipeWritePermSubCommand | 已实现 |
| AddWritePermSubCommand | 已实现 |
| GetNamesrvConfigCommand | 已实现 |
| UpdateNamesrvConfigCommand | 已实现 |

### Queue / RocksDB（2）

| 命令类 | 状态 |
|---|---|
| QueryConsumeQueueCommand | 已实现 |
| CheckRocksdbCqWriteProgressCommand | 已实现 |

### Export / Metadata（6）

| 命令类 | 状态 |
|---|---|
| ExportMetadataCommand | 已实现 |
| ExportConfigsCommand | 已实现 |
| ExportMetricsCommand | 已实现 |
| ExportMetadataInRocksDBCommand | 已实现 |
| ExportPopRecordCommand | 已实现 |
| RocksDBConfigToJsonCommand | 已实现 |

### Controller（5）

| 命令类 | 状态 |
|---|---|
| GetControllerMetaDataSubCommand | 已实现 |
| GetControllerConfigSubCommand | 已实现 |
| UpdateControllerConfigSubCommand | 已实现 |
| ReElectMasterSubCommand | 已实现 |
| CleanControllerBrokerMetaSubCommand | 已实现 |

### Auth - User（6）

| 命令类 | 状态 |
|---|---|
| CreateUserSubCommand | 已实现 |
| UpdateUserSubCommand | 已实现 |
| DeleteUserSubCommand | 已实现 |
| GetUserSubCommand | 已实现 |
| ListUserSubCommand | 已实现 |
| CopyUsersSubCommand | 已实现 |

### Auth - ACL（6）

| 命令类 | 状态 |
|---|---|
| CreateAclSubCommand | 已实现 |
| UpdateAclSubCommand | 已实现 |
| DeleteAclSubCommand | 已实现 |
| GetAclSubCommand | 已实现 |
| ListAclSubCommand | 已实现 |
| CopyAclsSubCommand | 已实现 |

### Lite Topic（6）

| 命令类 | 状态 |
|---|---|
| GetBrokerLiteInfoSubCommand | 已实现 |
| GetParentTopicInfoSubCommand | 已实现 |
| GetLiteTopicInfoSubCommand | 已实现 |
| GetLiteClientInfoSubCommand | 已实现 |
| GetLiteGroupInfoSubCommand | 已实现 |
| TriggerLiteDispatchSubCommand | 已实现 |

### Stats（1）

| 命令类 | 状态 |
|---|---|
| StatsAllSubCommand | 已实现 |

## 汇总

- 总命令数：96
- 已实现：96
- 未实现：0

## 备注

- 本阶段聚焦能力层 API，不包含 Java mqadmin CLI 参数与输出格式的 1:1 复制。
- 某些命令在 Java 端存在更复杂的并发、回退与兼容分支；当前实现采用直接 remoting 调用的简化路径。
- 当前实测环境（`/opt/rocketmq/bin/mqadmin`）命令名为 `copyUser`/`copyAcl`（单数），且未暴露 Lite Topic 子命令；Lite 能力主要通过 Go 侧 remoting 与单元测试校验。
- 真实环境对照已覆盖 Message 关键命令 `queryMsgByKey`、`queryMsgById`（通过 Go 发送真实消息后进行 Go/Java 双端查询校验）。
- 真实环境对照已新增覆盖：`resetMasterFlushOffset`、`cleanExpiredCQ`、`deleteExpiredCommitLog`、`cleanUnusedTopic`、`checkRocksdbCqWriteProgress`、`rocksDBConfigToJson`、`exportMetadata`、`exportConfigs`、`exportMetrics`、`exportPopRecord`、`sendMsgStatus`、`queryMsgByUniqueKey`、`queryMsgByOffset`、`producerConnection`、`consumerConnection`。
- `producerConnection`/`consumerConnection` 在无在线实例时，Go 与 Java 可能同时返回 not found/not online/not exist；测试按双端一致失败语义判定通过。
- 当前集群的 Java CLI 不存在 `reElectMaster`、`cleanControllerBrokerMeta` 子命令，因此 Controller 相关仍通过 Go 侧实现 + 单元测试验证，请在支持该 CLI 子命令的环境补最终双向实测。
