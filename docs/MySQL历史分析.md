# 可选 MySQL 历史分析

适用于 `0.3.0` 及以上版本。原 `redis-mq-dashboard` 仍只需要 Redis；需要历史分析时增加独立扩展，不替换业务数据源，也不参与 MQ 的投递事务。

## 引入与开启

在宿主应用中从 Maven Central 引入：

```xml
<dependency>
  <groupId>io.github.linbin-mk</groupId>
  <artifactId>redis-mq-dashboard-mysql</artifactId>
  <version>0.3.0</version>
</dependency>
```

它包含原看板、starter、core、HikariCP 和 MySQL Connector/J，不需要额外引入 JDBC starter 或 ORM。保留宿主原来的 `RedissonClient`。通过宿主已有配置来源提供：

```yaml
redis-mq:
  dashboard:
    enabled: true
    username: monitor
    password: ${MQ_DASHBOARD_PASSWORD}
    persistence:
      enabled: true
      source-id: daily-prod-redis
      retention: 5d
      sample-interval: 1m
      flush-interval: 10s
      cleanup-interval: 1m
      lease-duration: 30s
      max-queues: 128
      pending-minutes: 3
      cleanup-batch-size: 1000
      datasource:
        url: ${MQ_MONITOR_DB_URL}
        username: ${MQ_MONITOR_DB_USERNAME}
        password: ${MQ_MONITOR_DB_PASSWORD}
        maximum-pool-size: 2
```

`source-id` 标识 Redis 部署，应稳定且显式配置；同一 MySQL 中不同 Redis 部署不能复用同一 source-id 与 namespace 组合。分析范围由两者共同隔离，队列名区分大小写。

默认使用独立连接池，内部资源不注册成业务 `DataSource` Bean，不接管业务事务。连接池默认最多两个连接，最少空闲连接为零，借用超时 500 ms，独立驱动连接超时 1 秒、读写超时 2 秒，SQL 超时 1 秒。JDBC URL 中的 connectTimeout/socketTimeout 不能关闭或提高这些上限。接口历史查询最多并发一个，写入使用另一个后台线程。

需要复用宿主数据源时，只设置 `datasource.bean-name: yourDataSourceBean`，不要同时设置 URL。扩展不关闭或重配置借用的数据源；该模式的网络和借用超时由宿主管理，停止时不额外借用宿主连接，采集者由租约到期后接管。配置在启动时校验并冻结，改变后需重启；同一监控源各实例的保留时间、采样间隔、租约和队列范围必须一致，配置不一致时分析降级，MQ 不受影响。

如需只采集指定队列，配置 `persistence.queues: [task-execution, notification]`；否则从已登记目录读取至多 `max-queues` 个。超过上限时状态接口会报告未采样队列数，指标缓冲也受队列数限制，不能将未监控队列理解为零流量。

## 初始化数据库

由部署方显式执行 [V1__monitor_history.sql](../redis-mq-dashboard-mysql/src/main/resources/com/mk/mq/dashboard/mysql/V1__monitor_history.sql)，该文件也打包在扩展 JAR 的 `com/mk/mq/dashboard/mysql/` 下。运行时不自动建表、不修改业务表、不执行 OPTIMIZE，也不使用 MySQL Event Scheduler。

三张表为：

| 表 | 用途 |
|---|---|
| rmq_monitor_lease | 监控源、配置摘要、采集实例及有到期时间的租约令牌 |
| rmq_monitor_snapshot | 每队列按采样间隔保存的状态、数量、正文总字节数及防重记录数 |
| rmq_monitor_metrics | 每实例、每队列、每分钟的累计计数和固定耗时直方图 |

初始化账号需要建表权限；运行账号只需要以上三张表的 SELECT、INSERT、UPDATE、DELETE 权限。建议使用独立分析库。数据源密码、JDBC URL 等不会进入历史查询响应，不保存消息正文、领取凭证、逐条业务日志或失败原因。

## 指标口径

- `sent`：观察到发送脚本成功接受的新消息数，重复请求记入 `duplicate`。
- `received`：观察到领取成功的投递次数，包含同一消息的再次投递。
- `acknowledged`：观察到有效凭证确认成功的次数，不能当作去重后的业务成功数。
- `retried`：失败操作接受后重新调度的次数，含 Lua 内处理权超时恢复；进入死信的终结操作不再算一次重试调度。
- `dead`、`expired`：观察到的进入死信次数、处理权超时恢复次数，不是当前库存；当前数量看队列快照。
- `handler_success`、`handler_failure`：组件管理的业务回调正常返回或抛出异常的次数。ACK 网络异常不会伪装成业务回调异常。
- 处理耗时包括反序列化及业务回调，不包括 ACK/重试 RPC；低层手动领取的外部处理耗时无法自动统计。
- 领取延迟从可执行时间算到 Redis 实际领取时间，不把主动延迟或重试退避算成排队延迟。
- P95 使用固定直方图合并估算，不平均不同实例的 P95。合并窗口的积压图显示各项数量峰值，各状态峰值未必同时发生。

没有采集到的窗口保留为缺口，不绘制为零。当前尚未结束的分钟以及进程中断后未完成的窗口会标记为不完整。统计仅用于分析，不是可靠审计；Redis 超时导致调用结果不确定、进程崩溃、达到容量上限或长时间数据库故障均可能产生缺口。

## 容量、故障与回收

MQ 线程只做有界内存累计，不执行 SQL、不写 Redis 监控日志、不保存逐条事件。每个实例默认保留当前分钟及前两分钟，最多 `max-queues × pending-minutes` 个聚合窗口。数据库异常时按间隔退避，最长 30 秒；过老窗口丢弃并增加 `droppedEvents`。它统计丢弃的指标事件，不是丢失的 MQ 消息数量。

快照与回收通过 MySQL 租约选择一个采集实例；写入会验证租约及令牌，旧实例不能在失去处理权后覆盖新采集者。各实例指标独立累计，唯一键与递增序号使重复或迟到写入不重复累加。数据库故障不进入 MQ 就绪检查，不停止消费者；状态接口仍可读取降级原因。

`retention: 5d` 是滚动 120 小时，按数据库时间计算。查询立即排除窗口外数据；后台按监控源及时间索引分批删除，每批最多 1000 行，单轮约 2 秒预算。过期积压时继续限速清理并报告 `CLEANUP_BEHIND`。缩短保留时间后分批回收，延长时间不能恢复已删除数据。清理先于 Redis 采样执行，Redis 不可用时只要 MySQL 仍可用就继续回收。自动清理需要至少一个该监控源的实例运行；永久停用或更换 source-id 时，应交接旧范围的清理任务。

MySQL 删除记录后主要释放表内可复用空间，不承诺 `.ibd` 立即缩小。分析清理不删除 Redis 中的待执行、处理中或死信数据。健康运行时，同一队列保留 5 天的分钟快照最多约 7200 条，指标上限还要乘实际活跃实例数；请按实例数、队列数和保留窗口规划数据库容量。

历史接口最多返回 1000 个点，自动按范围降采样；聚合前先用覆盖索引检查每张表最多十万条记录，超过时要求缩短范围，不能只用返回 LIMIT 掩盖大扫描。所有查询都带监控源、队列与时间范围，不使用无限 OFFSET 翻页。

## 页面与接口

原实时查询继续只读。新增同一 Basic Auth 保护下的 GET 接口：

- `/redis-mq/dashboard/api/history/status`：开关、连接状态、最近成功时间、内存窗口、丢弃计数及清理状态。
- `/redis-mq/dashboard/api/queues/{queue}/history?from=...&to=...&points=300`：Unix 毫秒时间范围内的历史，起点按分钟窗口对齐并受保留边界约束，返回实际有效范围、分辨率、峰值、累计计数及耗时摘要。

未引入扩展时状态为 DISABLED，历史查询返回不可用；开启但数据库异常时历史降级，实时数据保持独立。历史图支持 1 小时、24 小时、5 天、自定义范围及积压/速率/P95 切换，刷新页面后仍可查看 MySQL 中的历史。

## 本机 Docker E2E

使用独立 Redis 和 MySQL 8.0+，测试库名固定为 `rmq_history_test`，不要指向业务实例。凭据通过环境变量传入，不写入源码或命令行参数：

```bash
# MYSQL_USER / MYSQL_PASSWORD 使用专用测试账号，与容器初始化配置保持一致。
export MYSQL_USER=rmq_history
export MYSQL_PASSWORD='<专用测试口令>'
export MQ_TEST_MYSQL_URL='jdbc:mysql://127.0.0.1:13316/rmq_history_test?useSSL=false&allowPublicKeyRetrieval=true'
mvn clean verify -Dmq.redis=redis://127.0.0.1:16379 "-Dmq.mysql=$MQ_TEST_MYSQL_URL"
python3 scripts/start-history-e2e.py --mysql-url "$MQ_TEST_MYSQL_URL"
```

最后一条启动只监听 127.0.0.1:18083 的测试宿主，含有界合成流量及合成历史展示数据；不进入发布 JAR。浏览器打开 `http://127.0.0.1:18083/redis-mq/dashboard/`，测试账号 monitor，专用本机测试口令为 `local-e2e-monitor-2026`，不能用于真实部署。

另开终端，指定本轮专用 MySQL 容器：

```bash
python3 scripts/history-e2e.py --mysql-container redis-mq-history-mysql --output /tmp/history-e2e.json
```

脚本只接受本机地址、本机 Docker 和带专用数据库标记的测试容器，实际停止/恢复 MySQL，验证数据库停机时 MQ 继续消费、历史返回 503、恢复后重新写入及保留窗口/点数限制。测试的消息流转、数据库停机和恢复是真实操作；旧时间段的页面展示数据明确为合成，不能当作真实负载测量。
