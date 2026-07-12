# AGENTS

规则说明
- AGENTS.md 使用中文描述。
- 如包含代码段或命令，在保持原始语义的前提下附上中文解释。

## 项目本质

单模块 Maven 项目，用于构造海量 Fake NM 节点对真实 YARN RM 进行压力测试。核心思想：Fake NM 不做真正的容器启动，Fake AM 只是线程池管理的对象，所有操作都是 RPC 级别的模拟。

## 构建

```bash
# 编译并打包（会自动复制依赖到 target/lib）
mvn clean package

# 跳过测试快速迭代
mvn -DskipTests clean package

# 仅编译
mvn compile

# 打包时不带 site 配置文件（core-site.xml / hdfs-site.xml / yarn-site.xml / fake.properites 会被排除）
```

- **pom.xml 声明 Java 21 + `--enable-preview`**，但 CI 用 JDK 17（`.github/workflows/maven.yml`）。本地开发请用 JDK 21。
- 不存在 Maven Wrapper，直接用系统 `mvn`。
- **不存在 Checkstyle / PMD / SpotBugs 等静态检查插件**，`mvn checkstyle:check` 不可用。
- 依赖通过 `maven-dependency-plugin` 在 package 阶段复制到 `target/lib`。

## 测试

- **JUnit 3.8.1** + Mockito 4.11.0
- JUnit 3 风格：**没有 `@Test` 注解**，测试类不需要 `extends TestCase`，方法名以 `test` 开头即可被 Surefire 识别。
- 使用 `junit.framework.Assert`（不是 `org.junit.Assert`）。
- 测试配置文件使用 `src/test/resources/sls-test.properties`（与主配置分离）。
- 集成测试会构造真实的 `YarnFakeNodeManager` 实例并调用 RPC，依赖本地 YARN 配置。
- 单元测试命令：
  ```bash
  mvn -Dtest=org.apache.hadoop.sls.metrics.MetricsDataTest test
  mvn -Dtest=org.apache.hadoop.sls.metrics.MetricsDataTest#testMultipleHeartbeatTimeUpdates test
  ```

## 入口与运行

| 类 | 作用 | 启动方式 |
|---|---|---|
| `SLSNodeManager.main()` | 构造大量 Fake NM 并注册到 RM，启动心跳循环 + Metrics Server | `java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.SLSNodeManager <config_dir>` |
| `SLSRunner.main()` | 提交批量 FakeJob 进行压测 | `java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.SLSRunner <config_dir>` |

两个入口都内嵌了默认配置路径（硬编码为 `src/main/resources`），传命令行参数可覆盖。

## 关键文件

| 文件 | 说明 |
|---|---|
| `src/main/resources/fake.properites` | **注意文件名有 typo：`properites` 而非 `properties`**，代码中硬编码了此文件名，不可改名 |
| `src/main/resources/{core,hdfs,yarn}-site.xml` | 从目标 RM 集群获取，构建时会从 JAR 中排除，需外部提供 |
| `src/test/resources/sls-test.properties` | 测试用独立配置文件 |
| `start-metrics.bat` / `start-metrics.sh` | 引用了 `org.apache.hadoop.sls.SLSMetrics`，**但该类在源码中不存在** |

## 核心组件

- **`SLSConfig`** — 读取 `fake.properites`，提供所有配置项的 getter（NM 数量、端口范围、线程池大小、作业参数、监控配置）。
- **`YarnFakeNodeManager`** — 实现 `ContainerManagementProtocol`，通过 YARN RPC 注册到真实 RM，周期性 heartbeat，管理 Container 状态。**不真正启动进程**。
- **`FakeJob`** — 模拟客户端，通过 `ApplicationClientProtocol` 向 RM 提交应用。
- **`FakeApplication`** — 模拟 AM，向 RM 注册、申请 Container、通过 `ContainerManagementProtocol`"启动"容器。
- **`NodeManagerCommon`** — 持有全局 `Map<NodeId, YarnFakeNodeManager>` 静态变量，所有组件通过它查找 Fake NM。
- **`MetricsServer`** — 内嵌在 `SLSNodeManager` 中的 HTTP 服务（端口默认 28080），暴露 `/metrics` 接口。
- **`HeartbeatResponseCollector`** / **`MetricsData`** — 收集每次心跳的响应数据（成功/失败次数、容器分配/释放数）。
- **`NodeHeartbeatStats`** — 线程安全的心跳延迟统计（计数、总耗时、最大/最小/平均耗时）。
- **`ResourceManagerMetricsCollector`** — 通过 `YarnClient` 从 RM 获取集群指标。

## 包结构

```
org.apache.hadoop.sls           — SLSRunner, SLSNodeManager（入口）
org.apache.hadoop.sls.config    — SLSConfig
org.apache.hadoop.sls.job       — FakeJob, FakeApplication
org.apache.hadoop.sls.nm        — YarnFakeNodeManager, NodeManagerCommon, NMHttpHandler, JobStatUpdater
org.apache.hadoop.sls.metrics   — MetricsServer, MetricsHttpHandler, MetricsData, NodeHeartbeatStats, HeartbeatResponseCollector, ResourceManagerMetricsCollector
org.apache.hadoop.sls.util      — CommonUtils
```

包名使用 `org.apache.hadoop.sls`（与 Hadoop SLS 保持一致），非 `org.example`。

## CI 配置

- GitHub Actions（`.github/workflows/maven.yml`）：push/PR 到 main 触发，JDK 17 + `mvn -B package`。
- **已知不匹配**：CI 用 JDK 17，但 pom.xml 需要 JDK 21 + `--enable-preview`。构建可能因 preview 特性失败。

## 注意事项

- 所有 XMl site 配置文件从 JAR 中排除，运行时必须通过 classpath 或外部目录提供。
- `fake.properites` 文件名的 typo 是故意的——代码和配置文件名保持一致即可。
- `.gitignore` 排除了 `.opencode/` 目录，OpenCode 本地配置不提交。
- 修改线程池大小时注意：`yarn.fake.threadpool.size` 控制心跳线程池，`yarn.fake.job.update.threadpool.size` 控制作业状态更新线程池。

---

## PuaSE 执行流程优化规则

### §A 执行前自检清单（委派前）

在任何 TODO 委派前，执行以下检查：

1. **TODO 状态检查**：当前 TODO 状态必须为 `pending`
   - 若为 `in_progress` → 检查是否有对应委派记录
   - 无委派记录 → 自动修正为 `pending`
2. **依赖检查**：前置 TODO 必须为 `completed` 或 `skipped`
   - 不满足 → 标记当前 TODO 为 `blocked`
3. **Agent 可用性检查**：目标 Agent 必须可用
   - 不可用 → 尝试备用 Agent 或降级自执行
4. **循环委派检测**：同一 Agent 不能在委派链中出现 ≥2 次
   - 检测到循环 → 终止委派并上报
5. **Skill 加载检查**：如计划要求依赖特定 skill 且未加载 → 自动加载

**自检输出格式**：
```
[Pre-Check] TODO #{n} → agent={agent}
  ✓ TODO 状态为 pending
  ✓ 依赖已完成
  ✓ Agent 可用
  ✓ 无循环委派
  ✓ Skill 已加载
  → 开始委派: {agent}
```

### §B 委派后验证机制（委派后）

Agent 委派后 3 秒，执行以下验证：

1. **TODO 状态一致性验证**：
   - TODO 应为 `in_progress` → 若不是，检查 delegation_log
   - 有委派记录但状态不对 → 自动修正
   - 无委派记录 → 重置为 `pending`

2. **Agent 启动验证**：
   - Agent 应在 5 秒内返回初始响应
   - 超时 → 触发重试（最多 3 次）
   - 重试间隔：1s → 2s → 4s（指数退避 + 10% 抖动）

3. **委派上下文验证**：
   - 确认 `delegation_context` 包含 `task_goal`、`expected_outputs`、`todo_ref`
   - 缺失字段 → 记录警告，重试时补充

**验证结果处理**：
| 状态 | 含义 | 处理方式 |
|------|------|---------|
| `active` | 执行正常 | 不干预 |
| `stuck` | 卡住 | 自动重试（最多 3 次） |
| `failed` | 失败 | 标记 TODO 为 `pending`，上报用户 |

### §C TODO 状态一致性规则

**状态转换矩阵**：

| 从 → 到 | 条件 | 禁止条件 |
|---------|------|---------|
| `pending` → `in_progress` | 委派已启动 | 无委派记录 |
| `in_progress` → `completed` | Agent 返回通过验收 | 无验证结果 |
| `in_progress` → `pending` | Agent 打回，附理由 | 无打回记录 |
| `in_progress` → `skipped` | 判定不适用 | 无跳过理由 |
| `in_progress` → `blocked` | 前置依赖失败 | 依赖已完成 |

**自动修正规则**：
1. **虚假 in_progress**：TODO 为 `in_progress` 但无委派记录 → 重置为 `pending`（静默修复）
2. **虚假 completed**：TODO 为 `completed` 但委派失败 → 重置为 `in_progress`（通知用户）
3. **孤儿委派**：有委派记录但 TODO 为 `pending` → 标记委派为 orphaned（静默）
4. **循环委派**：同一 Agent 出现在链中 ≥2 次 → 终止，标记 TODO 为 `blocked`

**委派日志格式**：
```
delegation_log_entry:
  timestamp: "ISO 时间戳"
  todo_ref: int
  agent: "Agent 名称"
  status: "started" | "completed" | "failed" | "orphaned"
  context: { task_goal, expected_outputs }
  result: "Agent 返回摘要"
  retries: int
```

### §D 延迟自动检测机制

**触发条件**：

| 场景 | 触发条件 | 自动行为 |
|------|---------|---------|
| 执行卡住 | 用户选择执行方式后 5s 内未调用 skill | 自动调用对应 skill |
| 状态不一致 | TODO 为 `in_progress` 但无活跃执行 | 自动修正状态 |
| Agent 静默 | 委派后 10s 无 Agent 响应 | 重试（最多 3 次） |
| Agent 失败 | Agent 返回失败结果 | 自动重试后上报 |

**检测循环**：
```
每 5 秒检查一次执行状态（最多 3 次）：
  attempt 1: 5s → 正常 → 关闭检查
  attempt 2: 5s → 异常 → 自动修复
  attempt 3: 5s → 异常 → 上报用户
  总超时: 35s（含缓冲）
```

**自修正优先级**：
1. 优先静默修复（不干扰用户）
2. 修复失败时通知用户
3. 用户决策 > 自动修复（元规则）

### §E 对话效率优化规则

**消息密度控制**：

| 阶段 | 消息密度 | 说明 |
|------|---------|------|
| 设计阶段 | 正常 | 详细说明 + 等待确认 |
| 执行阶段 | 低 | 仅状态更新，自动继续 |
| 故障阶段 | 高 | 暂停执行，请求决策 |

**执行模式**：
1. **立即执行**：用户选择执行方式后，立即调用对应 skill，无需等待"继续"确认
2. **批量汇报**：每完成 3-5 个 Task 汇报一次进度
3. **仅阻塞暂停**：仅在以下情况暂停等待用户决策：
   - Agent 打回 ≥3 次
   - 架构变更需要确认
   - 用户主动要求暂停
4. **自动压缩**：每完成一个 Task，自动压缩相关对话历史

**消息模板**：
```
# 执行进度
PuaSE 进度: ✅ 1/5 | ✅ 2/5 | ▶️ 3/5 | □ 4/5 | □ 5/5

# 阻塞时
[Blocked] Task 3: agent 打回超过阈值
请确认：继续重试 / 跳过 / 修改设计？

# 完成时
✅ 全部任务完成
```
