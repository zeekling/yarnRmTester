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

