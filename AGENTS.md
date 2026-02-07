# AGENTS

规则说明
- AGENTS.md 使用中文描述：本文件内的所有描述应以中文撰写，以确保团队内外的一致性与易读性。
- 如需要包含代码段或命令，请在保持原始语义的前提下，附上中文解释，必要时使用中文注释。

概览
- 本仓库是一个 Java Maven 项目，用于 Hadoop/YARN 圈的实验场景。构建、测试、lint 命令在不同环境中应保持稳定。
- 指南强调在 CI 或本地自动化中的可重复性、确定性；遇到不确定处，请优先选择明确的命令和步骤，以确保可复现性。
- 如需要，后续可添加更多约束，如静态分析、依赖审查等。

构建 / lint / 测试 命令
- 构建（编译并打包）:
  mvn clean package
- 构建并跳过测试（快速迭代）:
  mvn -DskipTests clean package
- 运行全部测试:
  mvn test
- 运行单个测试类（JUnit 3/4 风格）:
  mvn -Dtest=SomeTest test
- 运行单个测试方法:
  mvn -Dtest=SomeTest#testMethod test
- 运行子集测试（不打包）:
  mvn -Dtest=SomeTest#testMethod test
- 快速检查代码风格（Checkstyle）:
  mvn checkstyle:checkstyle 或 mvn -Dcheckstyle.consoleOutput=true checkstyle:check
- 使用 Maven Wrapper（若存在）:
  ./mvnw clean package 或 mvnw.bat clean package

运行单测的推荐模式
- 仅运行某个方法:
  mvn -Dtest=com.example.SomeTest#testSpecificBehavior test
- 仅运行某个类:
  mvn -Dtest=com.example.SomeTest test
- Surefire：始终使用显式的测试目标，避免执行无关测试。
- 在 CI 场景中，固定具体测试减少回归风险。

代码风格指南（Java）
- 规则概览：一致性、可读性与可维护性优先。
- 缩进：4 空格，禁止使用制表符。
- 行长：建议不超过约 120 字符，必要时换行。
- 大括号：K&R 风格，左花括号与语句同行，右花括号单独一行。
- Imports：先 java.*，再第三方，最后内部包；分组并排序；禁止未使用导入。
- 包结构：遵循 Maven 的标准布局，顶级包应唯一且清晰。
- 命名：类名 PascalCase；方法名 lowerCamelCase；常量 UPPER_SNAKE_CASE。
- 方法设计：短小、聚焦单一职责；一个类内方法数量适中。
- 可见性：最小暴露原则，公共接口应具备清晰契约。
- 异常处理：尽量捕获具体异常，避免吞掉上下文；必要时封装并抛出具备上下文的异常。
- 日志：统一使用 SLF4J，避免 System.out.println。
- 泛型：避免原始类型，尽量使用泛型提高类型安全。
- 空值处理：参数校验、使用 Optional提升表达力，必要时抛出有意义的异常。
- 线程与并发：使用固定大小的线程池，避免死锁，确保关闭。
- 资源管理：资源要在 finally/try-with-resources 中关闭。
- 测试设计：测试快速、可重复、独立、断言清晰。
- 文档：必要处写 Javadoc，避免冗余注释。
- 性能与优化：先注重清晰性，再进行必要的优化；如需优化请先进行基准测试。
- 可访问性：代码结构应便于新成员理解和贡献。

导入、格式、命名（示例）
- 导入排序示例：
```java
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.*; // 示例
import org.slf4j.Logger;
```
- 命名要表达意图，例如 useTotalBytes 而非 ctb。
- 方法长度：若超过约 200 行，应分解为私有方法。
- 注释：解释为什么而非仅描述做了什么。

错误处理与可观测性
- 抛出带上下文的异常，避免信息丢失。
- 日志中包含关键标识、状态、错误码等上下文。
- 不暴露敏感信息，必要时采用安全日志策略。

并发与同步
- 使用固定大小的线程池，优雅关闭。
- 使用并发集合或同步块保护共享状态。
- 避免阻塞在事件循环线程，必要时改为异步设计。

测试策略
- 测试应快速、可重复且独立于外部系统。
- 测试用例命名要清晰，断言应明确表达期望。
- 优先单元测试，减少对外部依赖。
- 数据准备和清理要简单、可重复。
- 多线程场景要覆盖并发安全性。

仓库与 CI 健康
- 提交信息应解释原因而不仅仅描述内容。
- 不提交密钥/凭据等敏感信息。
- PR 应聚焦单一逻辑变更，便于评审。
- 本地执行测试，确保在干净环境中也能通过。

环境与工具
- Java 版本要与 pom.xml 对应。
- Maven Wrapper 优先使用，如有则直接使用 ./mvnw。
- Git 提交历史要干净，信息可读。
- IDE 配置要统一格式化规则。

Cursor 规则
- Cursor 规则目录未发现（.cursor/rules/ 或 .cursorrules），如后续出现将合并进来。

Copilot 规则
- Copilot 指引文件未发现（.github/copilot-instructions.md），如未来存在将纳入本文件。

变更记录与演进
- 版本变更应简述为何修改，避免只描述变更内容。
- 如有重大改动，优先附上回滚与兼容性说明。

联系和演进路径
- 如需改进本 AGENTS.md，请提交 issue 以说明原因与背景。
- 文档随工具链变化进行定期更新。

RM/NM 压力仿真场景（扩展）
 - 场景目标：在高并发、资源紧张、节点故障等条件下，测试 RM 的调度、资源分配、心跳与容错策略，以及 NM 的承载能力和自愈能力。
 - 场景配置：通过 FakeJob、YarnFakeNodeManager、SLSConfig 等组件组合实现；通过修改 fake.properites、core-site.xml、hdfs-site.xml、yarn-site.xml 等实现压力等级的切换。
 - 场景设计示例：
   1) 基线场景：中等并发、适量 NM，资源充足，观察基线吞吐与响应时间；
   2) 资源紧张场景：增大作业数、降低 NM 容量，观察等待队列长度与调度延迟；
   3) 节点故障场景：部分 NM 停止心跳，评估 RM 的故障转移及任务重调度时序；
   4) 突发高优先级场景：触发抢占策略，评估响应时间与正确性；
 - 指标与观测：提交吞吐、平均/最大延迟、心跳延迟、丢失率、资源利用率、成功/失败比例、节点恢复时间、日志中的关键事件计数；
 - 实现方式与扩展：可在 SLSRunner/SLSNodeManager 的入口添加压力测试入口函数 PressureTestMain，支持 -DpressureMode、-Diterations、-Dconcurrency 等参数；
 - 运行注意：在仿真环境中执行，完成后清理数据与状态，确保可重复性；
