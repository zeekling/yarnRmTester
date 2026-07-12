# PuaSE 执行流程优化设计

**设计日期**：2026-07-12
**状态**：✅ 已批准
**优化策略**：渐进式优化

---

## 1. 背景与问题分析

### 1.1 暴露的问题

在本会话中，PuaSE 在执行 Web Dashboard 实现计划时出现了以下问题：

| # | 问题 | 描述 | 影响 |
|---|------|------|------|
| 1 | **虚假 in_progress 状态** | TODO 标记为 `in_progress` 但实际未委派 Agent | 用户看到"正在执行"但实际没做任何事 |
| 2 | **Skill 调用缺失** | 用户选择 Subagent-Driven 后，未调用对应的 skill | 执行流程卡住 |
| 3 | **过度解释** | 解释"如何执行"而不是实际执行 | 浪费消息次数和用户时间 |
| 4 | **无延迟检测** | 执行卡住后没有主动发现和恢复机制 | 需要用户主动指出问题 |

### 1.2 根因分析

```
根本原因链：
─────────────────────────────────────────────────────

① PuaSE 协议定义了"委派 → 检查 → 完成"的循环
   但缺少"是否真的开始委派了"的检查

② TODO 状态的转换规则是"软约束"：
   - "标记 in_progress"是主动行为
   - "验证是否已委派"是被动的
   - 没有自动检测机制

③ 执行选项的"选择"和"执行"之间存在间隙：
   - 用户选择 → PuaSE 知道要执行
   - PuaSE 执行 → 但可能因为各种原因未启动
   - 间隙中没有检测点

④ 对话节奏控制缺失：
   - 每个步骤都需要"解释 → 确认 → 执行"
   - 没有"自动执行 → 批量汇报"的模式
```

---

## 2. 优化策略

### 2.1 优化原则

1. **渐进式**：不重写现有协议，仅添加检查点和自动化机制
2. **可回退**：优化内容可逐步启用，出现问题可快速回退
3. **静默修复**：大部分检查自动执行，不干扰用户
4. **用户优先**：用户永远是最终决策者

### 2.2 优化范围

| 维度 | 优化内容 | 优先级 |
|------|---------|--------|
| TODO 状态管理 | 状态一致性检查 + 自动修正 | P0 |
| 执行流程 | 委派前自检 + 委派后验证 | P0 |
| 错误检测 | 延迟自动检测 + 自动恢复 | P1 |
| 对话效率 | 消息压缩 + 批量执行 | P2 |

---

## 3. 详细设计

### 3.1 执行前自检清单（Pre-Check）

#### 3.1.1 检查流程

```
委派前 → executePreCheck(todo) → { pass: bool, issues: string[] }
                          │
                          ├─ pass == true  → 继续执行 ▶️
                          │
                          └─ pass == false → 输出 issues
                                             标记 todo 为 'pending'
                                             暂停委派
```

#### 3.1.2 检查项

```yaml
pre_check:
  # 1. TODO 状态验证
  todo_state_check:
    description: "当前 TODO 状态必须为 'pending'"
    check: "todo.status == 'pending'"
    if_fail: "自动修正为 'pending'，记录警告"

  # 2. 依赖关系验证
  dependency_check:
    description: "所有前置 TODO 必须已完成"
    check: "for dep in todo.dependencies: dep.status == 'completed'"
    if_fail: "标记当前 TODO 为 'blocked'，暂停委派"

  # 3. 目标 Agent 可用性验证
  agent_availability_check:
    description: "委派目标 Agent 必须可用"
    check: "agent.is_available()"
    if_fail: "尝试备用 Agent，如果全部不可用则上报"

  # 4. 循环委派检测
  cycle_check:
    description: "同一 Agent 不能在委派链中出现 2 次"
    check: "for agent in delegation_chain: agent.count() <= 1"
    if_fail: "终止委派，上报用户"

  # 5. Skill 加载检查（如果适用）
  skill_load_check:
    description: "如果计划中指定了 skill，必须已加载"
    check: "if plan.requires_skill: skill.is_loaded()"
    if_fail: "自动加载 skill，继续执行"
```

#### 3.1.3 自检输出格式

```yaml
pre_check_result:
  pass: bool
  todo_ref: int
  issues:
    - severity: "warning" | "error"
      check_name: "todo_state_check"
      message: "TODO 状态为 'in_progress'，预期为 'pending'"
      action: "auto_fix"
      fix_result: "已修正为 'pending'"
  timestamp: "2026-07-12T12:00:00Z"
```

---

### 3.2 委派后验证机制（Post-Check）

#### 3.2.1 验证流程

```
委派 3 秒后 → postCheck(delegationId) → { status, detail }

                │
                ├─ active  → 继续执行，不干预 ▶️
                │
                ├─ stuck   → 自动重试委派（最多 3 次）↻
                │            重试成功 → active
                │            重试失败 → failed
                │
                └─ failed  → 标记 TODO 为 'pending'
                             上报用户 🚨
```

#### 3.2.2 验证项

```yaml
post_check:
  check_delay: 3000ms          # 委派后 3 秒检查
  max_retries: 3               # 最大重试次数
  retry_delay: 1000ms          # 重试间隔

  # 1. TODO 状态一致性
  todo_state_check:
    check: "todo.status == 'in_progress'"
    if_mismatch: "检查 delegation_log 是否有对应记录"
    fix:
      - "有记录但状态不对 → 修正为 'in_progress'"
      - "无记录 → 标记为 'pending'，不重试"

  # 2. Agent 启动验证
  agent_launch_check:
    check: "agent 是否返回了初始响应（或开始执行的信号）"
    timeout: 5000ms
    if_timeout: "标记为 'stuck'，触发重试"

  # 3. 委派上下文验证
  context_check:
    check: "delegation_context 是否包含所有必需字段"
    required_fields:
      - task_goal
      - expected_outputs
      - todo_ref
    if_missing: "记录缺失字段，重试时补充"
```

#### 3.2.3 重试策略

```yaml
retry_strategy:
  max_attempts: 3
  base_delay: 1000ms
  backoff: "exponential"       # 指数退避
  backoff_factor: 2            # 1s → 2s → 4s
  jitter: 0.1                  # ±10% 抖动
  
  attempt_1:
    delay: 1000ms (±100ms)
    成功后: "继续执行"
    
  attempt_2:
    delay: 2000ms (±200ms)
    成功后: "继续执行"
    
  attempt_3:
    delay: 4000ms (±400ms)
    成功后: "继续执行"
    失败后: "标记为 failed，上报用户 🚨"
```

---

### 3.3 延迟自动检测逻辑（Auto-Detection）

#### 3.3.1 触发条件

```yaml
auto_detection_trigger:
  # 场景 1：执行选项提供后 5 秒未响应
  execution_stalled:
    condition: "用户已选择执行方式，但 5 秒内 skill 未调用"
    action: "自动调用对应的 skill"
    
  # 场景 2：TODO 状态与实际执行不一致
  state_mismatch:
    condition: "TODO 标记为 in_progress，但技能未执行"
    action: "自动修正 TODO 状态为 pending"
    
  # 场景 3：委派后 Agent 无响应
  agent_silent:
    condition: "Agent 已委派，但 10 秒内无响应"
    action: "触发重试，重试失败后上报"
    
  # 场景 4：Agent 返回失败
  agent_failure:
    condition: "Agent 返回失败结果"
    action: "自动重试（最多 3 次）"
```

#### 3.3.2 检测循环

```python
def auto_execution_detection_loop():
    """
    延迟自动检测循环
    每 5 秒检查一次执行状态，最多检查 3 次
    """
    check_interval = 5  # 秒
    max_checks = 3
    total_timeout = 35  # 秒（含缓冲）
    
    for attempt in range(1, max_checks + 1):
        sleep(check_interval)
        state = snapshot_execution_state()
        
        if state.execution_started and state.todo_consistent:
            # ✅ 正常执行，关闭检测
            return DetectionResult.OK
        
        if state.execution_started and not state.todo_consistent:
            # ⚠️ TODO 不一致，自动修正
            auto_correct_todo_state()
            continue
        
        if not state.execution_started and state.user_chose:
            # ⚠️ 已选择但未执行
            auto_call_skill()
            continue
        
        if state.execution_started and state.agent_failed:
            # 🚨 Agent 失败，自动重试
            retry_delegation()
            continue
    
    # 所有尝试都失败
    report_to_user(BlockingIssue(
        message="执行自动恢复失败，请手动检查",
        recovery_options=["手动重试", "回退到上一步", "跳过任务"]
    ))
```

#### 3.3.3 执行快照

```yaml
execution_snapshot:
  timestamp: "2026-07-12T12:00:05Z"
  fields:
    user_chose: bool           # 用户是否选择了执行方式
    execution_started: bool    # 是否已启动执行
    todo_consistent: bool      # TODO 状态与实际一致
    agent_active: bool         # Agent 是否活跃
    agent_failed: bool         # Agent 是否返回失败
    pending_tasks: int         # 剩余任务数
    completed_tasks: int       # 已完成任务数
```

---

### 3.4 状态一致性规则（State Consistency）

#### 3.4.1 状态转换矩阵

```
┌─────────────────────────────────────────────────────────────┐
│                  TODO 状态转换矩阵                            │
├─────────────┬──────────────┬─────────────────────────────────┤
│ 从 → 到      │ 转换条件      │ 禁止条件                         │
├─────────────┼──────────────┼─────────────────────────────────┤
│ pending →    │ 委派已启动   │ 无委派记录 → 不可转换              │
│ in_progress  │              │                                 │
├─────────────┼──────────────┼─────────────────────────────────┤
│ in_progress →│ Agent 返回   │ 无验证结果 → 不可转换              │
│ completed    │ 通过验收      │                                 │
├─────────────┼──────────────┼─────────────────────────────────┤
│ in_progress →│ Agent 打回   │ 无打回记录 → 不可转换              │
│ pending      │ 有打回理由    │                                 │
├─────────────┼──────────────┼─────────────────────────────────┤
│ in_progress →│ 用户要求      │ 自动判断 → 不可跳过               │
│ skipped      │ 或判定不适用  │ 必须附跳过理由                    │
├─────────────┼──────────────┼─────────────────────────────────┤
│ in_progress →│ 前置依赖失败  │ 依赖已完成 → 不可阻塞             │
│ blocked      │              │                                 │
└─────────────┴──────────────┴─────────────────────────────────┘
```

#### 3.4.2 自动修正规则

```yaml
auto_correction:
  # 规则 1：虚假 in_progress（无对应委派）
  rule_1:
    detect: "todo.status == 'in_progress' AND delegation_log 中无 todo_ref"
    fix: "重置为 'pending'"
    notify: false
    reason: "可能由中断或错误状态导致"

  # 规则 2：虚假 completed（委派返回失败）
  rule_2:
    detect: "todo.status == 'completed' AND 最后一次委派状态 == 'failed'"
    fix: "重置为 'in_progress'"
    notify: true
    reason: "委派失败但 TODO 标记为完成"

  # 规则 3：孤儿 pending（存在活跃委派但 TODO 为 pending）
  rule_3:
    detect: "todo.status == 'pending' AND delegation_log 中有活跃委派"
    fix: "保持 pending，标记委派为 orphaned"
    notify: true
    reason: "委派记录与 TODO 状态不一致"

  # 规则 4：委派链循环检测
  rule_4:
    detect: "同一 Agent 在委派链中出现 2 次"
    fix: "终止当前委派，标记 TODO 为 blocked"
    notify: true
    reason: "检测到循环委派，可能无限循环"
```

#### 3.4.3 委派日志

```yaml
delegation_log:
  # 格式
  entry:
    timestamp: "ISO 时间戳"
    todo_ref: int
    agent: "Agent 名称"
    status: "started" | "completed" | "failed" | "orphaned"
    context: "上下文物件摘要"
    result: "Agent 返回结果摘要"
    retries: int              # 重试次数
  
  # 生命周期
  started:
    - 委派发起时记录
    - status: "started"
  
  completed:
    - Agent 返回成功时记录
    - status: "completed"
    - 包含 result
  
  failed:
    - Agent 返回失败或超时时记录
    - status: "failed"
    - 包含 error
  
  orphaned:
    - 状态一致性检测发现误配时记录
    - status: "orphaned"
    - 不影响执行
```

---

### 3.5 对话效率优化（Message Efficiency）

#### 3.5.1 消息压缩规则

```yaml
message_efficiency:
  # 规则 1：选择执行方式后立即执行
  immediate_execution:
    condition: "用户选择了执行方式"
    wait_for: "无需等待"       # 不需要用户说"继续"
    action: "立即调用对应 skill"
    
  # 规则 2：执行进度仅汇报状态变更
  progress_reporting:
    batch_min: 3               # 每批至少 3 个任务
    batch_max: 5               # 每批最多 5 个任务
    format: "✅ Task 1/6 | ✅ Task 2/6 | ▶️ Task 3/6 | □ Task 4/6"
    only_report:
      - "批处理完成"
      - "遇到阻塞"
      - "用户主动询问"
      
  # 规则 3：故障时暂停等待用户
  pause_on_fault:
    conditions:
      - "Agent 打回 3 次以上"
      - "架构变更需要确认"
      - "用户主动要求暂停"
    
  # 规则 4：执行后自动压缩上下文
  auto_compress:
    trigger: "每完成一个 Task"
    action: "自动压缩已完成的 Task 相关对话"
    preserve: "TODO 状态、委派日志、检查结果"
```

#### 3.5.2 消息模板

```markdown
# 优化前（详细解释 + 等待确认）
Task 1 已完成。
- 结果：创建了 CSS 文件
- 位置：src/main/resources/static/assets/dashboard.css
接下来开始 Task 2（创建主仪表盘页面）。
您是否要继续？

# 优化后（状态驱动 + 自动执行）
PuaSE 进度: ✅ Task 1 | ✅ Task 2 | ▶️ Task 3 | □ Task 4 | □ Task 5
批处理完成: Task 1-2/5
当前: Task 3 (JS 逻辑实现)
```

#### 3.5.3 执行阶段消息密度

| 阶段 | 消息密度 | 说明 | 示例 |
|------|---------|------|------|
| 设计阶段 | 正常 | 需要用户确认 | "方案 A/B/C，推荐 A" |
| 执行阶段 | 低 | 仅状态更新 | "✅ Task 2/6 完成" |
| 故障阶段 | 高 | 需要用户决策 | "Agent 打回，需要确认" |

---

## 4. 实现计划

### 4.1 实现步骤

| 步骤 | 内容 | 优先级 | 预计耗时 |
|------|------|--------|---------|
| 1 | 实现执行前自检逻辑 | P0 | 30 分钟 |
| 2 | 实现 TODO 状态一致性检查 | P0 | 20 分钟 |
| 3 | 实现委派后验证机制 | P0 | 30 分钟 |
| 4 | 实现延迟自动检测逻辑 | P1 | 20 分钟 |
| 5 | 实现对话效率优化 | P2 | 15 分钟 |
| 6 | 编写测试用例 | P2 | 20 分钟 |
| 7 | 验证和优化 | P2 | 15 分钟 |

### 4.2 修改范围

| 文件 | 修改内容 |
|------|---------|
| `AGENTS.md`（PuaSE 协议部分） | 添加自检规则、状态一致性规则 |
| `PuaSE.md` | 无（PuaSE 协议定义在主 prompt 中） |

### 4.3 回退方案

如果优化导致执行流程异常，可以快速回退到原始行为（移除检查点）。

---

## 5. 附录

### 5.1 关键术语

| 术语 | 定义 |
|------|------|
| **委派记录** | 记录每次委派的 Agent、时间、状态 |
| **孤儿委派** | 有委派记录但 TODO 状态为 pending 的异常状态 |
| **虚假状态** | TODO 状态与实际执行进度不一致 |
| **自动修正** | 检测到状态不一致时自动修复，无需用户干预 |

### 5.2 检查清单

```yaml
completion_checklist:
  # 实现完成后验证
  pre_check_implemented: false
  post_check_implemented: false
  state_consistency_implemented: false
  auto_detection_implemented: false
  message_efficiency_implemented: false
  tests_implemented: false
  verified_working: false
```

---

---

## 6. 优化迭代 v2.0 —— 基于 SLSMetrics 执行复盘的新发现

**设计日期**：2026-07-12
**版本**：v2.0
**触发场景**：SLSMetrics Web Dashboard 全链路执行复盘

### 6.1 背景

SLSMetrics 项目完整执行一次 PuaSE 编排循环（设计 → 开发 → 审查 → 修复 → 验收）。复盘发现：§A-§E 规则全部存在于 AGENTS.md 中，但**落地率为 0%**——没有一条规则被实际执行。同时发现 security-expert 探活机制失效、状态机不完整等问题。

### 6.2 优化一：§A-§E 落地率保证（P0）

**问题**：§A-§E 规则在之前的设计文档和 AGENTS.md 中均有详细定义，但 SLSMetrics 执行中全部未遵守。

**根因**：规则以被动文档形式存在，缺乏主动行为保障。

**解决策略**：不再依赖外部约束加载，改为 PuaSE **内化承诺**。§A-§E 是编排者身份的一部分，不是外部要求。

| § | 规则 | 执行承诺 | 验证方式 |
|---|------|---------|---------|
| §A | 委派前输出 `[Pre-Check]` 格式 | 每次委派前执行 5 项检查并输出日志 | 用户可见的 `[Pre-Check]` 块 |
| §B | 委派后 3s 验证 TODO 一致性 | 每次委派后 3s 内检查状态 | TODO 状态与实际一致 |
| §C | 状态转换门禁 | 禁止无委派记录的 `in_progress` | 无虚假状态 |
| §D | 5s 自动检测循环 | 委托后启动循环（最多 3 次、总 35s） | 卡住时自动恢复 |
| §E-2 | 批量汇报 | 每 3-5 Task 批量汇报而非逐个 | 消息数降低 ≥60% |
| §E-4 | 自动压缩 | 每个 Task 完成后即压缩相关部分 | context 保持在 <50% |

### 6.3 优化二：Agent 探活增强（P0）

**问题**：security-expert 连续 2 次委派返回空（no response），按 §5.2 进入 L2 熔断 → PuaSE 人工代检。当前 §5.5 的探活方式（文件存在性 + 轻量消息发送）无法检测"能响应消息但不能返回审计结果"的故障。

**解决**：三层探活机制

```yaml
health_check_v2:
  levels:
    L1: # 文件存在性检查（快速）
      check: "Agent prompt 文件是否存在且可读"
      cost: ~0s
      pass: "进入 L2"
      fail: "标记不可用，发警告"

    L2: # 消息往返检查（中速）
      probe: "通过 task/delegate 发送轻量消息"
      expected: "Agent 返回有效响应（非空）"
      timeout: 5000ms
      pass: "进入 L3"
      fail: "重试 1 次，再失败 → 标记不可用"

    L3: # 功能性探活（完整）
      description: "对 key subagent（security-expert、quality-inspector），
                    在会话启动时发一条真实的审计任务验证能返回有效审计结果"
      probe_task: "发送简单审计 prompt（如'检查类 X 的 SQL 注入风险'）"
      expected: "返回包含具体审计发现的非空结果"
      timeout: 15000ms
      pass: "Agent 就绪"
      fail: "标记为 degraded，触发降级预案"
```

**降级预案**：
- security-expert 不可用 → 加载 `manual-security-audit-checklist.md`，PuaSE 自查
- quality-inspector 不可用 → 逐项自检（按 QI-ARC→QI-DEV→QI-BIG→QI-SEC→QI-DBA→QI-DOC 顺序）
- 探活结果缓存 300s，避免重复探测

### 6.4 优化三：technical_failure 状态追加（P0）

**问题**：当前状态机只有 `pending → in_progress → completed/pending/skipped/blocked`。但"委派成功但返回空"或"Agent 超时"不属于任何一种现有状态：
- 不是 `pending`（已经委派了）
- 不是 `completed`（没有有效结果）
- 不是 `pending`（不是质量打回）
- 不是 `skipped`（确实尝试执行了）
- 不是 `blocked`（依赖没问题）

将技术故障和质量打回混为一谈导致：security-expert 空返被当做"打回"处理，触发了 L2 升级，而实际上应该先重试。

**解决**：追加 `technical_failure` 状态到状态机

```
新状态转换：
  in_progress ──→ technical_failure  ← 新增
      条件：Agent 返回空/超时/格式错误/不可解析
      处理：自动重试（最多 3 次，指数退避）
            重试成功 → completed
            重试 3 次失败 → L1 升级
            L1 失败（换 Agent）→ L2 熔断
```

**更新状态转换矩阵**：

| 从 → 到 | 条件 | 禁止条件 |
|---------|------|---------|
| `pending` → `in_progress` | 委派已启动 | 无委派记录 |
| `in_progress` → `completed` | Agent 返回通过验收 | 无验证结果 |
| `in_progress` → `pending` | Agent 质量打回，附理由 | 无打回记录 |
| **`in_progress` → `technical_failure`** | **Agent 返回空/超时/格式错误** | **有有效审计结果** |
| `technical_failure` → `in_progress` | **重试启动** | **重试 >3 次** |
| `in_progress` → `skipped` | 判定不适用 | 无跳过理由 |
| `in_progress` → `blocked` | 前置依赖失败 | 依赖已完成 |

**新增自动修正规则**：
```
5. 技术故障自动修正：
   detect: "todo.status == 'in_progress' AND 
            agent 返回结果为空/超时/格式错误"
   fix:    "标记为 technical_failure → 自动重试
            （最多 3 次，1s→2s→4s 指数退避）
            重试成功 → 标记 completed
            均失败 → L1 升级换 Agent"
   notify: "重试中静默，连续失败时通知"
```

**状态转换总图**：

```
                    ┌──────────────────────────────────┐
                    │           pending                │
                    └──────┬───────────────────────────┘
                           │ 委派启动
                           ▼
                    ┌──────────────────────────────────┐
                    │         in_progress              │
                    └──┬──────┬──────────┬─────────────┘
                       │      │          │
            Agent返回  │  Agent打回  │  返回空/超时
           通过验收     │  附理由      │  (技术故障)
                       ▼      ▼          ▼
               ┌──────┐ ┌──────┐ ┌──────────────┐
               │compl.│ │pend. │ │tech_failure  │
               └──────┘ └──────┘ └──────┬───────┘
                                        │ 重试（≤3次）
                                        │ 成功 → completed
                                        │ 全失败 → L1升级
                                        ▼
                                  ┌──────────┐
                                  │ L1 换Agent│
                                  └──────────┘
```

## 7. Anti-Familiarity-Bias Enforcement（防熟悉度偏误强执）

### 7.1 问题复现

2026-07-12 实际执行 JMX 双源采集任务时，PuaSE 在已完整阅读所有相关代码后，
**直接 write 了 JmxMetricsCollector.java**，未做委派、未输出自执行归因宣言。

### 7.2 根因

```
熟悉度偏误："代码已经在脑子里了，委派出去还要重传上下文，太慢"
     ↓
上下文隔离原则被跳过："就写一个文件而已"
     ↓
自执行归因宣言被跳过："写完再补"
     ↓
最终：P8 自检钩子失效
```

### 7.3 强执机制（PuaSE 内化，不依赖文件存储）

在每次调用 write/edit 工具前，PuaSE 必须执行以下三步自检：

```
[Pre-Write Self-Check]
① 检查：当前操作是否涉及写文件？
   → 否 → 通过（搜索/读取类操作豁免）
   → 是 → 进入 ②

② 检查：是否有对应的子 Agent 可承担此工作？
   → 否 → 输出自执行归因宣言后继续
   → 是 → 进入 ③

③ 检查："直接改更快"的想法是否出现了？
   → 否 → 输出自执行归因宣言后继续
   → 是 → 必须委派，禁止自执行

输出格式（任一命中时输出）：
【Pre-Write Self-Check】涉及文件：<路径>
对应子 Agent：<有/无> | 熟悉度偏误风险：<是/否>
裁决：→ □ 委派 / □ 自执行（附归因宣言）
```

### 7.4 违反后果

| 违规情形 | 后果 |
|---------|------|
| 未输出 Pre-Write Self-Check 直接写文件 | P0 违规，需立即中止并在 KPI 卡中标记 ⚠️ |
| 子Agent存在但未委派 | 等同 P0 违规，KPI 卡标记 ❌ |
| 归因宣言已输出但未遵守 | 等同 P0 违规，触发 reflector 复盘 |

---

**文档版本**：v2.1
**最后更新**：2026-07-12（追加 §7 Anti-Familiarity-Bias Enforcement）
