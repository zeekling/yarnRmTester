# PuaSE 执行流程优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 PuaSE 执行流程的 5 个优化设计落地到 AGENTS.md 规则中，并实际展示优化效果

**Architecture:** 将自检、验证、状态一致性、自动检测、对话效率规则追加到 AGENTS.md；优化效果通过实际对话行为验证

**Tech Stack:** AGENTS.md（规则文件）、PuaSE 协议（元规则）

---

### Task 1: 添加执行前自检规则到 AGENTS.md

**Files:**
- Modify: `AGENTS.md`（PuaSE 协议追加）

- [ ] **Step 1: 读取 AGENTS.md 当前内容**

- [ ] **Step 2: 在 AGENTS.md 中添加执行前自检清单章节**

追加以下内容到 AGENTS.md 的 PuaSE 协议部分：

```markdown
## §A 执行前自检清单（委派前）

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

**自检格式**（输出到对话日志）：
```
[Pre-Check] TODO #{n} → agent={agent}
  ✓ TODO 状态为 pending
  ✓ 依赖已完成
  ✓ Agent 可用
  ✓ 无循环委派
  ✓ Skill 已加载
  → 开始委派: {agent}
```
```

- [ ] **Step 3: 验证修改**

Run: `Select-String -Pattern "§A 执行前自检清单" .\AGENTS.md`

Expected: 找到匹配行

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): 添加执行前自检清单规则"
```

---

### Task 2: 添加委派后验证机制

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: 添加委派后验证章节**

追加到 AGENTS.md（在 §A 之后）：

```markdown
## §B 委派后验证机制（委派后）

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
```

- [ ] **Step 2: 验证修改**

Run: `Select-String -Pattern "§B 委派后验证机制" .\AGENTS.md`

Expected: 找到匹配行

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): 添加委派后验证机制"
```

---

### Task 3: 添加状态一致性规则

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: 添加状态一致性规则**

追加到 AGENTS.md（在 §B 之后）：

```markdown
## §C TODO 状态一致性规则

### 状态转换矩阵

| 从 → 到 | 条件 | 禁止条件 |
|---------|------|---------|
| `pending` → `in_progress` | 委派已启动 | 无委派记录 |
| `in_progress` → `completed` | Agent 返回通过验收 | 无验证结果 |
| `in_progress` → `pending` | Agent 打回，附理由 | 无打回记录 |
| `in_progress` → `skipped` | 判定不适用 | 无跳过理由 |
| `in_progress` → `blocked` | 前置依赖失败 | 依赖已完成 |

### 自动修正规则

1. **虚假 in_progress**：TODO 为 `in_progress` 但无委派记录 → 重置为 `pending`（静默修复）
2. **虚假 completed**：TODO 为 `completed` 但委派失败 → 重置为 `in_progress`（通知用户）
3. **孤儿委派**：有委派记录但 TODO 为 `pending` → 标记委派为 orphaned（静默）
4. **循环委派**：同一 Agent 出现在链中 ≥2 次 → 终止，标记 TODO 为 `blocked`

### 委派日志

```yaml
delegation_log_entry:
  timestamp: "ISO 时间戳"
  todo_ref: int
  agent: "Agent 名称"
  status: "started" | "completed" | "failed" | "orphaned"
  context: { task_goal, expected_outputs }
  result: "Agent 返回摘要"
  retries: int     # 重试次数
```
```

- [ ] **Step 2: 验证修改**

Run: `Select-String -Pattern "§C TODO 状态一致性规则" .\AGENTS.md`

Expected: 找到匹配行

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): 添加 TODO 状态一致性规则"
```

---

### Task 4: 添加延迟自动检测规则

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: 添加自动检测规则**

追加到 AGENTS.md（在 §C 之后）：

```markdown
## §D 延迟自动检测机制

### 触发条件

| 场景 | 触发条件 | 自动行为 |
|------|---------|---------|
| 执行卡住 | 用户选择执行方式后 5s 内未调用 skill | 自动调用对应 skill |
| 状态不一致 | TODO 为 `in_progress` 但无活跃执行 | 自动修正状态 |
| Agent 静默 | 委派后 10s 无 Agent 响应 | 重试（最多 3 次） |
| Agent 失败 | Agent 返回失败结果 | 自动重试后上报 |

### 检测循环

```
每 5 秒检查一次执行状态（最多 3 次）：
  attempt 1: 5s → 正常 → 关闭检查
  attempt 2: 5s → 异常 → 自动修复
  attempt 3: 5s → 异常 → 上报用户
  总超时: 35s（含缓冲）
```

### 自修正优先级

1. 优先静默修复（不干扰用户）
2. 修复失败时通知用户
3. 用户决策 > 自动修复（元规则）
```

- [ ] **Step 2: 验证修改**

Run: `Select-String -Pattern "§D 延迟自动检测机制" .\AGENTS.md`

Expected: 找到匹配行

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): 添加延迟自动检测机制"
```

---

### Task 5: 添加对话效率优化规则

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: 添加对话效率规则**

追加到 AGENTS.md（在 §D 之后）：

```markdown
## §E 对话效率优化规则

### 消息密度控制

| 阶段 | 消息密度 | 说明 |
|------|---------|------|
| 设计阶段 | 正常 | 详细说明 + 等待确认 |
| 执行阶段 | 低 | 仅状态更新，自动继续 |
| 故障阶段 | 高 | 暂停执行，请求决策 |

### 执行模式

1. **立即执行**：用户选择执行方式后，立即调用对应 skill，无需等待"继续"确认
2. **批量汇报**：每完成 3-5 个 Task 汇报一次进度
3. **仅阻塞暂停**：仅在以下情况暂停等待用户决策：
   - Agent 打回 ≥3 次
   - 架构变更需要确认
   - 用户主动要求暂停
4. **自动压缩**：每完成一个 Task，自动压缩相关对话历史

### 消息模板

```
# 执行进度
PuaSE 进度: ✅ 1/5 | ✅ 2/5 | ▶️ 3/5 | □ 4/5 | □ 5/5

# 阻塞时
[Blocked] Task 3: agent 打回超过阈值
请确认：继续重试 / 跳过 / 修改设计？

# 完成时
✅ 全部任务完成 | 耗时: {duration}
```
```

- [ ] **Step 2: 验证修改**

Run: `Select-String -Pattern "§E 对话效率优化规则" .\AGENTS.md`

Expected: 找到匹配行

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): 添加对话效率优化规则"
```

---

### Task 6: 自检与最终验证

**Files:**
- Modify: 无（验证现有修改）

- [ ] **Step 1: 规格覆盖检查**

逐项检查设计文档中的每个需求是否都有对应的 AGENTS.md 规则：

| 设计章节 | AGENTS.md 章节 | 状态 |
|---------|---------------|------|
| 3.1 执行前自检清单 | §A | ✅ |
| 3.2 委派后验证机制 | §B | ✅ |
| 3.4 状态一致性规则 | §C | ✅ |
| 3.3 延迟自动检测逻辑 | §D | ✅ |
| 3.5 对话效率优化 | §E | ✅ |

- [ ] **Step 2: 完整性验证**

Run: `Select-String -Pattern "§[A-F]" .\AGENTS.md`

Expected: 显示 §A 到 §E 共 5 个章节

- [ ] **Step 3: 最终 Commit**

```bash
git add AGENTS.md
git commit -m "docs(puase): PuaSE 执行流程优化规则集完成"
```

---

## 执行检查清单（设计文档 vs 实现计划）

| 设计文档需求 | 实现任务 | 是否覆盖 |
|------------|---------|---------|
| 执行前自检清单 | Task 1 | ✅ |
| 委派后验证机制 | Task 2 | ✅ |
| 状态一致性规则 | Task 3 | ✅ |
| 延迟自动检测逻辑 | Task 4 | ✅ |
| 对话效率优化 | Task 5 | ✅ |
| 自检与验证 | Task 6 | ✅ |

## 执行效果验证

优化完成后，在后续执行中验证以下行为：

```yaml
verification_checklist:
  - [ ] 委派前输出自检日志（[Pre-Check]）
  - [ ] 委派后 3s 内输出验证结果
  - [ ] TODO 状态与实际执行一致
  - [ ] 执行卡住时自动恢复
  - [ ] 仅阻塞时暂停等待用户
  - [ ] 每 3-5 个 Task 汇报一次进度
```
