from __future__ import annotations

from typing import Any


def default_provider_profiles() -> list[dict[str, Any]]:
    return [
        {
            "builtin_ref": "mock_local",
            "name": "本地 Mock",
            "provider_type": "mock",
            "description": "本地验证与演示默认提供方。",
            "config": {
                "model": "mock-model",
                "models": [
                    {
                        "name": "mock-model",
                        "model_type": "chat",
                        "context_window": 8192,
                    }
                ],
                "base_url": "mock://local",
                "backend": "mock",
                "temperature": 0.2,
            },
            "secret": {},
        }
    ]


def default_plugins() -> list[dict[str, Any]]:
    return [
        {
            "key": "research_kit",
            "name": "研究工作台",
            "version": "v1",
            "plugin_type": "toolset",
            "description": "搜索、资料归纳、需求调研。",
            "manifest": {
                "workbench_key": "research",
                "tools": ["search", "docs"],
                "permissions": ["readonly", "network_limited"],
                "description": "研究与资料采集工具集。",
            },
        },
        {
            "key": "codebase_kit",
            "name": "代码库工作台",
            "version": "v1",
            "plugin_type": "toolset",
            "description": "代码检索、差异查看、结构梳理。",
            "manifest": {
                "workbench_key": "codebase",
                "tools": ["files", "git", "diff"],
                "permissions": ["readonly"],
                "description": "代码库检索与审阅工具集。",
            },
        },
        {
            "key": "terminal_kit",
            "name": "执行工作台",
            "version": "v1",
            "plugin_type": "toolset",
            "description": "终端执行与工作区文件操作。",
            "manifest": {
                "workbench_key": "terminal",
                "tools": ["terminal", "files"],
                "permissions": ["workspace_write", "terminal_safe"],
                "description": "代码执行、文件修改与命令行操作。",
            },
        },
        {
            "key": "qa_kit",
            "name": "质检工作台",
            "version": "v1",
            "plugin_type": "toolset",
            "description": "测试、报告、验收审查。",
            "manifest": {
                "workbench_key": "qa",
                "tools": ["tests", "reports"],
                "permissions": ["readonly"],
                "description": "测试验证与质检审阅工具集。",
            },
        },
    ]


def default_agent_templates() -> list[dict[str, Any]]:
    return [
        {
            "builtin_ref": "strategy_planner",
            "name": "策略规划师",
            "role": "planner",
            "description": "负责拆解目标、里程碑和阶段性交付。",
            "spec": {
                "goal": "将复杂任务拆为可执行阶段、约束和验收里程碑。",
                "instructions": "优先输出阶段计划、依赖、风险和可验证交付物。",
                "provider_ref": "mock_local",
                "model": "mock-plan",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["research_kit"],
                "skills": ["任务拆解", "交付规划", "风险识别"],
                "delegation_mode": "none",
                "metadata": {"department": "planning"},
            },
        },
        {
            "builtin_ref": "solution_architect",
            "name": "方案架构师",
            "role": "architect",
            "description": "负责边界、模块划分和接口设计。",
            "spec": {
                "goal": "基于目标拆解产出架构边界、接口和关键约束。",
                "instructions": "聚焦模块边界、数据流、依赖和技术选型。",
                "provider_ref": "mock_local",
                "model": "mock-arch",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["codebase_kit"],
                "skills": ["架构设计", "接口定义", "边界划分"],
                "delegation_mode": "none",
                "metadata": {"department": "architecture"},
            },
        },
        {
            "builtin_ref": "implementation_engineer",
            "name": "实现工程师",
            "role": "developer",
            "description": "负责实现计划、执行步骤与验证清单。",
            "spec": {
                "goal": "输出实现顺序、文件边界、验证步骤和返工入口。",
                "instructions": "以交付落地为目标，约束执行顺序和验证方式。",
                "provider_ref": "mock_local",
                "model": "mock-dev",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["terminal_kit"],
                "skills": ["实现规划", "执行编排", "验证设计"],
                "delegation_mode": "none",
                "metadata": {"department": "engineering"},
            },
        },
        {
            "builtin_ref": "quality_reviewer",
            "name": "质量审查员",
            "role": "reviewer",
            "description": "负责验收判断和返工决策。",
            "spec": {
                "goal": "根据验收条件审查当前交付是否可通过。",
                "instructions": "明确指出缺口、风险、返工方向和通过依据。",
                "provider_ref": "mock_local",
                "model": "mock-review",
                "temperature": 0.1,
                "memory_policy": "agent_private",
                "plugin_refs": ["qa_kit"],
                "skills": ["审查", "验收", "返工建议"],
                "delegation_mode": "none",
                "metadata": {"department": "qa"},
            },
        },
        {
            "builtin_ref": "proposal_planner",
            "name": "审批策划",
            "role": "planner",
            "description": "负责产出可供审批的执行提案。",
            "spec": {
                "goal": "生成清晰、可审批的任务提案和理由。",
                "instructions": "输出可供人工审核的方案摘要和执行理由。",
                "provider_ref": "mock_local",
                "model": "mock-plan",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["research_kit"],
                "skills": ["提案", "摘要", "审批说明"],
                "delegation_mode": "none",
                "metadata": {"department": "approval"},
            },
        },
        {
            "builtin_ref": "topic_researcher",
            "name": "主题研究员",
            "role": "researcher",
            "description": "负责机会、范围和用户价值调研。",
            "spec": {
                "goal": "调研主题价值、范围和影响。",
                "instructions": "聚焦目标对象、问题空间和结果价值。",
                "provider_ref": "mock_local",
                "model": "mock-research",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["research_kit"],
                "skills": ["研究", "情报收集", "价值分析"],
                "delegation_mode": "none",
                "metadata": {"department": "research"},
            },
        },
        {
            "builtin_ref": "risk_specialist",
            "name": "风险分析员",
            "role": "analyst",
            "description": "负责约束、依赖和失败模式分析。",
            "spec": {
                "goal": "识别关键约束、风险和失败模式。",
                "instructions": "明确指出依赖、风险和需要预案的点。",
                "provider_ref": "mock_local",
                "model": "mock-risk",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["research_kit"],
                "skills": ["风险识别", "依赖梳理", "失败分析"],
                "delegation_mode": "none",
                "metadata": {"department": "analysis"},
            },
        },
        {
            "builtin_ref": "synthesis_lead",
            "name": "综合汇总者",
            "role": "synthesizer",
            "description": "负责合并并行分支结果。",
            "spec": {
                "goal": "汇总分支结论，形成单一建议。",
                "instructions": "解决分支冲突并输出统一建议。",
                "provider_ref": "mock_local",
                "model": "mock-synth",
                "temperature": 0.2,
                "memory_policy": "agent_private",
                "plugin_refs": ["research_kit", "qa_kit"],
                "skills": ["总结", "综合", "决策建议"],
                "delegation_mode": "none",
                "metadata": {"department": "synthesis"},
            },
        },
    ]


def default_team_templates() -> list[dict[str, Any]]:
    return [
        {
            "builtin_ref": "software_delivery_team",
            "name": "软件交付团队",
            "description": "围绕规划、架构、实现、审查的标准交付团队。",
            "spec": {
                "workspace_id": "local-workspace",
                "project_id": "default-project",
                "agents": [
                    {"key": "planner", "name": "规划负责人", "agent_template_ref": "strategy_planner"},
                    {"key": "architect", "name": "架构负责人", "agent_template_ref": "solution_architect"},
                    {"key": "developer", "name": "交付工程师", "agent_template_ref": "implementation_engineer"},
                    {"key": "reviewer", "name": "质量关卡", "agent_template_ref": "quality_reviewer"},
                ],
                "flow": {
                    "nodes": [
                        {"id": "start", "type": "start"},
                        {"id": "plan", "type": "agent", "agent": "planner", "instruction": "拆解目标、里程碑、风险和交付顺序。"},
                        {"id": "design", "type": "agent", "agent": "architect", "instruction": "定义模块边界、接口和关键技术约束。"},
                        {"id": "implement", "type": "agent", "agent": "developer", "instruction": "输出实现任务包、顺序和验证计划。"},
                        {"id": "review", "type": "agent", "agent": "reviewer", "instruction": "判断 review.pass 是否应为 true，并指出剩余风险。"},
                        {"id": "gate", "type": "condition", "expr": "review.pass == true"},
                        {"id": "rework", "type": "loop", "max_iterations": 2},
                        {
                            "id": "artifact",
                            "type": "artifact",
                            "name": "delivery-summary.md",
                            "artifact_kind": "report",
                            "template": "# Delivery Summary\n\n{{review.summary}}\n\n## Implementation Plan\n{{implement.deliverables_text}}\n\n## Risks\n{{review.risks_text}}",
                        },
                        {"id": "end", "type": "end"},
                    ],
                    "edges": [
                        {"from": "start", "to": "plan"},
                        {"from": "plan", "to": "design"},
                        {"from": "design", "to": "implement"},
                        {"from": "implement", "to": "review"},
                        {"from": "review", "to": "gate"},
                        {"from": "gate", "when": "true", "to": "artifact"},
                        {"from": "gate", "when": "false", "to": "rework"},
                        {"from": "rework", "to": "implement"},
                        {"from": "artifact", "to": "end"},
                    ],
                },
                "definition_of_done": ["review.pass == true", "artifact.path != null"],
                "acceptance_checks": ["review.pass == true"],
                "metadata": {"communication_policy": "graph-ancestor-scoped"},
            },
        },
        {
            "builtin_ref": "approval_delivery_team",
            "name": "审批交付团队",
            "description": "带人工审批关卡的提案团队。",
            "spec": {
                "workspace_id": "local-workspace",
                "project_id": "default-project",
                "agents": [{"key": "planner", "name": "审批策划", "agent_template_ref": "proposal_planner"}],
                "flow": {
                    "nodes": [
                        {"id": "start", "type": "start"},
                        {"id": "plan", "type": "agent", "agent": "planner", "instruction": "产出方案摘要和审批理由。"},
                        {"id": "approve", "type": "approval", "name": "人工执行审批"},
                        {"id": "artifact", "type": "artifact", "name": "approval-note.md", "artifact_kind": "report", "template": "# Approved Delivery Note\n\n{{plan.summary}}"},
                        {"id": "end", "type": "end"},
                    ],
                    "edges": [
                        {"from": "start", "to": "plan"},
                        {"from": "plan", "to": "approve"},
                        {"from": "approve", "to": "artifact"},
                        {"from": "artifact", "to": "end"},
                    ],
                },
                "definition_of_done": ["artifact.path != null"],
                "acceptance_checks": [],
                "metadata": {"communication_policy": "graph-ancestor-scoped"},
            },
        },
        {
            "builtin_ref": "research_parallel_team",
            "name": "并行研究团队",
            "description": "并行研究后合并综合的多 Agent 团队。",
            "spec": {
                "workspace_id": "local-workspace",
                "project_id": "default-project",
                "agents": [
                    {"key": "researcher", "name": "主题研究分支", "agent_template_ref": "topic_researcher"},
                    {"key": "risk_analyst", "name": "风险研究分支", "agent_template_ref": "risk_specialist"},
                    {"key": "synthesizer", "name": "综合汇总", "agent_template_ref": "synthesis_lead"},
                ],
                "flow": {
                    "nodes": [
                        {"id": "start", "type": "start"},
                        {"id": "parallel", "type": "parallel"},
                        {"id": "topic_research", "type": "agent", "agent": "researcher", "instruction": "研究主题价值、范围和预期影响。"},
                        {"id": "risk_research", "type": "agent", "agent": "risk_analyst", "instruction": "研究风险、约束和关键依赖。"},
                        {"id": "merge", "type": "merge"},
                        {"id": "synthesize", "type": "agent", "agent": "synthesizer", "instruction": "合并分支结果并输出统一建议。"},
                        {"id": "end", "type": "end"},
                    ],
                    "edges": [
                        {"from": "start", "to": "parallel"},
                        {"from": "parallel", "to": "topic_research"},
                        {"from": "parallel", "to": "risk_research"},
                        {"from": "topic_research", "to": "merge"},
                        {"from": "risk_research", "to": "merge"},
                        {"from": "merge", "to": "synthesize"},
                        {"from": "synthesize", "to": "end"},
                    ],
                },
                "definition_of_done": [],
                "acceptance_checks": [],
                "metadata": {"communication_policy": "graph-ancestor-scoped"},
            },
        },
    ]
