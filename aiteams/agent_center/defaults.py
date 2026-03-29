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
            "key": "memory_core",
            "name": "记忆核心插件",
            "version": "v1",
            "plugin_type": "toolset",
            "description": "短期上下文、长期记忆检索与记忆治理。",
            "manifest": {
                "workbench_key": "memory_core",
                "tools": ["memory.context", "memory.search", "memory.manage", "memory.background_reflection"],
                "permissions": ["memory_read", "memory_write"],
                "description": "Agent 默认挂载的长短期记忆与后台反思工具集。",
            },
        },
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


def _legacy_default_agent_templates_removed() -> list[dict[str, Any]]:
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


def default_skills() -> list[dict[str, Any]]:
    return [
        {
            "key": "planning_skill",
            "name": "任务拆解",
            "description": "将目标拆解为阶段、依赖和里程碑。",
            "version": "v1",
            "spec": {
                "group_refs": [
                    {
                        "key": "planning",
                        "name": "规划设计",
                    }
                ],
                "instructions": [
                    "优先识别目标、约束、里程碑、依赖和风险。",
                    "输出结构化阶段计划和验收标准。",
                ],
                "recommended_plugins": ["memory_core", "research_kit"],
            },
        },
        {
            "key": "architecture_skill",
            "name": "架构设计",
            "description": "定义模块边界、接口和关键约束。",
            "version": "v1",
            "spec": {
                "group_refs": [
                    {
                        "key": "planning",
                        "name": "规划设计",
                    }
                ],
                "instructions": [
                    "说明模块边界、数据流和接口契约。",
                    "明确关键技术选型和取舍。",
                ],
                "recommended_plugins": ["memory_core", "codebase_kit"],
            },
        },
        {
            "key": "delivery_skill",
            "name": "交付执行",
            "description": "规划实施步骤、验证手段和交付边界。",
            "version": "v1",
            "spec": {
                "group_refs": [
                    {
                        "key": "delivery",
                        "name": "交付执行",
                    }
                ],
                "instructions": [
                    "输出实现顺序、文件边界和验证步骤。",
                    "优先保证可以交付和可以回滚。",
                ],
                "recommended_plugins": ["memory_core", "terminal_kit"],
            },
        },
        {
            "key": "review_skill",
            "name": "质量审查",
            "description": "根据验收条件作出通过或返工判断。",
            "version": "v1",
            "spec": {
                "group_refs": [
                    {
                        "key": "quality",
                        "name": "质量保障",
                    }
                ],
                "instructions": [
                    "优先输出发现、风险、是否通过和返工方向。",
                    "结论要和验收条件对应。",
                ],
                "recommended_plugins": ["memory_core", "qa_kit"],
            },
        },
    ]


def default_static_memories() -> list[dict[str, Any]]:
    return [
        {
            "key": "static.planner.role",
            "name": "规划负责人角色规范",
            "description": "负责任务拆解和上层协调。",
            "version": "v1",
            "spec": {
                "system_prompt": (
                    "你是规划负责人，负责拆解任务、规划阶段并协调上下级。\n"
                    "优先明确里程碑、任务拆分和优先级建议。\n"
                    "遇到范围变更、高风险决策或关键审批点时必须升级。"
                ),
            },
        },
        {
            "key": "static.architect.role",
            "name": "架构负责人角色规范",
            "description": "负责边界和技术约束。",
            "version": "v1",
            "spec": {
                "system_prompt": (
                    "你是架构负责人，负责模块边界、接口设计和技术约束。\n"
                    "聚焦接口、模块划分和技术方案建议。\n"
                    "遇到跨系统影响、安全风险或性能瓶颈时必须升级。"
                ),
            },
        },
        {
            "key": "static.engineer.role",
            "name": "交付工程师角色规范",
            "description": "负责实施执行。",
            "version": "v1",
            "spec": {
                "system_prompt": (
                    "你是交付工程师，负责实现、验证和交付整理。\n"
                    "你可以决定实现顺序、文件修改和验证步骤。\n"
                    "遇到需求冲突、关键失败或外部副作用时必须升级。"
                ),
            },
        },
        {
            "key": "static.reviewer.role",
            "name": "审查员角色规范",
            "description": "负责验收和返工判断。",
            "version": "v1",
            "spec": {
                "system_prompt": (
                    "你是审查员，负责审查、验收和风险标注。\n"
                    "你可以判断是否通过以及返工方向。\n"
                    "遇到高风险发布、关键外部动作或最终交付时必须升级。"
                ),
            },
        },
    ]


def default_memory_profiles() -> list[dict[str, Any]]:
    return [
        {
            "key": "memory.default.collab",
            "name": "默认协作记忆",
            "description": "适用于大多数协作型 Agent 的读写记忆策略。",
            "version": "v1",
            "spec": {
                "short_term": {"enabled": True, "summary_trigger_tokens": 1800, "summary_max_tokens": 400},
                "long_term": {"enabled": True, "namespace_strategy": "agent_team_project", "ttl_days": 30},
                "background_reflection": {"enabled": True, "debounce_seconds": 30},
                "read_scopes": ["agent", "team", "project"],
                "write_scopes": ["agent", "team"],
            },
        },
        {
            "key": "memory.default.reviewer",
            "name": "审查共享记忆",
            "description": "适用于需要沉淀跨任务验收经验的审查 Agent。",
            "version": "v1",
            "spec": {
                "short_term": {"enabled": True, "summary_trigger_tokens": 1600, "summary_max_tokens": 320},
                "long_term": {"enabled": True, "namespace_strategy": "agent_team_project", "ttl_days": 90},
                "background_reflection": {"enabled": True, "debounce_seconds": 15},
                "read_scopes": ["agent", "team", "project"],
                "write_scopes": ["agent", "team", "project"],
            },
        },
    ]


def default_review_policies() -> list[dict[str, Any]]:
    return [
        {
            "key": "review.high_risk_tools",
            "name": "高风险工具审核",
            "description": "关键工具调用前需要人工审核。",
            "version": "v1",
            "spec": {
                "triggers": ["before_tool_call", "before_external_side_effect"],
                "conditions": {
                    "plugin_keys": ["terminal_kit"],
                    "risk_tags": ["workspace_write", "terminal_safe"],
                },
                "actions": ["approve", "reject", "edit_payload"],
            },
        },
        {
            "key": "review.cross_level_message",
            "name": "跨层消息审核",
            "description": "跨层级升级消息可配置为人工审核。",
            "version": "v1",
            "spec": {
                "triggers": ["before_agent_to_agent_message", "before_escalation_to_upper_level"],
                "conditions": {"message_types": ["handoff", "escalation"]},
                "actions": ["approve", "reject", "reroute"],
            },
        },
        {
            "key": "review.shared_memory_write",
            "name": "共享记忆写入审核",
            "description": "团队或项目共享记忆写入前可配置为人工审核。",
            "version": "v1",
            "spec": {
                "triggers": ["before_memory_write"],
                "conditions": {
                    "memory_scopes": ["team", "project"],
                    "risk_tags": ["memory_mutation", "shared_memory"],
                },
                "actions": ["approve", "reject", "edit_records"],
            },
        },
    ]


DEFAULT_AGENT_DEFINITION_IDS = {
    "planner": "0196078f-5f00-7000-8000-000000000001",
    "architect": "0196078f-5f00-7000-8000-000000000002",
    "engineer": "0196078f-5f00-7000-8000-000000000003",
    "reviewer": "0196078f-5f00-7000-8000-000000000004",
}


def default_agent_definitions() -> list[dict[str, Any]]:
    return [
        {
            "id": DEFAULT_AGENT_DEFINITION_IDS["planner"],
            "name": "规划负责人",
            "role": "planner",
            "description": "组合规划技能、静态记忆和默认记忆插件的规划 Agent。",
            "version": "v1",
            "spec": {
                "provider_ref": "mock_local",
                "model": "mock-plan",
                "goal": "拆解复杂任务并协调上下级。",
                "instructions": "优先输出计划、依赖、风险和升级节点。",
                "tool_plugin_refs": ["memory_core", "research_kit"],
                "skill_refs": [],
                "static_memory_ref": "static.planner.role",
                "knowledge_base_refs": [],
                "review_policy_refs": ["review.cross_level_message"],
            },
        },
        {
            "id": DEFAULT_AGENT_DEFINITION_IDS["architect"],
            "name": "架构负责人",
            "role": "architect",
            "description": "组合架构技能和静态记忆的架构 Agent。",
            "version": "v1",
            "spec": {
                "provider_ref": "mock_local",
                "model": "mock-arch",
                "goal": "定义模块边界、接口和关键约束。",
                "instructions": "聚焦边界、依赖、约束和技术方案。",
                "tool_plugin_refs": ["memory_core", "codebase_kit"],
                "skill_refs": [],
                "static_memory_ref": "static.architect.role",
                "knowledge_base_refs": [],
                "review_policy_refs": ["review.cross_level_message"],
            },
        },
        {
            "id": DEFAULT_AGENT_DEFINITION_IDS["engineer"],
            "name": "交付工程师",
            "role": "developer",
            "description": "组合交付技能、终端工具和记忆治理的工程 Agent。",
            "version": "v1",
            "spec": {
                "provider_ref": "mock_local",
                "model": "mock-dev",
                "goal": "执行交付方案并给出验证结果。",
                "instructions": "输出实施步骤、验证结果和风险。",
                "tool_plugin_refs": ["memory_core", "terminal_kit"],
                "skill_refs": [],
                "static_memory_ref": "static.engineer.role",
                "knowledge_base_refs": [],
                "review_policy_refs": ["review.high_risk_tools"],
            },
        },
        {
            "id": DEFAULT_AGENT_DEFINITION_IDS["reviewer"],
            "name": "质量审查员",
            "role": "reviewer",
            "description": "组合审查技能和静态记忆的审查 Agent。",
            "version": "v1",
            "spec": {
                "provider_ref": "mock_local",
                "model": "mock-review",
                "goal": "判断交付是否通过并指出缺口。",
                "instructions": "以验收条件为准，给出明确通过结论。",
                "tool_plugin_refs": ["memory_core", "qa_kit"],
                "skill_refs": [],
                "static_memory_ref": "static.reviewer.role",
                "knowledge_base_refs": [],
                "review_policy_refs": ["review.cross_level_message"],
            },
        },
    ]


def default_team_definitions() -> list[dict[str, Any]]:
    return [
        {
            "key": "team.software_delivery.v1",
            "name": "软件交付团队",
            "description": "按 create_deep_agent 层级组织的默认软件交付团队。",
            "version": "v1",
            "spec": {
                "workspace_id": "local-workspace",
                "project_id": "default-project",
                "lead": {
                    "kind": "agent",
                    "source_kind": "agent_definition",
                    "agent_definition_ref": DEFAULT_AGENT_DEFINITION_IDS["planner"],
                    "name": "规划负责人",
                },
                "children": [
                    {
                        "kind": "agent",
                        "source_kind": "agent_definition",
                        "agent_definition_ref": DEFAULT_AGENT_DEFINITION_IDS["architect"],
                        "name": "架构负责人",
                    },
                    {
                        "kind": "agent",
                        "source_kind": "agent_definition",
                        "agent_definition_ref": DEFAULT_AGENT_DEFINITION_IDS["engineer"],
                        "name": "交付工程师",
                    },
                    {
                        "kind": "agent",
                        "source_kind": "agent_definition",
                        "agent_definition_ref": DEFAULT_AGENT_DEFINITION_IDS["reviewer"],
                        "name": "质量审查员",
                    },
                ],
            },
        }
    ]


def _legacy_default_team_templates_removed() -> list[dict[str, Any]]:
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
