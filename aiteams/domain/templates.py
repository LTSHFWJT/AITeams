from __future__ import annotations

from typing import Any


def built_in_blueprint_templates() -> list[dict[str, Any]]:
    return [
        software_delivery_template(),
        approval_delivery_template(),
        research_parallel_template(),
    ]


def software_delivery_template() -> dict[str, Any]:
    return {
        "name": "software_delivery",
        "description": "Software delivery workflow with role templates, isolated agent memory, and graph-scoped coordination.",
        "workspace_id": "local-workspace",
        "project_id": "default-project",
        "role_templates": {
            "strategy_planner": {
                "name": "Strategy Planner",
                "role": "planner",
                "goal": "Break down the objective into milestones, constraints, and execution checkpoints.",
                "instructions": "Think in terms of staged delivery, explicit dependencies, and verifiable outputs.",
                "backend": "mock",
                "model": "mock-plan",
                "workbenches": ["research"],
                "memory_policy": "agent_private",
            },
            "solution_architect": {
                "name": "Solution Architect",
                "role": "architect",
                "goal": "Define boundaries, contracts, and system-level decisions for the project.",
                "instructions": "Translate planning outputs into architecture slices, interfaces, and tradeoffs.",
                "backend": "mock",
                "model": "mock-arch",
                "workbenches": ["codebase"],
                "memory_policy": "agent_private",
            },
            "implementation_engineer": {
                "name": "Implementation Engineer",
                "role": "developer",
                "goal": "Produce an implementation sequence, module ownership, and validation steps.",
                "instructions": "Prepare a delivery-oriented build plan with explicit file and testing boundaries.",
                "backend": "mock",
                "model": "mock-dev",
                "workbenches": ["terminal"],
                "memory_policy": "agent_private",
            },
            "quality_reviewer": {
                "name": "Quality Reviewer",
                "role": "reviewer",
                "goal": "Decide whether the current delivery package meets the stated acceptance bar.",
                "instructions": "Review for completeness, correctness, and missing evidence before sign-off.",
                "backend": "mock",
                "model": "mock-review",
                "workbenches": ["qa"],
                "memory_policy": "agent_private",
            },
        },
        "agents": {
            "planner": {"role_template": "strategy_planner", "name": "Planner Lead"},
            "architect": {"role_template": "solution_architect", "name": "Architecture Lead"},
            "developer": {"role_template": "implementation_engineer", "name": "Delivery Engineer"},
            "reviewer": {"role_template": "quality_reviewer", "name": "Review Gate"},
        },
        "workbenches": {
            "research": {
                "name": "Research Workbench",
                "tools": ["search", "docs"],
                "permissions": ["readonly", "network_limited"],
            },
            "codebase": {
                "name": "Codebase Workbench",
                "tools": ["files", "git", "diff"],
                "permissions": ["readonly"],
            },
            "terminal": {
                "name": "Terminal Workbench",
                "tools": ["terminal", "files"],
                "permissions": ["workspace_write", "terminal_safe"],
            },
            "qa": {
                "name": "QA Workbench",
                "tools": ["tests", "reports"],
                "permissions": ["readonly"],
            },
        },
        "flow": {
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "plan", "type": "agent", "agent": "planner", "instruction": "Break down the task into milestones, risks, and a delivery sequence."},
                {"id": "design", "type": "agent", "agent": "architect", "instruction": "Define boundaries, modules, interfaces, and technical constraints."},
                {"id": "implement", "type": "agent", "agent": "developer", "instruction": "Produce the implementation backlog, execution order, and validation plan."},
                {"id": "review", "type": "agent", "agent": "reviewer", "instruction": "Decide whether review.pass should be true and summarize remaining risks."},
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
    }


def approval_delivery_template() -> dict[str, Any]:
    return {
        "name": "approval_delivery",
        "description": "Delivery flow with an explicit manual approval gate and graph-scoped visibility.",
        "workspace_id": "local-workspace",
        "project_id": "default-project",
        "role_templates": {
            "proposal_planner": {
                "name": "Proposal Planner",
                "role": "planner",
                "goal": "Generate an execution proposal that is ready for human review.",
                "instructions": "Summarize the plan clearly enough for an approval checkpoint.",
                "backend": "mock",
                "model": "mock-plan",
                "memory_policy": "agent_private",
            }
        },
        "agents": {
            "planner": {"role_template": "proposal_planner", "name": "Approval Planner"},
        },
        "flow": {
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "plan", "type": "agent", "agent": "planner", "instruction": "Produce the plan and the approval rationale."},
                {"id": "approve", "type": "approval", "name": "Manual execution approval"},
                {
                    "id": "artifact",
                    "type": "artifact",
                    "name": "approval-note.md",
                    "artifact_kind": "report",
                    "template": "# Approved Delivery Note\n\n{{plan.summary}}",
                },
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
    }


def research_parallel_template() -> dict[str, Any]:
    return {
        "name": "research_parallel",
        "description": "Parallel research flow where branches stay isolated until they converge at merge.",
        "workspace_id": "local-workspace",
        "project_id": "default-project",
        "role_templates": {
            "topic_researcher": {
                "name": "Topic Researcher",
                "role": "researcher",
                "goal": "Investigate the opportunity, audience, and delivery target.",
                "instructions": "Focus on product value, user scope, and expected outcomes.",
                "backend": "mock",
                "model": "mock-research",
                "memory_policy": "agent_private",
            },
            "risk_specialist": {
                "name": "Risk Specialist",
                "role": "analyst",
                "goal": "Investigate dependencies, constraints, and failure modes.",
                "instructions": "Surface operational, technical, and schedule risks explicitly.",
                "backend": "mock",
                "model": "mock-risk",
                "memory_policy": "agent_private",
            },
            "synthesis_lead": {
                "name": "Synthesis Lead",
                "role": "synthesizer",
                "goal": "Merge branch results into one decision-ready conclusion.",
                "instructions": "Resolve branch differences and produce a single recommendation.",
                "backend": "mock",
                "model": "mock-synth",
                "memory_policy": "agent_private",
            },
        },
        "agents": {
            "researcher": {"role_template": "topic_researcher", "name": "Topic Branch"},
            "risk_analyst": {"role_template": "risk_specialist", "name": "Risk Branch"},
            "synthesizer": {"role_template": "synthesis_lead", "name": "Merge Synthesizer"},
        },
        "flow": {
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "parallel", "type": "parallel"},
                {"id": "topic_research", "type": "agent", "agent": "researcher", "instruction": "Research the core value, scope, and intended impact."},
                {"id": "risk_research", "type": "agent", "agent": "risk_analyst", "instruction": "Research risks, constraints, and critical dependencies."},
                {"id": "merge", "type": "merge"},
                {"id": "synthesize", "type": "agent", "agent": "synthesizer", "instruction": "Combine the merged branch results into one recommendation."},
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
    }
