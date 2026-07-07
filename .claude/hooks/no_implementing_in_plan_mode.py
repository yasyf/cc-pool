"""Block delegating implementation work to a write-capable subagent while plan mode is
still active and unapproved.

Plan mode's own permission gate already stops the *main* agent from calling Edit/Write
directly, but that gate does not reach a subagent spawned via the Agent/Task tool -- a
general-purpose (or other write-capable) subagent runs with its own full tool access
regardless of the parent's plan-mode state. That is the loophole this hook closes: plan
mode means design and present a plan, not quietly delegate the implementation to a
subagent before the plan is ever approved via ExitPlanMode.
"""

from __future__ import annotations

from captain_hook import (
    Allow,
    BaseHookEvent,
    Block,
    CustomCondition,
    Event,
    InPlanMode,
    Input,
    Tool,
    hook,
)

# Subagent types that are read-only by design (see .claude/agents docs) and so never
# trip this guard, even while spawned mid-plan for research or design purposes.
_READONLY_SUBAGENT_TYPES = {"explore", "plan"}


class WriteCapableSubagent(CustomCondition):
    """True when an Agent/Task call's ``subagent_type`` is not one of the read-only
    research/design types -- i.e. the spawned subagent can write files."""

    def check(self, evt: BaseHookEvent) -> bool:
        subagent_type = str(evt.input.raw.get("subagent_type") or "").strip().lower()
        return subagent_type not in _READONLY_SUBAGENT_TYPES


hook(
    Event.PreToolUse,
    only_if=[Tool("Agent"), InPlanMode(), WriteCapableSubagent()],
    block=True,
    message=(
        "BLOCKED: still in plan mode -- spawning a write-capable subagent to implement "
        "before the plan is approved. User feedback: \"what? you're in plan mode you're "
        "supposed to make a plan not go off and implement\" (session f4bb5349, "
        "2026-06-24). Finish researching and designing, then get the plan approved via "
        "ExitPlanMode before delegating any implementation work."
    ),
    tests={
        # The offending shape: delegating an implementation task to a general-purpose
        # subagent while the plan is still unapproved.
        Input(
            tool="Agent",
            agent_type="general-purpose",
            prompt="Make cc-pool (at /Users/yasyf/Code/claude-pool) a genuinely blind consumer...",
            permission_mode="plan",
        ): Block(pattern="still in plan mode"),
        # Benign neighbor: a read-only Explore subagent doing research mid-plan is
        # exactly what plan mode is for -- never fires.
        Input(
            tool="Agent",
            agent_type="Explore",
            prompt="Find where the version-skew logic lives in fusekit",
            permission_mode="plan",
        ): Allow(),
        # Benign neighbor: the same delegation, but the plan has already been approved
        # (plan mode is no longer active) -- never fires.
        Input(
            tool="Agent",
            agent_type="general-purpose",
            prompt="Make cc-pool a genuinely blind consumer...",
            permission_mode="default",
        ): Allow(),
    },
)
