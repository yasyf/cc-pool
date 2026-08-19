"""Nudge to home a new generic mechanism in fusekit instead of siloing it in cc-pool.

User feedback (session 077a077a-8fb1-4d11-8512-c53ec3aa0335, 2026-06-22), answering how
far a fix plan should go: "2, and move any of those fixes that make sense into fusekit
so all can benefit". This repo consumes github.com/yasyf/fusekit as a shared library
across several of the user's projects; a fix that is a generic, reusable mechanism
(process supervision, backoff, mount/overlay primitives) belongs there first so every
consumer benefits, not patched only in cc-pool. Scoped to the two dirs where that kind
of mechanism code actually lives here (internal/daemon, internal/overlay) -- cc-pool's
own CLI/account/policy code is out of scope.
"""

from __future__ import annotations

import re

from captain_hook import (
    Allow,
    BaseHookEvent,
    CustomCondition,
    Event,
    FilePath,
    Input,
    Tool,
    Warn,
    nudge,
)

TOPLEVEL_DECL = re.compile(r"^(?:func\s+(?:\([^)]*\)\s*)?|type\s+)(\w+)")


def decl_names(text: str) -> set[str]:
    return {m.group(1) for line in text.splitlines() if (m := TOPLEVEL_DECL.match(line))}


class NewGenericMechanism(CustomCondition):
    """True when an edit to a file already touching fusekit adds a brand-new top-level
    func/type -- the shape of re-implementing a mechanism locally instead of extending
    fusekit, rather than a plain call-site or policy tweak on an existing declaration."""

    def check(self, evt: BaseHookEvent) -> bool:
        content = evt.content or ""
        if "fusekit" not in content:
            return False
        return bool(decl_names(content) - decl_names(evt.old or ""))


EXISTING_IMPORT = 'import "github.com/yasyf/fusekit/proc"\n\nfunc reconcile() {}\n'

NEW_MECHANISM = (
    'import "github.com/yasyf/fusekit/proc"\n\n'
    "func reconcile() {}\n\n"
    "func retryBackoff(attempt int) time.Duration {\n"
    "\treturn time.Duration(attempt) * time.Second\n"
    "}\n"
)

POLICY_TWEAK = (
    'import "github.com/yasyf/fusekit/proc"\n\n'
    "func reconcile() {\n\treturn proc.Tick()\n}\n"
)

nudge(
    "Centralize shared fixes: this adds a new generic mechanism to a file that already "
    'touches fusekit. User feedback: "move any of those fixes that make sense into '
    'fusekit so all can benefit" -- before implementing this locally, check whether it '
    "belongs in fusekit (github.com/yasyf/fusekit) first, with cc-pool consuming the new "
    "release, so cc-squash/cc-notes/synckit/hostsync get it too.",
    only_if=[
        Tool("Edit", "Write"),
        FilePath("internal/daemon/*.go", "internal/overlay/*.go"),
        NewGenericMechanism(),
    ],
    events=Event.PostToolUse,
    max_fires=3,
    tests={
        # A new top-level func lands in a fusekit-touching daemon file -- the offending shape.
        Input(
            tool="Edit",
            file="internal/daemon/holderpolicy.go",
            old=EXISTING_IMPORT,
            content=NEW_MECHANISM,
        ): Warn(pattern="Centralize shared fixes"),
        # Same file, same fusekit contact, but only a call-site/policy tweak inside the
        # existing declaration -- no new top-level name. Closest benign neighbor.
        Input(
            tool="Edit",
            file="internal/daemon/holderpolicy.go",
            old=EXISTING_IMPORT,
            content=POLICY_TWEAK,
        ): Allow(),
        # cc-pool-specific CLI code is out of scope even with a brand-new function.
        Input(
            tool="Edit",
            file="internal/cli/status.go",
            old=EXISTING_IMPORT,
            content=NEW_MECHANISM,
        ): Allow(),
    },
)
