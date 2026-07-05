"""Nudge when an edit leaves an oversized contiguous comment run in Swift source.

Swift-only: the builtin general pack's ``comments.py`` already covers Go (and every other
ast-grep language) at a stricter diff-gated budget, so this fills the one gap — ast-grep has
no Swift grammar. Text-based for the same reason. A run must exceed :data:`COMMENT_RUN_LIMIT`
lines, so a normal doc comment never fires; only genuine bloat does.
"""

from __future__ import annotations

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

COMMENT_RUN_LIMIT = 6

# Fixtures for the inline tests — module constants keep each physical line short.
GO_BLOAT = (
    "package p\n\n"
    "// F does a thing in the pool.\n"
    "// It first resolves the account dir.\n"
    "// Then it checks the keychain item.\n"
    "// If the item is missing it errors.\n"
    "// Otherwise it refreshes the token.\n"
    "// Finally it persists the new blob.\n"
    "// Callers must hold no locks here.\n"
    "func F() {}\n"
)
SWIFT_BLOCK_BLOAT = (
    "/**\n"
    " Renders the status tile.\n"
    "\n"
    " The tile shows 5h and 7d headroom.\n"
    " Colors follow the system palette.\n"
    " Updates arrive via the bridge socket.\n"
    " Redraws are debounced to 1Hz.\n"
    " */\n"
    "struct Tile {}\n"
)
SWIFT_SHORT_DOC = "/// Renders the status tile.\nstruct Tile {}\n"
PY_HASH_RUN = (
    "# one\n# two\n# three\n# four\n# five\n# six\n# seven\nx = 1\n"
)


def longest_comment_run(text: str) -> int:
    """Longest run of contiguous comment lines: adjacent ``//`` lines or a ``/* */`` block."""
    longest = run = 0
    in_block = False
    for raw in text.splitlines():
        line = raw.strip()
        if in_block:
            run += 1
            in_block = "*/" not in line
        elif line.startswith("//"):
            run += 1
        elif line.startswith("/*"):
            run += 1
            in_block = "*/" not in line
        else:
            longest, run = max(longest, run), 0
    return max(longest, run)


class ExcessiveCommentRun(CustomCondition):
    """True when the written content contains a comment run past the line budget."""

    def check(self, evt: BaseHookEvent) -> bool:
        return evt.content is not None and longest_comment_run(evt.content) > COMMENT_RUN_LIMIT


nudge(
    f"Comment bloat: this edit leaves a comment run longer than {COMMENT_RUN_LIMIT} lines. "
    "Comments are terse and sparing — names, types, and organization carry the meaning; a "
    "godoc is a description, not an essay. Move rationale, history, or investigation notes "
    "to cc-notes (`ccn doc add`) and keep at most a one-line pointer. Legitimate exceptions "
    "(load-bearing invariants, workarounds) stay. See: STYLEGUIDE.md § Comments.",
    only_if=[Tool("Edit", "Write"), FilePath("*.swift"), ExcessiveCommentRun()],
    events=Event.PostToolUse,
    max_fires=3,
    tests={
        # Bloated Swift run — warned.
        Input(tool="Write", file="Tile.swift", content=SWIFT_BLOCK_BLOAT): Warn(pattern="Comment bloat"),
        # At or under the budget — allowed.
        Input(tool="Write", file="Tile.swift", content=SWIFT_SHORT_DOC): Allow(),
        # Go is the builtin general pack's job — out of scope here even when bloated.
        Input(file="pool.go", content=GO_BLOAT): Allow(),
        # Other languages are out of scope (the builtin general pack covers them).
        Input(file="conf.py", content=PY_HASH_RUN): Allow(),
    },
)
