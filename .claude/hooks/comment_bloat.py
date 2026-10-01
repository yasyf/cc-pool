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


# WORKAROUND: ast-grep has no Swift grammar, so comment runs are scanned as text.
def longest_comment_run(text: str) -> int:
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
    def check(self, evt: BaseHookEvent) -> bool:
        return evt.content is not None and longest_comment_run(evt.content) > COMMENT_RUN_LIMIT


nudge(
    f"This edit leaves a comment run longer than {COMMENT_RUN_LIMIT} lines. "
    "Move the rationale to `ccn doc add` and keep at most a one-line pointer.",
    only_if=[Tool("Edit", "Write"), FilePath("*.swift"), ExcessiveCommentRun()],
    events=Event.PostToolUse,
    max_fires=3,
    tests={
        Input(tool="Write", file="Tile.swift", content=SWIFT_BLOCK_BLOAT): Warn(pattern="comment run"),
        Input(tool="Write", file="Tile.swift", content=SWIFT_SHORT_DOC): Allow(),
        Input(file="pool.go", content=GO_BLOAT): Allow(),
        Input(file="conf.py", content=PY_HASH_RUN): Allow(),
    },
)
