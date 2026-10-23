#!/usr/bin/env python3
"""PreToolUse hook: refuse Bash commands that bypass the git pre-commit hooks.

Works on shell tokens (shlex), so text inside quoted arguments such as commit
messages or PR bodies is never mistaken for a flag. Exit code 2 blocks the tool
call and shows stderr to the agent.
"""
import json
import re
import shlex
import sys

SEPARATORS = {"&&", "||", ";", "|", "&", "(", ")"}
HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1")


def strip_heredocs(cmd: str) -> str:
    """Drop heredoc bodies: they are data fed to a command, not commands."""
    out, pos = [], 0
    for m in HEREDOC.finditer(cmd):
        if m.start() < pos:
            continue
        nl = cmd.find("\n", m.end())
        if nl == -1:
            break
        end = re.search(rf"^\t*{re.escape(m.group(2))}[ \t]*$", cmd[nl + 1 :], re.M)
        stop = nl + 1 + end.end() if end else len(cmd)
        out.append(cmd[pos : m.start()] + cmd[m.end() : nl + 1])
        pos = stop
    out.append(cmd[pos:])
    return "".join(out)


def tokenize(cmd: str) -> list[str]:
    cmd = strip_heredocs(cmd).replace("\n", " ; ")
    lex = shlex.shlex(cmd, posix=True, punctuation_chars=";&|()")
    lex.whitespace_split = True
    try:
        return list(lex)
    except ValueError:
        return cmd.split()


def simple_commands(tokens: list[str]):
    cur: list[str] = []
    for tok in tokens:
        if tok in SEPARATORS:
            if cur:
                yield cur
            cur = []
        else:
            cur.append(tok)
    if cur:
        yield cur


def violations(cmd: str) -> list[str]:
    hits = []
    for words in simple_commands(tokenize(cmd)):
        # Leading VAR=value assignments.
        i = 0
        while i < len(words) and "=" in words[i] and not words[i].startswith("-") and words[i].split("=", 1)[0].isidentifier():
            if words[i].startswith("SKIP="):
                hits.append("SKIP= (pre-commit skip list)")
            i += 1
        args = words[i:]
        if not args or args[0] != "git":
            continue
        # git global options before the subcommand, e.g. -c core.hooksPath=...
        j = 1
        while j < len(args) and args[j].startswith("-"):
            if args[j] == "-c" and j + 1 < len(args):
                if args[j + 1].lower().startswith("core.hookspath"):
                    hits.append("overriding core.hooksPath")
                j += 2
                continue
            j += 1
        sub = args[j] if j < len(args) else ""
        rest = args[j + 1 :]
        if sub == "config" and any(a.lower().startswith("core.hookspath") for a in rest):
            hits.append("overriding core.hooksPath")
        if sub in ("commit", "merge", "push", "rebase", "am", "cherry-pick", "revert"):
            skip_next = False
            for a in rest:
                if skip_next:
                    skip_next = False
                    continue
                if a == "--":
                    break
                if a == "--no-verify":
                    hits.append("--no-verify")
                elif a in ("-m", "-F", "-C", "-c", "--author", "--date", "--file", "--message"):
                    skip_next = True  # flag value is free text
                elif sub == "commit" and a.startswith("-") and not a.startswith("--"):
                    flags = a[1:]
                    # In combined short flags, anything after m/F/C is that flag's value.
                    for k, ch in enumerate(flags):
                        if ch == "n":
                            hits.append("git commit -n")
                            break
                        if ch in "mFCc":
                            if k == len(flags) - 1:
                                skip_next = True
                            break
    return hits


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    cmd = (data.get("tool_input") or {}).get("command") or ""
    hits = violations(cmd)
    if hits:
        print(
            "Blocked: this command bypasses the git pre-commit hooks ("
            + ", ".join(sorted(set(hits)))
            + "). Project rule: never skip hooks. Fix what the hook reports; "
            "to set work aside use `git stash push -u -m <tag>` (apply by SHA) instead of a WIP commit. "
            "If a hook is genuinely broken, stop and report it to the orchestrator.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
