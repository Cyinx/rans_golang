#!/usr/bin/env bash
# sync-oss-snapshot 同步公开线快照（票 22：不触碰工作树的临时 index 流程）。
# 产出 oss-snapshot 分支新提交 = main 的树减内部路径；工作树零修改。
# 自检：排除清单 grep 为空 + 排除内部路径后 diff main 为空（调用方执行）。
set -euo pipefail
REPO="${1:-$(git rev-parse --show-toplevel)}"
cd "$REPO"

INTERNAL_PATHS=(.scratch AGENTS.md docs/superpowers docs/porting docs/decisions docs/agents docs/ROADMAP.md docs/ISSUES.md)

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
GIT_INDEX_FILE="$tmp" git read-tree main
GIT_INDEX_FILE="$tmp" git rm -r -q --cached --ignore-unmatch "${INTERNAL_PATHS[@]}"
tree=$(GIT_INDEX_FILE="$tmp" git write-tree)
c=$(git commit-tree "$tree" -p oss-snapshot -m "同步 main@$(git rev-parse --short main)")
git update-ref refs/heads/oss-snapshot "$c"
echo "oss-snapshot -> $c"
