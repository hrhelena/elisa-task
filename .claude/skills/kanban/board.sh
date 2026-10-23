#!/usr/bin/env bash
# Kanban helper for https://github.com/users/hrhelena/projects/1
# Usage:
#   board.sh add <issue#> [status] [priority] [size]   add issue to board and set fields
#   board.sh move <issue#> <status>                    Backlog|Ready|In progress|In review|Done
#   board.sh set <issue#> <priority|size> <value>      P0|P1|P2 / XS|S|M|L|XL
#   board.sh list [status]                             list items (optionally filtered by status)
set -euo pipefail

OWNER=hrhelena
PROJECT=1
PROJECT_ID=PVT_kwHOAJfFqc4BoWiQ
REPO=hrhelena/elisa-task
STATUS_FIELD=PVTSSF_lAHOAJfFqc4BoWiQzhm3DHs
PRIORITY_FIELD=PVTSSF_lAHOAJfFqc4BoWiQzhm3DV0
SIZE_FIELD=PVTSSF_lAHOAJfFqc4BoWiQzhm3DV4

option_id() {
  case "$1" in
    Backlog) echo f75ad846 ;; Ready) echo 61e4505c ;; "In progress") echo 47fc9ee4 ;;
    "In review") echo df73e18b ;; Done) echo 98236657 ;;
    P0) echo 79628723 ;; P1) echo 0a877460 ;; P2) echo da944a9c ;;
    XS) echo 6c6483d2 ;; S) echo f784b110 ;; M) echo 7515a9f1 ;; L) echo 817d0097 ;; XL) echo db339eb2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
}

item_id() {
  gh project item-list "$PROJECT" --owner "$OWNER" --limit 500 --format json \
    --jq ".items[] | select(.content.number == $1 and .content.repository == \"$REPO\") | .id"
}

set_field() { # item field value
  gh project item-edit --project-id "$PROJECT_ID" --id "$1" --field-id "$2" \
    --single-select-option-id "$(option_id "$3")" >/dev/null
}

cmd=${1:-}; shift || true
case "$cmd" in
  add)
    id=$(gh project item-add "$PROJECT" --owner "$OWNER" \
      --url "https://github.com/$REPO/issues/$1" --format json --jq .id)
    set_field "$id" "$STATUS_FIELD" "${2:-Backlog}"
    [ -n "${3:-}" ] && set_field "$id" "$PRIORITY_FIELD" "$3"
    [ -n "${4:-}" ] && set_field "$id" "$SIZE_FIELD" "$4"
    echo "added #$1 ($id)" ;;
  move)
    id=$(item_id "$1"); [ -n "$id" ] || { echo "issue #$1 not on board" >&2; exit 1; }
    set_field "$id" "$STATUS_FIELD" "$2"; echo "#$1 -> $2" ;;
  set)
    id=$(item_id "$1")
    case "$2" in priority) f=$PRIORITY_FIELD ;; size) f=$SIZE_FIELD ;; *) echo "bad field" >&2; exit 2 ;; esac
    set_field "$id" "$f" "$3"; echo "#$1 $2=$3" ;;
  list)
    gh project item-list "$PROJECT" --owner "$OWNER" --limit 500 --format json \
      --jq ".items[] | select(\"${1:-}\" == \"\" or .status == \"${1:-}\") | \"#\(.content.number)\t\(.status)\t\(.priority // \"-\")\t\(.size // \"-\")\t\(.title)\"" ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
