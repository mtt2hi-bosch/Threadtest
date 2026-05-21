#!/bin/sh
set -eu

REPO="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
BIN="$REPO/threadtest"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

"$BIN" help >"$TMPDIR/help.txt"
grep -q "threadtest \[options\]" "$TMPDIR/help.txt"

"$BIN" --name=solo --timeout=2ms --work=100us --runtime=20ms >"$TMPDIR/solo.txt"
grep -q "Detected OS:" "$TMPDIR/solo.txt"
grep -q "Statistics for solo" "$TMPDIR/solo.txt"

"$BIN" --name=A --events=ping --set=pong --timeout=2ms --work=50us --runtime=50ms >"$TMPDIR/a.txt" &
PID_A=$!
"$BIN" --name=B --events=pong --set=ping --trigger-start=ping --timeout=2ms --work=50us --runtime=50ms >"$TMPDIR/b.txt" &
PID_B=$!

wait "$PID_A"
wait "$PID_B"

grep -q "Statistics for A" "$TMPDIR/a.txt"
grep -q "Statistics for B" "$TMPDIR/b.txt"
grep -Eq "triggers received:[[:space:]]*[1-9]" "$TMPDIR/a.txt"
grep -Eq "triggers sent:[[:space:]]*[1-9]" "$TMPDIR/b.txt"
