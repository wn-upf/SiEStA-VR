#!/usr/bin/env bash
# mksnap.sh <snap dir> <label> <commit>: source snapshot of a commit for an HPC build (scripts/hpc_perf/build.sbatch).
# Adds the 'optimized' profile to the workspace Cargo.toml when the commit predates it (no source change), and copies
# the worktree's untracked Cargo.lock and its gate / benchmark scripts.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
D="$1/$2"; rm -rf "$D"; mkdir -p "$D"
git -C "$ROOT" archive "$3" Cargo.toml asynchronix csv_framesizes | tar -x -C "$D"
cp -r "$ROOT/scripts" "$ROOT/gate_data" "$D/"   # the worktree's harness, the same for every snapshot
rm -rf "$D"/asynchronix/video_samples* "$D"/asynchronix/orig_examples/*.mp4 2>/dev/null || true
cp "$ROOT/Cargo.lock" "$D/"
grep -q '^\[profile.optimized\]' "$D/Cargo.toml" || printf '\n[profile.optimized]\ninherits = "release"\nlto = "fat"\ncodegen-units = 1\n' >>"$D/Cargo.toml"
echo "$2 $(git -C "$ROOT" rev-parse --short "$3") $(du -sh "$D" | cut -f1)"
