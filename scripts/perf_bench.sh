#!/usr/bin/env bash
# Interleaved A/B timing of several XR_sim binaries on the paper's command line.
#
#   scripts/perf_bench.sh <out.csv> <label=binary> [label=binary ...]
#
# Every round runs every scenario with every binary, one after another (interleaved, so slow and fast periods of the
# machine hit every label alike), and records wall time, user+sys CPU time and peak RSS (GNU time). Scenarios are
# gate_data/scenarios.list lines (BENCH_ONLY regex on names) at BENCH_STOP simulated seconds; outputs go to a scratch
# folder and are deleted after each run.
#
# Env: BENCH_STOP (default 60), BENCH_ROUNDS (default 3), BENCH_ONLY (default ^(slo4_cbr|mlo8_cbr|rwk8_mlo_cbr)$),
#      BENCH_SEED (default 1), BENCH_WORK (scratch, default /tmp/xrsim_bench.$USER), BENCH_PIN (taskset CPU list, optional)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:?out.csv}"; shift
STOP="${BENCH_STOP:-60}"; ROUNDS="${BENCH_ROUNDS:-3}"; SEED="${BENCH_SEED:-1}"
ONLY="${BENCH_ONLY:-^(slo4_cbr|mlo8_cbr|rwk8_mlo_cbr)$}"
WORK="${BENCH_WORK:-/tmp/xrsim_bench.$(id -un)}"; mkdir -p "$WORK"
TIME_BIN=$(command -v /usr/bin/time || true); [ -x "$TIME_BIN" ] || { echo "FATAL: GNU time (/usr/bin/time) needed" >&2; exit 1; }
PIN=${BENCH_PIN:+taskset -c $BENCH_PIN}
[ -s "$OUT" ] || echo "label,scenario,round,stop_s,wall_s,cpu_s,max_rss_kb,host" >"$OUT"

for r in $(seq 1 "$ROUNDS"); do
    for s in $(grep -vE '^\s*(#|$)' "$ROOT/gate_data/scenarios.list" | awk '{print $1}' | grep -E "$ONLY"); do
        for lb in "$@"; do
            label=${lb%%=*}; bin=$(readlink -f "${lb#*=}")
            rm -rf "$WORK/run"
            # byteexact_gate.sh's run_one, with GNU time around the binary
            GATE_WORK="$WORK" GATE_BIN="$bin" GATE_STOP="$STOP" GATE_SEED="$SEED" GATE_ONLY="^$s\$" \
                GATE_TIME="$TIME_BIN -f %e,%U,%S,%M -o $WORK/time.txt $PIN" \
                "$ROOT/scripts/byteexact_gate.sh" run >/dev/null
            IFS=, read -r wall usr sys rss <"$WORK/time.txt"
            cpu=$(awk -v u="$usr" -v s="$sys" 'BEGIN{printf "%.2f", u+s}')
            echo "$label,$s,$r,$STOP,$wall,$cpu,$rss,$(hostname -s)" | tee -a "$OUT"
        done
    done
done
rm -rf "$WORK/run"
