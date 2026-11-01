#!/usr/bin/env bash
# Byte-exactness gate: fixed-seed XR_sim runs whose every output file must hash the same before and after a change.
#
#   scripts/byteexact_gate.sh sanity                # each scenario twice (2nd run: other CPU allotment) -> identical
#   scripts/byteexact_gate.sh record <manifest>     # run once, write the sha256 manifest
#   scripts/byteexact_gate.sh check  <manifest>     # run once, compare with the manifest (FAIL on any drift)
#   scripts/byteexact_gate.sh time   <label> <csv>  # GATE_REPS runs per scenario, append label,scenario,rep,wall_s
#   scripts/byteexact_gate.sh run                   # run once into $GATE_WORK/run/<scenario>/res, no hashing
#
# Scenarios: gate_data/scenarios.list (one per line: name, optional ENV=value, key=value overrides of the paper's
# command line). Every scenario runs XR_sim with cwd = repo root (it reads csv_framesizes/ from there) and writes into
# its own results folder; every file in it is hashed, cmp-equivalent (sha256 of the bytes), and so is the MAC
# summary table the run prints (the stdout's "| ... |" lines: blocking probability, queue and service times).
#
# Env:
#   GATE_BIN    XR_sim binary (default: $CARGO_TARGET_DIR or <repo>/target, release/examples/XR_sim)
#   GATE_STOP   simulated seconds (default 20)       GATE_SEED   seed (default 1)
#   GATE_JOBS   scenarios run at once (default 4)    GATE_ONLY   regex on scenario names (default: all)
#   GATE_WORK   scratch dir (default /tmp/xrsim_gate.$USER)
#   GATE_SCEN   scenario file (default gate_data/scenarios.list)
#   GATE_REPS   repetitions in time mode (default 3)
#   GATE_ALT    command prefix of sanity's second run (default "taskset -c 0-1"; empty = same as the first)
#   GATE_TIME   command prefix of every run (perf_bench.sh puts GNU time here)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${GATE_WORK:-/tmp/xrsim_gate.$(id -un)}"
BIN="${GATE_BIN:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/XR_sim}"
STOP="${GATE_STOP:-20}"
SEED="${GATE_SEED:-1}"
JOBS="${GATE_JOBS:-4}"
ONLY="${GATE_ONLY:-.}"
SCEN="${GATE_SCEN:-$ROOT/gate_data/scenarios.list}"
REPS="${GATE_REPS:-3}"
ALT="${GATE_ALT-taskset -c 0-1}"
[ -x "$BIN" ] || { echo "FATAL: XR_sim binary not found: $BIN" >&2; exit 1; }
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
mkdir -p "$WORK"

# The paper's command line (cmds of the 2026-10 rerun of bash_5m_SLO.sh / bash_5m_MLO.sh / bash_rwalk_MLO.sh).
args_of() { # $1 = key=value overrides, $2 = results path -> the 35 XR_sim arguments
    local -A k=([stop]=$STOP [len]=12000.0 [dist]=5.0 [br]=100.0 [pl]=0.1 [nxr]=1 [nbg]=0 [rate]=5000 [isul]=0
        [test]=STD [video]=snow_short [fps]=90.0 [nclose]=0 [dclose]=1.5 [seed]=$SEED [gop]=30 [ir]=1 [fov]=0
        [vbv]=1 [abr]=0 [nest]=1 [rwalk]=0 [simid]=1 [obs]=1 [reward]=0 [tabr]=1.0 [mlo]=SLO80 [edcabe]=0
        [policy]=1 [ampdu]=64 [codec]=HEVC [notrack]=0 [det]=0 [delay]=1)
    local kv
    for kv in $1; do k[${kv%%=*}]=${kv#*=}; done
    echo "${k[stop]} ${k[len]} ${k[dist]} ${k[br]} ${k[pl]} ${k[nxr]} ${k[nbg]} ${k[rate]} ${k[isul]} ${k[test]}" \
        "${k[video]} ${k[fps]} ${k[nclose]} ${k[dclose]} ${k[seed]} ${k[gop]} ${k[ir]} ${k[fov]} ${k[vbv]} ${k[abr]}" \
        "${k[nest]} ${k[rwalk]} ${k[simid]} ${k[obs]} ${k[reward]} ${k[tabr]} ${k[mlo]} ${k[edcabe]} ${k[policy]}" \
        "${k[ampdu]} ${k[codec]} $2 ${k[notrack]} ${k[det]} ${k[delay]}"
}

scenarios() { grep -vE '^\s*(#|$)' "$SCEN" | awk '{print $1}' | grep -E "$ONLY" || true; }

run_one() { # $1 = scenario, $2 = out dir, $3 = command prefix (may be empty) -> status 0/1
    local name=$1 out=$2 prefix=${3:-} line envs="" kvs="" w
    line=$(grep -E "^$name\s" "$SCEN" | head -1 | cut -d' ' -f2-)
    for w in $line; do
        if [[ $w =~ ^[A-Z][A-Z0-9_]*= ]]; then envs="$envs $w"; else kvs="$kvs $w"; fi
    done
    rm -rf "$out"; mkdir -p "$out/res"
    # shellcheck disable=SC2086
    (cd "$ROOT" && env $envs ${GATE_TIME:-} $prefix "$BIN" $(args_of "$kvs" "$out/res") >"$out/.stdout.log" 2>&1) \
        || { echo "RUN FAILED $name (log: $out/.stdout.log)" >&2; return 1; }
}

hash_dir() { # relative-path sha256 manifest of every file a run wrote, plus the MAC summary table it prints
    (cd "$1/res" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum)
    echo "$(grep '^|' "$1/.stdout.log" | sha256sum | cut -d' ' -f1)  ./stdout:mac_summary_table"
}

export -f run_one args_of hash_dir
export ROOT BIN SCEN STOP SEED GATE_TIME

par() { # $1 = function body taking the scenario name as $1; runs over every scenario, GATE_JOBS at a time
    scenarios | xargs -P "$JOBS" -I{} bash -c "$1" _ {}
}

case "${1:?mode}" in
sanity)
    ALT_PREFIX="$ALT"; export ALT_PREFIX WORK
    par 'run_one "$1" "$WORK/s1/$1" && run_one "$1" "$WORK/s2/$1" "$ALT_PREFIX" || exit 0
         h1=$(hash_dir "$WORK/s1/$1"); h2=$(hash_dir "$WORK/s2/$1")
         if [ "$h1" = "$h2" ]; then echo "SANITY OK   $1 ($(echo "$h1" | wc -l) files identical)"
         else echo "SANITY FAIL $1: $(diff <(echo "$h1") <(echo "$h2") | grep -c "^<") of $(echo "$h1" | wc -l) files differ"
              diff <(echo "$h1") <(echo "$h2") | grep "^<" | awk "{print \"    \" \$3}" | head -8; fi' \
        | tee "$WORK/sanity.txt"
    ! grep -q "SANITY FAIL\|RUN FAILED" "$WORK/sanity.txt" ;;
record|check)
    manifest="${2:?manifest}"; export WORK
    par 'run_one "$1" "$WORK/run/$1" && hash_dir "$WORK/run/$1" | sed "s|^|$1  |" > "$WORK/run/$1.sha"'
    tmp="$WORK/manifest.new"
    for s in $(scenarios); do
        [ -s "$WORK/run/$s.sha" ] || { echo "FATAL: no hashes for $s" >&2; exit 1; }
        cat "$WORK/run/$s.sha"
    done >"$tmp"
    if [ "$1" = record ]; then
        { echo "# byteexact_gate.sh manifest: GATE_STOP=$STOP GATE_SEED=$SEED, $(scenarios | wc -l) scenarios"; cat "$tmp"; } >"$manifest"
        echo "recorded $(wc -l <"$tmp") file hashes of $(scenarios | wc -l) scenarios -> $manifest"
    else
        want=$(grep -v '^#' "$manifest" | awk -v re="$ONLY" '$1 ~ re')
        if diff -u <(echo "$want") "$tmp" >"$WORK/check.diff"; then
            echo "BYTE-EXACT OK ($(wc -l <"$tmp") file hashes of $(scenarios | wc -l) scenarios match the manifest)"
        else
            echo "BYTE-EXACT FAIL: drift vs manifest in"
            grep '^[-+][^-+]' "$WORK/check.diff" | awk '{print "   ", $1, $3}' | sort -u | head -40
            exit 1
        fi
    fi ;;
run)
    export WORK
    par 'run_one "$1" "$WORK/run/$1"' ;;
time)
    label="${2:?label}"; csv="${3:?csv}"
    [ -s "$csv" ] || echo "label,scenario,rep,wall_s" >"$csv"
    for s in $(scenarios); do
        for rep in $(seq 1 "$REPS"); do
            t0=$(date +%s.%N); run_one "$s" "$WORK/t/$s"; t1=$(date +%s.%N)
            echo "$label,$s,$rep,$(echo "$t1 $t0" | awk '{printf "%.3f", $1-$2}')" | tee -a "$csv"
        done
    done ;;
*) echo "unknown mode $1" >&2; exit 2 ;;
esac
