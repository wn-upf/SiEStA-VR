#!/usr/bin/env python3
# examples/run_grid.py
import argparse
import json
import random
import shutil
import subprocess
import sys
from pathlib import Path
from itertools import product
from datetime import datetime

EXAMPLE_NAME = "XR_sim"

# ---------- Cargo helpers ----------
def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        if (p / "Cargo.toml").exists():
            return p
    raise RuntimeError(f"Cargo.toml not found upwards from {start}")

def cargo_metadata_target_dir(project_root: Path) -> Path:
    out = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=project_root
    )
    data = json.loads(out.decode("utf-8"))
    return Path(data["target_directory"])

def ensure_built(project_root: Path, release: bool = True):
    if not shutil.which("cargo"):
        raise RuntimeError("cargo not found in PATH.")
    cmd = ["cargo", "build", "--example", EXAMPLE_NAME]
    if release:
        cmd.insert(2, "--release")
    subprocess.check_call(cmd, cwd=project_root)

def find_example_exe(project_root: Path, release: bool = True) -> Path:
    target_dir = cargo_metadata_target_dir(project_root)
    profile = "release" if release else "debug"
    suffix = ".exe" if sys.platform.startswith("win") else ""
    return target_dir / profile / "examples" / f"{EXAMPLE_NAME}{suffix}"

# ---------- Runner ----------
def run_one(exe: Path, args: list[str], log_path: Path, also_stdout: bool = True) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", buffering=1) as f:
        f.write(f"# Launched: {datetime.now().isoformat()}\n")
        f.write("# CMD: " + " ".join([str(exe), *args]) + "\n\n")
        # Stream output to file (and optionally to stdout)
        proc = subprocess.Popen([str(exe), *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            f.write(line)
            if also_stdout:
                print(line, end="")
        return proc.wait()

# ---------- Main grid ----------
def main():
    parser = argparse.ArgumentParser(description="Run XR_sim grid (Python Option A).")
    parser.add_argument("--release", action="store_true", default=True, help="Use release build (default: True).")
    parser.add_argument("--no-build", action="store_true", help="Skip cargo build step.")
    parser.add_argument("--parallel", type=int, default=1, help="Parallel workers (1 = serial).")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle run order.")
    parser.add_argument("--results-root", default="Results", help="Results folder root (default: Results).")
    args = parser.parse_args()

    project_root = find_project_root(Path(__file__).parent)
    exe = find_example_exe(project_root, release=args.release)
    if not args.no_build and not exe.exists():
        ensure_built(project_root, release=args.release)
        exe = find_example_exe(project_root, release=args.release)
    if not exe.exists():
        raise FileNotFoundError(f"Example not found at {exe}")

    # ==== Define your parameter lists (ported from your bash) ====
    TEST_TYPE = ["STD"]                     # "BW", "JI", "PL", "RANDOM", "STD"
    simTime = 40.0
    k_queue = 10000
    mean_length_BG = 12000.0               # BG packet length (your meaning)
    rate_bps_src_BG = 20e6                 # BG arrival rate (bps)
    distance_list = [1.5]
    distance_close_users = [1.5]
    num_close_users = [0]
    N_XR = [1]
    PL = 0.1
    fps_list = [90.0]
    initial_bitrate_mbps = [10.0, 20.0, 40.0]
    ABR_ENABLED = [6]                      # 0 CBR, 1 Nest-VR, 2 Everest, 3 RL, 4 GCC, 5 NADA, 6 FovOptix
    nest_profiles = [1]                    # 0 Speedy, 1 Balanced, 2 Anxious
    RANDOM_SEEDS = list(range(1, 11))      # 1..10
    video_samples = ["snow"]
    N_BGs = [0]
    IS_UL_BG = [0]
    intrarefresh_choice = [1]
    GoP_sizes = [90]
    everest_tests = 1                      # your “test_distances_everest” flag

    # ==== Build the Cartesian product ====
    combos = list(product(
        TEST_TYPE,
        N_BGs,
        N_XR,
        IS_UL_BG,
        initial_bitrate_mbps,
        video_samples,
        fps_list,
        num_close_users,
        distance_close_users,
        RANDOM_SEEDS,
        distance_list,
        GoP_sizes,
        intrarefresh_choice,
        ABR_ENABLED,
        nest_profiles
    ))

    if args.shuffle:
        random.shuffle(combos)

    # ==== Convert each combo to exact argv order expected by main() ====
    # main() expects (23 args after the program):
    # stoptime mean_length_BG k_queue distance bitrate PL n_xr n_bg rate_bps_BG is_ul_bg test_type
    # video_filename FPS n_close distance_close seed gop intrarefresh abr nest_profile everest_tests sim_id
    jobs: list[tuple[list[str], Path]] = []
    sim_id = 0
    for (test, nbg, nxr, is_ul, bitrate, video_sample, FPS, close_users, close_distance,
         seed, distance, gop, intrarefresh, ABR, nest_profile) in combos:
        sim_id += 1

        # folder name (match your bash format)
        name_folder = (
            f"sim_T{simTime:.0f}_D{distance:.0f}_Br{bitrate:.1f}_PL{PL:.1f}"
            f"_NXR{nxr:.0f}_NBG{nbg:.0f}_UL{is_ul:.0f}_{test}_{video_sample}"
            f"_FPS{FPS:.0f}_Nclose{int(close_users)}_dclose{close_distance:.1f}"
            f"_S{seed:.0f}_GoP{gop:.0f}_IR{intrarefresh:.0f}_ABR{ABR:.0f}_nest{nest_profile:.0f}"
        )

        argv = [
            f"{simTime}",
            f"{mean_length_BG}",
            f"{k_queue}",
            f"{distance}",
            f"{bitrate}",
            f"{PL}",
            f"{nxr}",
            f"{nbg}",
            f"{rate_bps_src_BG}",
            f"{is_ul}",
            f"{test}",
            f"{video_sample}",
            f"{FPS}",
            f"{close_users}",
            f"{close_distance}",
            f"{seed}",
            f"{gop}",
            f"{intrarefresh}",
            f"{ABR}",
            f"{nest_profile}",
            f"{everest_tests}",
            f"{sim_id}",
        ]

        log_path = Path(args.results_root) / name_folder / "sim.log"
        jobs.append((argv, log_path))

    # ==== Execute: serial or parallel ====
    if args.parallel <= 1:
        for i, (argv, log) in enumerate(jobs, 1):
            print(f"\n[{i}/{len(jobs)}] Running → {log.parent.name}")
            code = run_one(exe, argv, log, also_stdout=True)
            if code != 0:
                print(f"✖ Run failed (exit {code}) → {log}", file=sys.stderr)
    else:
        # Use ThreadPool (stdout interleaves; logs are separate)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        print(f"Running {len(jobs)} sims with {args.parallel} workers...")
        with ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futs = {ex.submit(run_one, exe, argv, log, False): (argv, log) for (argv, log) in jobs}
            done = 0
            for fut in as_completed(futs):
                done += 1
                argv, log = futs[fut]
                try:
                    code = fut.result()
                except Exception as e:
                    print(f"✖ Exception in run → {log}: {e}", file=sys.stderr)
                    continue
                if code == 0:
                    print(f"[{done}/{len(jobs)}] ✓ {log.parent.name}")
                else:
                    print(f"[{done}/{len(jobs)}] ✖ {log.parent.name} (exit {code})", file=sys.stderr)

if __name__ == "__main__":
    main()
