#!/usr/bin/env python3
# asynchronix/examples/python_xrsim.py
#
# Python equivalent of ../../p_xrun.sh: builds the Cartesian product of a
# parameter sweep, turns each combination into the positional argv XR_sim
# expects, and runs them either serially or across a worker pool. See the
# "Running sweeps from Python" section of the top-level README.md.
#
# Deliberately does NOT replicate p_xrun.sh's SLURM/#SBATCH header or its
# weighted-interleaving-across-nodes logic -- this is meant for a single
# machine (or an interactive HPC allocation), with parallelism controlled by
# --workers. For an actual SLURM job array, use p_xrun.sh.

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from itertools import product
from datetime import datetime

EXAMPLE_NAME = "XR_sim"
NUM_INPUT_ARGS_SIM = 35  # must match asynchronix/examples/xr_entry/mod.rs::NUM_INPUT_ARGS_SIM - 1


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
        cwd=project_root,
    )
    data = json.loads(out.decode("utf-8"))
    return Path(data["target_directory"])


def ensure_built(project_root: Path, release: bool = True) -> None:
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
def run_one(exe: Path, argv: list[str], log_path: Path, use_script_ansi: bool, echo: bool) -> int:
    """Runs one XR_sim invocation, tees output to `log_path`.

    When `use_script_ansi` is set (only meaningful for serial runs), the
    child is launched through `script(1)` under a real PTY so the terminal
    color codes emitted by the Rust `colored`-based debug macros survive
    (they'd otherwise be stripped, since a plain subprocess pipe isn't a
    tty) -- mirrors `p_xrun.sh`'s `script -c "..." out_log.ans` call.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", buffering=1) as f:
        f.write(f"# Launched: {datetime.now().isoformat()}\n")
        f.write("# CMD: " + " ".join([str(exe), *argv]) + "\n\n")
        f.flush()

        if use_script_ansi and shutil.which("script"):
            cmd_str = " ".join([str(exe), *argv])
            # -q: quiet startup/exit banners, -f: flush output, -c: command to run
            proc = subprocess.Popen(["script", "-q", "-f", "-c", cmd_str, str(log_path)])
            return proc.wait()

        proc = subprocess.Popen(
            [str(exe), *argv], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            f.write(line)
            if echo:
                print(line, end="")
        return proc.wait()


# ---------- Sweep space (ported from p_xrun.sh) ----------
def build_jobs(results_path_name: str) -> list[tuple[list[str], Path]]:
    # ── Fixed (non-swept) parameters ────────────────────────────────────────
    simTime = 240.0
    mean_length_BG = 12000.0
    PL = 0.1
    T_ABR = 1.0
    observation_type = 1
    reward_mode = 0

    # ── Swept parameter arrays -- edit these to design your experiment ─────
    EMU_TEST_TYPE = ["STD"]
    N_BGs = [0]
    rates_bps_BGtraffic = [5000]
    IS_UL_BG = [0]
    EDCA_BE_MODE = [0]
    DELAY_MAC_ENABLED_MODE = [1]
    NO_UL_TRACKING_MODE = [0]
    MLO_CONFIGS = ["MLO80-80-320"]
    MLO_policies = [1]  # 0 PrimaryFirst, 1 Opportunistic, 2 LyapunovBackpressure
    RANDOMWALK_TEST = 1  # 0 static | 1 random walk | 2 scripted radial distance test
    distance_list = [4.0]
    num_close_users = [0]
    distance_close_users = [1.5]
    packs_per_ampdu = [64]
    CODEC_CHOICES = ["HEVC"]
    USE_FOVEATION = 0
    VBV_PERFRAME = 1
    DETERMINISTIC_FIBONACCI_VIDEO = [0]
    intrarefresh_choice = [1]
    GoP_sizes = [30]
    N_XR = [6]
    initial_bitrate_mbps_CBR = [100.0]
    fps_list = [120.0]
    ABR_ENABLED = [0]  # 0 CBR, 1 NeSt-VR, 2 EveREst, 4 GCC, 5 NADA, 8 Oracle
    nest_profiles = [1]  # 0 Speedy, 1 Balanced, 2 Anxious
    video_samples = ["snow_short"]
    RANDOM_SEEDS = list(range(1, 41))

    combos = product(
        EMU_TEST_TYPE, N_BGs, N_XR, IS_UL_BG, initial_bitrate_mbps_CBR, video_samples,
        fps_list, num_close_users, distance_close_users, RANDOM_SEEDS, distance_list,
        GoP_sizes, intrarefresh_choice, ABR_ENABLED, nest_profiles, MLO_CONFIGS,
        rates_bps_BGtraffic, EDCA_BE_MODE, MLO_policies, packs_per_ampdu, CODEC_CHOICES,
        NO_UL_TRACKING_MODE, DETERMINISTIC_FIBONACCI_VIDEO, DELAY_MAC_ENABLED_MODE,
    )

    jobs: list[tuple[list[str], Path]] = []
    sim_id = 0
    for (test, nbg, nxr, is_ul, bitrate, video_sample, fps, close_users, close_distance,
         seed, distance, gop, intrarefresh, abr, nest_profile, mlo_config, rate_bg,
         edca_be, mlo_policy, ampdu_packs, codec, tracking_bool, deterministic_video,
         delay_app_mac) in combos:
        sim_id += 1

        # Mirrors the Rust-side `name_folder` built in xr_entry::run_sim -- see
        # asynchronix/examples/xr_entry/mod.rs (kept here only for the on-disk log
        # path; the actual scenario subfolder is still created by XR_sim itself).
        name_folder = (
            f"sim_T{simTime:.0f}_D{distance:.1f}_Br{bitrate:.1f}Mbps_FPS{fps:.0f}"
            f"_Codec{codec}_GoP{gop:.0f}_IR{intrarefresh:.0f}_Foveate{USE_FOVEATION:.0f}"
            f"_VBVframe{VBV_PERFRAME:.0f}_macPL{PL:.1f}_aggAMPDU={ampdu_packs:.0f}"
            f"_NXR{nxr:.0f}_NBG{nbg:.0f}_BGLambda{rate_bg:.0f}_UL{is_ul:.0f}_{test}"
            f"_{video_sample}_Nclose{close_users:.0f}_dclose{close_distance:.1f}"
            f"_S{seed:.0f}_ABR{abr:.0f}_{mlo_config}_EDCAbe{edca_be:.0f}_RWALK{RANDOMWALK_TEST:.0f}"
        )

        # Positional argv, exactly matching parse_cli_to_params in
        # asynchronix/examples/xr_entry/mod.rs (args[1..35], args[0] is the exe path).
        argv = [
            f"{simTime}", f"{mean_length_BG}", f"{distance}", f"{bitrate}", f"{PL}",
            f"{nxr}", f"{nbg}", f"{rate_bg}", f"{is_ul}", f"{test}", f"{video_sample}",
            f"{fps}", f"{close_users}", f"{close_distance}", f"{seed}", f"{gop}",
            f"{intrarefresh}", f"{USE_FOVEATION}", f"{VBV_PERFRAME}", f"{abr}",
            f"{nest_profile}", f"{RANDOMWALK_TEST}", f"{sim_id}", f"{observation_type}",
            f"{reward_mode}", f"{T_ABR}", f"{mlo_config}", f"{edca_be}", f"{mlo_policy}",
            f"{ampdu_packs}", f"{codec}", f"{results_path_name}", f"{tracking_bool}",
            f"{deterministic_video}", f"{delay_app_mac}",
        ]
        assert len(argv) == NUM_INPUT_ARGS_SIM, f"expected {NUM_INPUT_ARGS_SIM} args, got {len(argv)}"

        log_path = Path(results_path_name) / name_folder / "sim.log"
        jobs.append((argv, log_path))

    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an XR_sim parameter sweep (Python equivalent of p_xrun.sh).")
    parser.add_argument("--no-build", action="store_true", help="Skip the cargo build step.")
    parser.add_argument("--debug-build", action="store_true", help="Use a debug build instead of --release.")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (1 = serial, default: 1).")
    parser.add_argument("--results-path", default="Results_python_sweep", help="Root results folder (default: Results_python_sweep).")
    parser.add_argument("--ansi-log", action="store_true", help="Serial mode only: capture each run's terminal session (with color) via `script`, like p_xrun.sh's out_log.ans.")
    args = parser.parse_args()

    release = not args.debug_build
    project_root = find_project_root(Path(__file__).parent)
    exe = find_example_exe(project_root, release=release)
    if not args.no_build:
        ensure_built(project_root, release=release)
        exe = find_example_exe(project_root, release=release)
    if not exe.exists():
        raise FileNotFoundError(f"Example not found at {exe} -- build it first or drop --no-build.")

    Path(args.results_path).mkdir(parents=True, exist_ok=True)
    jobs = build_jobs(args.results_path)
    print(f"Built {len(jobs)} simulation job(s) -> results root: {args.results_path}")

    if args.workers <= 1:
        for i, (argv, log) in enumerate(jobs, 1):
            print(f"\n[{i}/{len(jobs)}] Running -> {log.parent.name}")
            code = run_one(exe, argv, log, use_script_ansi=args.ansi_log, echo=not args.ansi_log)
            if code != 0:
                print(f"Run failed (exit {code}) -> {log}", file=sys.stderr)
    else:
        if args.ansi_log:
            print("--ansi-log is only supported in serial mode (--workers 1); ignoring it.", file=sys.stderr)
        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"Running {len(jobs)} sims with {args.workers} workers...")
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {
                ex.submit(run_one, exe, argv, log, False, False): (argv, log)
                for (argv, log) in jobs
            }
            done = 0
            for fut in as_completed(futs):
                done += 1
                argv, log = futs[fut]
                try:
                    code = fut.result()
                except Exception as e:
                    print(f"Exception in run -> {log}: {e}", file=sys.stderr)
                    continue
                status = "OK" if code == 0 else f"FAILED (exit {code})"
                print(f"[{done}/{len(jobs)}] {status} -> {log.parent.name}")


if __name__ == "__main__":
    main()
