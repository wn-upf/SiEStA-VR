import gymnasium as gym  # Changed from 'gym'
from gymnasium import spaces
import zmq
import json
import numpy as np
import wandb
import sys
import os
import torch
import warnings
import signal
import atexit
import random
import multiprocessing as mp
import time
from pathlib import Path
from itertools import product
import subprocess   
from datetime import datetime 
import shutil         
from omegaconf import OmegaConf  


DREAMER_REPO_PATH = "/home/boris/Desktop/Rust_MG1/asynchronix/python_RL/torch_dreamer/dreamerv3-torch"
sys.path.append(DREAMER_REPO_PATH)

# --- 2. Import Dreamer and your new Env ---
# --- 2. Import Dreamer and your new Env ---
# from dreamer import main as dreamer_train_main      # This is the training function
# from dreamer import load_config as dreamer_load_config  
# import tools as dreamer_tools                       # This is for patching

_ENV_CACHE = {}
RUST_PROCS = []

N_STEPS_RL= 10_000_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   
FEAT_DIM = 14
WINDOW_LEN = 1
OBSERVATION_SHAPE = (WINDOW_LEN * FEAT_DIM, )


#### RL INPUT ARGS (RUST)
observation_type = 1 ## 0-> Raw unscaled obs, 1 -> Scaled in expected bounds, 2-> Running Normalization. 
reward_mode = 0 ## normalized reward.  // 0-> naive , 1-> normalized, 2-> ??? todo shaping. 
T_ABR = 0.3 ## update every T seconds. With lower value, more frequent steps in simulation but noisier updates. 
##################################################################################################  
### SIMULATION PARAMS
simTime = [80.0]
TEST_TYPE = [ "STD", "BW", "RANDOM"]                     # "BW", "JI", "PL", "RANDOM", "STD"
k_queue = 10000
mean_length_BG = 12000.0
rate_bps_src_BG = [10e6, 20e6, 40e6]
distance_list = [1.5]
distance_close_users = [1.5]
num_close_users = [0]
N_XR = [1, 2, 3]
PL = [0.0001, 0.01, 0.1, 0.15]
fps_list = [60.0, 90.0, 120.0 ]
initial_bitrate_mbps = [10.0, 20.0, 40.0]
ABR_ENABLED = [3]
nest_profiles = [1]
RANDOM_SEEDS = list(range(1, 80))
video_samples = ["snow"]
N_BGs = [0]
IS_UL_BG = [0]
intrarefresh_choice = [1]
GoP_sizes = [90]
everest_tests = 1
##################################################################################################

# Import your env and constants
# from your_env_file import DreamerZmqEnv, ACTION_ENDPOINT, STEP_ENDPOINT, BITRATE_LADDER_MBPS
##############################

ACTION_ENDPOINT  = os.environ.get("ZMQ_ACTION_EP",  "ipc:///tmp/xr_default_action")
STEP_ENDPOINT    = os.environ.get("ZMQ_STEP_EP",    "ipc:///tmp/xr_default_step")
TRAINER_ENDPOINT = os.environ.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")
BITRATE_LADDER_MBPS = list(range(5, 101, 5))

################################################

# --- CHANGE 1: Use your unstacked feature dimension ---
# FEAT_DIM and WINDOW_LEN are from your constants
OBSERVATION_SHAPE = (FEAT_DIM,) # Now (14,) not (70,)
ACTION_DIM = 20 

def train_over_all_combos_iter(exe: Path, combos, num_passes: int = 10):
    """
    Run multiple shuffled passes over all simulation combos.
    """
    # ---- Fixed endpoints for the entire run ----
    base_id = os.environ.get("SLURM_JOB_ID") or os.getpid()
    RUN_ID = f"{base_id}_train"

    action_ep  = f"ipc:///tmp/xr_{RUN_ID}_action"
    step_ep    = f"ipc:///tmp/xr_{RUN_ID}_step"
    # trainer_ep = f"ipc:///tmp/xr_{RUN_ID}_trainer"

    trainer_process = mp.Process(
        target=train_dreamer_main, # <-- Use the new target
        args=(action_ep, step_ep),
        daemon=True
    )

    trainer_process.start()

    # pool = ThreadPoolExecutor(max_workers=5)
    # fut_rl = pool.submit(train_agent_single, action_ep, step_ep)
    # fut_rl = pool.submit(train_sac_single, action_ep, step_ep)

    time.sleep(15.0) ## TODO: WAIT UNTIL TRAINER IS READY (TempFile)

    # ---- Start RL thread (same endpoints for all episodes) ----
    print(f"RL loop started on:\n\t{action_ep},\n\t{step_ep}")

    # time.sleep(15.0)

    # ---- Outer training loop over multiple passes ----
    for pass_idx in range(1, num_passes + 1):
        random.shuffle(combos)
        print(f"\n🔁 Starting pass {pass_idx}/{num_passes} — {len(combos)} combos.")

        for sim_count, combo in enumerate(combos, 1):
            (simtime, test, nbg, nxr, is_ul, bitrate, video_sample, FPS,
             close_users, close_distance, seed, distance, gop,
             intrarefresh, ABR, nest_profile, rate_bps_src_BG, pl_prob) = combo

            argv = [
                f"{simtime}", "12000.0", "10000", f"{distance}", f"{bitrate}",
                f"{pl_prob}", f"{nxr}", f"{nbg}", f"{rate_bps_src_BG}", f"{is_ul}",
                f"{test}", f"{video_sample}", f"{FPS}", f"{close_users}", f"{close_distance}",
                f"{seed}", f"{gop}", f"{intrarefresh}", f"{ABR}", f"{nest_profile}",
                "1", f"{sim_count}", f"{observation_type}", f"{reward_mode}", f"{T_ABR}", f"dreamer", 
            ]

            env_sim = os.environ.copy()
            env_sim["ZMQ_ACTION_EP"]  = action_ep
            env_sim["ZMQ_STEP_EP"]    = step_ep
            env_sim["WANDB_RUN_GROUP"] = f"pass_{pass_idx}_episode_{sim_count}"

            log_path = Path("Results") / f"pass_{pass_idx}_combo_{sim_count}" / "sim.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)

            print(f"\n🚀 Pass {pass_idx}: launching combo {sim_count}/{len(combos)}")
            ret = run_sim(exe, argv, env_sim, log_path)
            print(f"✅ Pass {pass_idx}, combo {sim_count}: simulator exited with {ret}")

        print(f"🎯 Finished pass {pass_idx}/{num_passes}")

    print("🧹 All passes done — waiting for RL to finish or reach its timestep limit.")
    # server.close()
    # pool.shutdown(wait=False)
EXAMPLE_NAME = "XR_sim"

def find_project_root(start: Path) -> Path:
    for p in [start.resolve(), *start.resolve().parents]:
        if (p / "Cargo.toml").exists():
            return p
    raise RuntimeError("Cargo.toml not found.")


def rebuild_rust_binary(example_name="XR_sim"):
    """Force recompile the Rust example in --release mode before running."""
    project_root = find_project_root(Path(__file__).parent)
    print(f"🔨 Rebuilding Rust example `{example_name}` in release mode...")
    cmd = ["cargo", "build", "--release", "--example", example_name]
    start_time = time.time()
    result = subprocess.run(cmd, cwd=project_root, text=True, capture_output=True)
    duration = time.time() - start_time
    if result.returncode != 0:
        print(f"❌ Cargo build failed after {duration:.1f}s:\n{result.stderr}")
        sys.exit(1)
    else:
        print(f"✅ Rust binary rebuilt successfully in {duration:.1f}s")


def find_exe(release=True):
    root = find_project_root(Path(__file__).parent)
    target_dir = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=root
    ).decode())["target_directory"]
    suffix = ".exe" if sys.platform.startswith("win") else ""
    return Path(target_dir) / ("release" if release else "debug") / "examples" / f"{EXAMPLE_NAME}{suffix}"


def run_sim(exe: Path, argv: list[str], env: dict[str, str], log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", buffering=1) as f:
        f.write(f"# Started: {datetime.now().isoformat()}\nCMD: {' '.join([str(exe), *argv])}\n\n")
        proc = subprocess.Popen([str(exe), *argv],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env)

        RUST_PROCS.append(proc)  ## track rust globally 

        for line in proc.stdout:
            f.write(line)
            print(line, end="")
        return proc.wait()


def cleanup_rust_processes():
    """Kill all Rust simulator subprocesses still running."""
    global RUST_PROCS
    for p in RUST_PROCS:
        if p.poll() is None:  # still running
            try:
                print(f"[CLEANUP] Terminating Rust process pid={p.pid}")
                p.terminate()
                try:
                    p.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    print(f"[CLEANUP] Killing stubborn process pid={p.pid}")
                    p.kill()
            except Exception as e:
                print(f"[CLEANUP] Error killing pid={p.pid}: {e}")
    RUST_PROCS.clear()

def clear_results_directory(dir_path: Path):
    """Safely removes and recreates a directory."""
    try:
        if dir_path.exists() and dir_path.is_dir():
            print(f"--- Clearing old results from {dir_path} ---")
            shutil.rmtree(dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"--- Created empty directory {dir_path} ---")

    except OSError as e:
        print(f"Error clearing directory {dir_path}: {e}")
        print("Please check file permissions and if any files are in use.")


def periodic_clear(dir_path: Path, interval_s: int = 60):
    """Runs in background and clears directory every `interval_s` seconds."""
    while True:
        time.sleep(interval_s)
        clear_results_directory(dir_path)

class DreamerZmqEnv(gym.Env):
    """
    A simplified ZMQ Env for DreamerV3.
    - Returns unstacked observations (shape (FEAT_DIM,)).
    - Removes all action masking logic.
    - Includes dummy "image" key for Dreamer's video rendering.
    """
    metadata = {"render_modes": []}

    def __init__(self, action_ep: str, step_ep: str, bitrate_ladder_mbps: list):
        super().__init__()
        
        self.bitrate_ladder = bitrate_ladder_mbps
        self.n_actions = len(bitrate_ladder_mbps)
        
        # Action/Observation space definitions
        self.action_space = spaces.Discrete(self.n_actions)
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, 
            shape=OBSERVATION_SHAPE, # Use (14,)
            dtype=np.float32
        )
        
        self.ctx = zmq.Context()
        self.action_socket = self.ctx.socket(zmq.ROUTER)
        self.action_socket.bind(action_ep)
        self.transition_socket = self.ctx.socket(zmq.PULL)
        self.transition_socket.bind(step_ep)
        
        self.ep_return = 0.0
        self.ep_len = 0
        self.pending_obs_info = None
        self.last_obs_for_done = np.zeros(OBSERVATION_SHAPE, dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self.ep_return = 0.0
        self.ep_len = 0
        self.pending_obs_info = None
        
        try:
            parts = self.action_socket.recv_multipart()
            identity, _, obs_msg = parts if len(parts) == 3 else (parts[0], b'', parts[1])
            
            obs_request = json.loads(obs_msg)
            
            # Robustly get payload
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            
            flat_obs, meta = self._parse_obs_payload(obs_payload)
            
            self.pending_obs_info = (identity, flat_obs, meta)
            self.last_obs_for_done = flat_obs.copy()
            
            info = {"obs_meta": meta}
            
            # FIX: Return both "vector" and "image" keys
            # Create a dummy 64x64 RGB image for visualization
            dummy_image = np.zeros((64, 64, 3), dtype=np.uint8)
            
            return {
                "vector": flat_obs,
                "image": dummy_image
            }
            
        except Exception as e:
            print(f"❌ Error during reset: {e}")
            raise

    def step(self, action):
        action_idx = int(np.argmax(action['action']))
        bitrate_mbps = self.bitrate_ladder[action_idx]
        
        if self.pending_obs_info is None:
            raise RuntimeError("step() called before reset()")
        
        identity, _, current_meta = self.pending_obs_info
        self.pending_obs_info = None
        
        action_response = {"bitrate_mbps": float(bitrate_mbps)}
        
        try:
            msg = [identity, b'', json.dumps(action_response).encode('utf-8')]
            self.action_socket.send_multipart(msg)
        except zmq.ZMQError as e:
            print(f"❌ Error sending action: {e}")
            raise
        
        try:
            transition = self.transition_socket.recv_json()
        except zmq.ZMQError as e:
            print(f"❌ Error receiving transition: {e}")
            raise
        
        reward = float(transition["reward"])
        done = bool(transition["done"])
        truncated = False
        
        self.ep_return += reward
        self.ep_len += 1
        
        if done:
            next_obs = self.last_obs_for_done
            next_meta = current_meta
            self.pending_obs_info = None
        else:
            parts = self.action_socket.recv_multipart()
            identity, _, obs_msg = parts if len(parts) == 3 else (parts[0], b'', parts[1])
            obs_request = json.loads(obs_msg)
            
            # Robustly get payload
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            
            next_obs, next_meta = self._parse_obs_payload(obs_payload)
            
            self.pending_obs_info = (identity, next_obs, next_meta)
            self.last_obs_for_done = next_obs.copy()
        
        info = {"obs_meta": next_meta}
        
        # FIX: Return both "vector" and "image" keys
        dummy_image = np.zeros((64, 64, 3), dtype=np.uint8)
        
        return {
            "vector": next_obs,
            "image": dummy_image
        }, reward, done, info

    def close(self):
        self.action_socket.close()
        self.transition_socket.close()
        self.ctx.term()

    @staticmethod
    def _parse_obs_payload(payload):
        """
        Robust parser - finds the observation array even if nested
        """
        flat = None
        if isinstance(payload, (list, np.ndarray)):
            flat = np.asarray(payload, dtype=np.float32).ravel()
        elif "obs_flat" in payload:
            flat = np.asarray(payload["obs_flat"], dtype=np.float32).ravel()
        elif "obs" in payload:
            flat = np.asarray(payload["obs"], dtype=np.float32).ravel()
        else:
            raise KeyError(f"Cannot parse obs payload: {payload.keys()}")

        # Get only the last FEAT_DIM features from the stacked vector
        unstacked_obs = flat[-FEAT_DIM:]
        
        if unstacked_obs.shape[0] != FEAT_DIM:
             raise ValueError(f"Parsed wrong shape! Expected {FEAT_DIM}, Got {unstacked_obs.shape}")

        meta = dict(feat_dim=FEAT_DIM)
        return unstacked_obs.astype(np.float32, copy=False), meta
def make_custom_env_fn(config, mode='train', id=0):
    """
    This function will be called by the Dreamer trainer
    to create an instance of your ZMQ environment.
    
    --- MODIFIED TO USE A SINGLETON WRAPPER ---
    """
    
    global _ENV_CACHE
    
    # Import wrappers
    from envs.wrappers import TimeLimit
    import gym  # Old gym that DreamerV3 expects

    # --- FIX: Wrapper to add the .id attribute ---
    # Dreamer's tools.simulate requires env.id, but we must use a
    # SINGLETON instance of DreamerZmqEnv because it binds sockets.
    # This wrapper adds the .id to a *new* object that *points*
    # to the single, underlying environment.
    class IdWrapper(gym.Wrapper):
        def __init__(self, env, env_id):
            super().__init__(env)
            self.id = str(env_id) # The required attribute
    
    # --- FIX: Use a singleton cache key ---
    # All envs (id=0, id=1, ...) must share the same base env
    singleton_key = 'singleton_zmq_env'

    # --- Create the singleton instance ONCE ---
    if singleton_key not in _ENV_CACHE:
        # --- If not cached, this is the first call ---
        print(f"🔧 Creating SINGLETON ZMQ environment (for mode={mode}, id={id})")
        
        # Use the endpoints set by train_dreamer_main
        global _CURRENT_ACTION_EP, _CURRENT_STEP_EP
        
        if _CURRENT_ACTION_EP is None or _CURRENT_STEP_EP is None:
            raise RuntimeError("Endpoints not set! Call train_dreamer_main first.")
        
        # Create your base ZMQ environment
        env = DreamerZmqEnv(
            action_ep=_CURRENT_ACTION_EP,
            step_ep=_CURRENT_STEP_EP,
            bitrate_ladder_mbps=BITRATE_LADDER_MBPS
        )
        
        # Convert gymnasium spaces to gym spaces for compatibility
        env.action_space = gym.spaces.Discrete(env.action_space.n)
        env.observation_space = gym.spaces.Box(
            low=env.observation_space.low,
            high=env.observation_space.high,
            shape=env.observation_space.shape,
            dtype=env.observation_space.dtype
        )
        
        # Apply time limit wrapper
        env = TimeLimit(env, config.time_limit)
        
        print(f"✅ Created wrapped singleton environment")
        print(f"   Action space: {env.action_space}")
        print(f"   Observation space: {env.observation_space}")
        
        # --- Add the new env to the cache before returning ---
        _ENV_CACHE[singleton_key] = env
    
    # --- Return a new IdWrapper around the singleton ---
    # Every call (for id=0, id=1, etc.) gets the same base env
    # but wrapped in a *new* object that has the correct .id
    print(f"🔧 Wrapping singleton ZMQ env for (mode={mode}, id={id})")
    base_env = _ENV_CACHE[singleton_key]
    return IdWrapper(base_env, id)

def train_dreamer_main(action_ep, step_ep):
    """
    This is the main "target" function for your multiprocessing.Process.
    """
    # Set the global endpoints so make_custom_env_fn can use them
    global _CURRENT_ACTION_EP, _CURRENT_STEP_EP
    _CURRENT_ACTION_EP = action_ep
    _CURRENT_STEP_EP = step_ep
    
    print(f"🚀 DreamerV3 Learner started on:")
    print(f"\tAction EP: {action_ep}")
    print(f"\tStep EP:   {step_ep}")

    # --- 1. Change CWD to the dreamer repo ---
    original_cwd = os.getcwd()
    os.chdir(DREAMER_REPO_PATH)
    
    # Import AFTER changing directory
    sys.path.insert(0, DREAMER_REPO_PATH)
    from dreamer import main as dreamer_train_main_fn
    import dreamer as dreamer_module
    
    # --- NEW: Import wandb in this new process ---
    import wandb

    print(f"Changed CWD to: {DREAMER_REPO_PATH}")

    # --- 2. Monkey-patch make_env in the dreamer module ---
    print("Monkey-patching dreamer.make_env...")
    original_make_env = dreamer_module.make_env
    dreamer_module.make_env = make_custom_env_fn
    
    try:
        # --- 3. Load configs (CORRECTED) ---
        print("Loading base config (configs.yaml)...")
        # OmegaConf.load() automatically merges the 'defaults' section
        # So 'config' is the fully resolved base configuration.

        # In train_dreamer_main:
        config = OmegaConf.load("configs.yaml").defaults
        print("✅ Loaded base config and resolved defaults.")
        
        print("Loading custom_env.yaml...")
        custom_config = OmegaConf.load("custom_env.yaml")
        
        # Merge the custom env settings on top of the *entire* base config
        if "custom_env" in custom_config:
            final_config = OmegaConf.merge(config, custom_config.custom_env)
            print("✅ Merged custom_env configuration")
        else:
            final_config = OmegaConf.merge(config, custom_config)
        
        # Set logdir if None
        if final_config.logdir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_config.logdir = f"~/dreamer_logs/xr_bitrate_{timestamp}"
            print(f"⚠️  logdir was None, set to: {final_config.logdir}")
        
        
        
        final_config.log_video = False
        final_config.video_pred_log = 0
        # --- 4. START WANDB CONFIGURATION (CORRECTED PATHS) ---
        print("Configuring W&B Logger...")

        # --- NEW: Check if 'logger' key exists, and if not, create the structure ---
        if 'logger' not in final_config:
            # Create the necessary dictionary structure for wandb settings
            final_config.logger = OmegaConf.create({'wandb': {}})
            print("⚠️ Created missing 'logger' and 'wandb' structure.")
        elif 'wandb' not in final_config.logger:
            # Create the necessary dictionary structure for wandb settings
            final_config.logger.wandb = OmegaConf.create({})
            print("⚠️ Created missing 'wandb' structure under 'logger'.")
        
        
        # --- Set your W&B project details FIRST ---
        # These lines will now work because the structure is guaranteed to exist.
        
        # The project to log to
        final_config.logger.wandb.project = 'dreamerv3_xr_abr'
        
        # A group name for this entire run (all combos)
        final_config.logger.wandb.group = f'run_{os.getpid()}' 
        
        # A unique name for this specific learner process
        final_config.logger.wandb.name = f'dreamer_learner_{os.getpid()}'
        
        # (Optional) Add tags
        final_config.logger.wandb.tags = ['dreamerv3', 'xr_abr', 'zmq_env']

        print(f"Logging to W&B project: {final_config.logger.wandb.project}")
        
        # This tells DreamerV3 to use the 'wandb' logger
        # SET THIS LAST after customizing the logger structure
        final_config.logger = 'wandb'
        # --- END WANDB CONFIGURATION ---
        
        
        print(f"Final config - Task: {final_config.task}, Steps: {final_config.steps}, Seed: {final_config.seed}")
        print(f"Logs will be saved to: {final_config.logdir}")
        
        # --- 5. Start training ---
        print("Starting DreamerV3 training loop...")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            dreamer_train_main_fn(final_config)  # Note: renamed to avoid name collision
    
    except Exception as e:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!!!!!!!!! ERROR IN DREAMER LEARNER PROCESS !!!!!!!!!!!")
        print(f"Exception: {e}")
        import traceback
        traceback.print_exc()
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    
    finally:
        # Restore original make_env
        dreamer_module.make_env = original_make_env
        os.chdir(original_cwd)
        print("DreamerV3 training loop finished.")


def main():
    print(f"DreamerV3 start:")
    # === Build all parameter combinations ===
    results_dir = Path("Results")
    # clear_results_directory(results_dir) # Uncomment if you have this function

    # Start background thread (daemon so it ends with main process)
    # thread = threading.Thread(target=periodic_clear, args=(results_dir, 60), daemon=True) # Uncomment if you have this
    # thread.start()
    combos = list(product(
        simTime,TEST_TYPE, N_BGs, N_XR, IS_UL_BG, initial_bitrate_mbps,
        video_samples, fps_list, num_close_users, distance_close_users,
        RANDOM_SEEDS, distance_list, GoP_sizes, intrarefresh_choice,
        ABR_ENABLED, nest_profiles, rate_bps_src_BG, PL, 
    ))

    random.shuffle(combos)
    print(f"***********************************\n************NUMBER OF COMBOS: {len(combos)}   ***********")
    
    rebuild_rust_binary(EXAMPLE_NAME) # Uncomment if you have this
    exe = find_exe(release=True)      # Uncomment if you have this
    
    # --- MOCK EXE for testing without Rust ---
    # exe = Path("echo") # Use a dummy executable for testing
    # print("WARNING: Using 'echo' as a mock executable. Re-enable `find_exe` for real runs.")
    
    train_over_all_combos_iter(exe, combos)

    print("🧹 All episodes finished.")

if __name__ == "__main__":
    
    atexit.register(cleanup_rust_processes) # Uncomment if you have this

    # Handle Ctrl-C / SIGTERM gracefully
    signal.signal(signal.SIGINT, lambda sig, frame: (print("\n[CTRL-C] stopping…"), cleanup_rust_processes(), exit(0)))
    signal.signal(signal.SIGTERM, lambda sig, frame: (print("\n[SIGTERM] stopping…"), cleanup_rust_processes(), exit(0)))    
    
    wandb.login()
    main()