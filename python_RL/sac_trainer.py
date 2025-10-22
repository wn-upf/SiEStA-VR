#sac_trainer.py
from stable_baselines3 import SAC
# Continuous action bounds in Mbps (adjust to your ladder / encoder limits)

import os
import time
import json
import pprint
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import zmq
from sb3_contrib import RecurrentPPO


from pathlib import Path
from itertools import product
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import random
import subprocess
import sys
import pprint
# --- Stable Baselines 3 Imports ---
from stable_baselines3 import PPO, DQN, A2C
import argparse

# --- W&B Imports ---
import wandb
from wandb.integration.sb3 import WandbCallback

# --- Environment Setup (No changes needed here) ---
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
from absl import logging as absl_logging
import os
from stable_baselines3.common.torch_layers import BaseFeaturesExtractor
import torch as th
from gymnasium import spaces

import torch.nn as nn

from stable_baselines3.common.callbacks import CallbackList


import subprocess
import signal
import atexit
import os
import shutil

# Keep global list of Rust child processes
RUST_PROCS = []

#CONSTS
##############################

N_STEPS_RL= 7_500_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   
FEAT_DIM = 14
WINDOW_LEN = 5
OBSERVATION_SHAPE = (WINDOW_LEN * FEAT_DIM, )

ACTION_DIM = 20

ACT_MIN_MBPS = 5.0
ACT_MAX_MBPS = 100.0



policy_ppo_a2c = "MlpPolicy"  # shared by PPO and A2C

ACTION_ENDPOINT  = os.environ.get("ZMQ_ACTION_EP",  "ipc:///tmp/xr_default_action")
STEP_ENDPOINT    = os.environ.get("ZMQ_STEP_EP",    "ipc:///tmp/xr_default_step")
TRAINER_ENDPOINT = os.environ.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")


#################################################
### SIMULATION PARAMS
TEST_TYPE = [ "STD"]                     # "BW", "JI", "PL", "RANDOM", "STD"

simTime = [60.0]

k_queue = 10000
mean_length_BG = 12000.0
rate_bps_src_BG = [10e6, 20e6, 40e6]
distance_list = [1.5]
distance_close_users = [1.5]
num_close_users = [0]
N_XR = [1, 2, 3, 4]
PL = [0.0001, 0.01, 0.1, 0.15]
fps_list = [60.0, 90.0, 120.0 ]
initial_bitrate_mbps = [10.0, 20.0, 40.0]
ABR_ENABLED = [3]
nest_profiles = [1]
RANDOM_SEEDS = list(range(1, 11))
video_samples = ["snow"]
N_BGs = [0]
IS_UL_BG = [0]
intrarefresh_choice = [1]
GoP_sizes = [90]
everest_tests = 1

###############################3
from stable_baselines3.common.callbacks import BaseCallback

class StuckActionEarlyStop(BaseCallback):
    """
    Stop training early if the chosen action doesn't change for N consecutive episodes.
    """
    def __init__(self, patience_episodes=200_000, verbose=1):
        super().__init__(verbose)
        self.patience_episodes = patience_episodes
        self.last_action = None
        self.same_action_count = 0
        self.last_episode = 0

    def _on_step(self) -> bool:
        # Called at every environment step
        if "train/action" in self.locals:
            current_action = int(self.locals["train/action"])
        elif "actions" in self.locals:
            # fallback if recorded under different key
            current_action = int(np.mean(self.locals["actions"]))
        else:
            return True  # nothing to do yet

        if self.last_action is None:
            self.last_action = current_action
            return True

        if current_action == self.last_action:
            self.same_action_count += 1
        else:
            self.same_action_count = 0
            self.last_action = current_action

        # Optional progress print
        if self.verbose > 0 and self.same_action_count % 10_000 == 0 and self.same_action_count > 0:
            print(f"[EarlyStop] Action {current_action} repeated {self.same_action_count} times...")

        # Early stop condition
        if self.same_action_count > self.patience_episodes:
            print(f"\n🛑 Early stopping: same action repeated {self.same_action_count} times.")
            return False  # returning False halts training

        return True

def train_sac_single(trainer_ep: str):
    run = wandb.init(
        project=os.environ.get("WANDB_PROJECT", "xr-abr"),
        entity=os.environ.get("WANDB_ENTITY"),
        save_code=True,
        # config=dict(
        #     algo="SAC",
        #     learning_rate=3e-4,
        #     gamma=0.99,
        #     tau=0.02,
        #     buffer_size=500_000,
        #     batch_size=256,
        #     train_freq=1,           # grad step every env step
        #     gradient_steps=1,       # 1 grad step per call
        #     ent_coef="auto",
        #     net_arch=[256, 256],
        #     use_vectorized_obs=True,
        # ),
        # name = f"SAC_{'-'.join(map(str, wandb.config.net_arch))}",
        # sync_tensorboard=True, # Recommended for SB3

    )

    use_vec = wandb.config.get("use_vectorized_obs", True)
    if use_vec:
        print(f"{Colors.BLUE}Using windowed observations (last row via extractor).{Colors.ENDC}")
        env = ZmqEnvClientVEC_Continuous(trainer_ep)    
        policy = "MlpPolicy"
        policy_kwargs = dict(
            features_extractor_class=LastRowExtractor,
            net_arch=list(wandb.config.net_arch),        )
    else:
        print(f"{Colors.BLUE}Using single-frame observations.{Colors.ENDC}")
        env = ZmqEnvClient(trainer_ep)
        policy = "MlpPolicy"
        policy_kwargs = dict(net_arch=list(wandb.config.net_arch))

    model = SAC(
        policy,
        env,
        learning_rate=wandb.config.learning_rate,
        gamma=wandb.config.gamma, # This will be pulled from sweep config
        tau=wandb.config.tau,
        buffer_size= 1_000_000,   # This is hardcoded, not from sweep
        learning_starts=10_000, 
        batch_size=wandb.config.batch_size,
        train_freq=wandb.config.train_freq,    # ("step") implied
        gradient_steps=wandb.config.gradient_steps,
        ent_coef=wandb.config.ent_coef,
        policy_kwargs=policy_kwargs,
        verbose=1,
        tensorboard_log=f"runs/{run.id}", # Added for WandbCallback
    )

    callback =  WandbCallback(
                        model_save_path=f"models/{run.id}",
                        model_save_freq=50_000,
                        verbose=2,
                        log="all",
                        )

    # callback =  CallbackList([ 
    #                 WandbCallback(
    #                     model_save_path=f"models/{run.id}",
    #                     model_save_freq=50_000,
    #                     verbose=2,
    #                     log="all",
    #                     ),
    #                 StuckActionEarlyStop(patience_episodes = N_STEPS_EARLY_STOP, verbose=1)
    #                 ]
    #             )

    model.learn(total_timesteps=N_STEPS_RL, callback=callback)

    final_model_path = f"models/{run.id}/final_model.zip"
    model.save(final_model_path)
    art = wandb.Artifact(
        name=f"SAC-{run.id}-final",
        type="model",
        description=f"Final SAC after {N_STEPS_RL} steps"
    )
    art.add_file(final_model_path)
    run.log_artifact(art)
    wandb.finish()



################# LIBS ################## 

absl_logging.set_verbosity(absl_logging.ERROR)
class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'


def _obs_from_payload_dict(d):
    if "obs_flat" in d:
        return d["obs_flat"]
    if "obs" in d:
        return d["obs"]
    raise KeyError("Neither 'obs_flat' nor 'obs' in payload")


OBSERVATION_KEYS = [
    "t_elapsed_s",
    "last_target_bitrate_mbps",
    "rtt_ms_avg_s",
    "rtt_ms_std_s",
    "bandwidth_mbps_avg_s",
    "bandwidth_mbps_std_s",
    "frame_interarrival_avg_ms",
    "frame_interarrival_std_ms",
    "flr_avg_s",
    "buffer_level_avg_s",
    "rebuffer_event_sum", 
]


def coerce_batch_size(n_steps: int, batch_size: int, n_envs: int = 1) -> int:
    total = n_steps * n_envs
    if total % batch_size == 0:
        return batch_size
    # pick the largest divisor of total that is <= batch_size
    for b in range(min(batch_size, total), 0, -1):
        if total % b == 0:
            return b
    return total  # fallback


class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'

class ZmqServer:
    """A standalone ZMQ server that acts as a bridge between Rust simulations
    and a Python-based RL training agent."""
    
    def __init__(self, action_ep: str, step_ep: str, trainer_ep: str):


        self.ctx = zmq.Context()
        # Sockets for Rust Simulations (ROUTER for req/rep, PULL for one-way data)
        self.router = self.ctx.socket(zmq.ROUTER)
        self.router.bind(action_ep)
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(step_ep)
        
        # Socket for Python Trainer Client (REP for req/rep)
        self.rep_socket = self.ctx.socket(zmq.REP)
        self.rep_socket.bind(trainer_ep)

        self.sim_is_done = False 
        
        self.active_sim_id = None
        print(f"{Colors.GREEN}✅ ZMQ Server Bridge is running.{Colors.ENDC}")
        print(f"📡 Listening for Rust sims on {action_ep} and {step_ep}")
        print(f"🤖 Listening for Python trainer on {trainer_ep}")

    def _obs_from_json(self, obs_json):
        # --- FIX ---
        # The observation from Rust is now a pre-ordered list (Vec<f32>).
        # We no longer need to sort it by key. We just return it as is.
        # This resolves the TypeError.
        if isinstance(obs_json, list):
            return obs_json
        # Fallback for old dictionary-based observations
        print(f"{Colors.YELLOW}Warning: Received a dictionary-based observation. Consider updating all clients.{Colors.ENDC}")
        return [obs_json[k] for k in sorted(obs_json)]


    def run_forever(self):
        """Main server loop."""
        while True:
            # Wait for a command from the training agent ('reset' or 'step')
            # print(f"\n{Colors.YELLOW}SERVER: Waiting for command from trainer...{Colors.ENDC}")
            req = self.rep_socket.recv_json()
            command = req.get("command")
            
            if command == "reset":
                print(f"{Colors.BLUE}SERVER: Received 'reset' command.{Colors.ENDC}")
                # 1) Get the first obs from a connecting Rust sim
                print("SERVER: Waiting for initial observation from a Rust simulation...")
                sim_id, payload = self.router.recv_multipart()
                req_obs = json.loads(payload.decode("utf-8"))
                initial_obs = _obs_from_payload_dict(req_obs)

                self.active_sim_id = sim_id

                # 2) Send a dummy *continuous* action to unblock Rust
                init_bitrate = float(os.environ.get("INIT_BITRATE_MBPS", ACT_MIN_MBPS))
                self.router.send_multipart([
                    self.active_sim_id,
                    json.dumps({"bitrate_mbps": init_bitrate}).encode("utf-8")
                ])

                # 3) Reply to the trainer with the initial observation
                self.rep_socket.send_json({"obs": initial_obs})
                print(f"{Colors.GREEN}SERVER: Reset done for sim {sim_id.decode()}, sent bitrate={init_bitrate:.2f} Mbps.{Colors.ENDC}")


            elif command == "step":
                action = req.get("action")
                if isinstance(action, (list, tuple, np.ndarray)):
                    action = float(np.asarray(action, dtype=np.float32).ravel()[0])
                else:
                    action = float(action)
                action = max(ACT_MIN_MBPS, min(ACT_MAX_MBPS, action))  # clamp

                # 1. First, send the action to the waiting Rust sim.
                #    (Only if the sim isn't already done)
                if not self.sim_is_done:
                    try:
                        # Wait for sim's REQ for the next action
                        sim_id, _ = self.router.recv_multipart()
                        # Send the action
                        self.router.send_multipart([sim_id, json.dumps({"bitrate_mbps": action}).encode("utf-8")])
                    except Exception as e:
                        print(f"SERVER: Error sending action to sim: {e}")
                        # Handle error if needed

                # 2. Now, wait for the sim to PUSH its transition data.
                transition = self.pull.recv_json()
                while self.active_sim_id is not None and transition.get("sim_id") != self.active_sim_id.decode():
                    print(f"SERVER: Skipping transition from {transition.get('sim_id')}, waiting for {self.active_sim_id.decode()}")
                    transition = self.pull.recv_json()

                reward = float(transition["reward"])
                done = bool(transition["done"])
                self.sim_is_done = done  # <-- Store the 'done' state

                next_obs_field = transition.get("next_obs")
                if next_obs_field is None:
                    raise KeyError(f"Transition missing 'next_obs'; got keys: {list(transition.keys())}")

                next_obs = _obs_from_payload_dict(next_obs_field) if isinstance(next_obs_field, dict) else next_obs_field

                # 3. Finally, send the transition data back to the (blocked) agent.
                self.rep_socket.send_json({"next_obs": next_obs, "reward": reward, "done": done})

                if done:
                    print(f"{Colors.YELLOW}SERVER: Episode finished for sim {self.active_sim_id.decode()}.{Colors.ENDC}")
                    self.active_sim_id = None


            
    def close(self):
        """Cleanly close all sockets and terminate the context."""
        self.router.close()
        self.pull.close()
        self.rep_socket.close()
        self.ctx.term()


import threading

def start_zmq_server_thread(env_vars):

    ACTION_ENDPOINT  = env_vars.get("ZMQ_ACTION_EP",  "ipc:///tmp/xr_default_action")
    STEP_ENDPOINT    = env_vars.get("ZMQ_STEP_EP",    "ipc:///tmp/xr_default_step")
    TRAINER_ENDPOINT = env_vars.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")

    print(f"📡 Binding server to:")
    print(f"  ACTION_ENDPOINT  = {ACTION_ENDPOINT}")
    print(f"  STEP_ENDPOINT    = {STEP_ENDPOINT}")
    print(f"  TRAINER_ENDPOINT = {TRAINER_ENDPOINT}")

    server = ZmqServer(ACTION_ENDPOINT, STEP_ENDPOINT, TRAINER_ENDPOINT)
    t = threading.Thread(target=server.run_forever, daemon=True)
    t.start()

    return server, t


class ZmqEnvClientVEC_Continuous(gym.Env):
    """Gym env that talks to your trainer server via ZMQ (REQ/REP)."""
    metadata = {"render_modes": []}

    def __init__(self, trainer_ep):
        super().__init__()
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32
        )
        # Continuous action in Mbps
        self.action_space = spaces.Box(
            low=np.array([ACT_MIN_MBPS], dtype=np.float32),
            high=np.array([ACT_MAX_MBPS], dtype=np.float32),
            dtype=np.float32,
            shape=(1,),
        )

        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(trainer_ep)

        self.step_count = 0
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0

        print(f"✅ Python ZMQ Client connected to server {TRAINER_ENDPOINT}.")
    # ---------- helpers ----------
    @staticmethod
    def _parse_obs_payload(payload):
        """
        Accept:
        - dict with {"obs_flat": [...]} or {"obs": [...]}
        - raw list/ndarray [...], which can be either a full flat window
            or a single-row (FEAT_DIM,) vector -> we left-pad to window.
        Return: (flat_obs: np.ndarray shape (WINDOW_LEN*FEAT_DIM,), meta: dict)
        """
        # --- raw list/ndarray ---
        if isinstance(payload, (list, np.ndarray)):
            raw = np.asarray(payload, dtype=np.float32).ravel()
            feat_dim = FEAT_DIM
            window_len = WINDOW_LEN
            if raw.size == feat_dim:
                # Single row -> left-pad into window
                flat = np.zeros((window_len * feat_dim,), dtype=np.float32)
                flat[-feat_dim:] = raw
                seq_len = 1
            else:
                # Already flat window (or larger): trim/pad to window size
                expect = window_len * feat_dim
                if raw.size < expect:
                    flat = np.pad(raw, (expect - raw.size, 0))
                else:
                    flat = raw[-expect:]
                # heuristic seq_len
                seq_len = min(window_len, max(1, flat.size // feat_dim))
            mask = np.zeros(window_len, dtype=bool)
            mask[-seq_len:] = True
            meta = dict(seq_len=seq_len, feat_dim=feat_dim, window_len=window_len, mask=mask)
            return flat.astype(np.float32, copy=False), meta

        # --- dict with keys ---
        if "obs_flat" in payload:
            flat = np.asarray(payload["obs_flat"], dtype=np.float32).ravel()
            seq_len = int(payload.get("seq_len", WINDOW_LEN))
            feat_dim = int(payload.get("feat_dim", FEAT_DIM))
            window_len = int(payload.get("window_len", WINDOW_LEN))
        elif "obs" in payload:
            raw = np.asarray(payload["obs"], dtype=np.float32).ravel()
            feat_dim = FEAT_DIM
            window_len = WINDOW_LEN
            if raw.size == feat_dim:
                flat = np.zeros((window_len * feat_dim,), dtype=np.float32)
                flat[-feat_dim:] = raw
                seq_len = 1
            else:
                expect = window_len * feat_dim
                flat = raw[-expect:] if raw.size >= expect else np.pad(raw, (expect - raw.size, 0))
                seq_len = min(window_len, max(1, flat.size // feat_dim))
        else:
            raise KeyError("Neither 'obs_flat' nor 'obs' in payload and payload is not a list/ndarray")

        expect = window_len * feat_dim
        if flat.size != expect:
            flat = flat[-expect:] if flat.size > expect else np.pad(flat, (expect - flat.size, 0))
        mask = np.zeros(window_len, dtype=bool)
        mask[-seq_len:] = True
        meta = dict(seq_len=seq_len, feat_dim=feat_dim, window_len=window_len, mask=mask)
        return flat.astype(np.float32, copy=False), meta


    @staticmethod
    def _log_last_row(log_dict, flat_obs):
        """Log only the most recent row for readability."""
        feat_dim = FEAT_DIM
        last_row = flat_obs[-feat_dim:]
        for i, v in enumerate(last_row):
            key = OBSERVATION_KEYS[i] if i < len(OBSERVATION_KEYS) else f"feat_{i}"
            log_dict[f"obs_last/{key}"] = float(v)

    # ---------- gym API ----------
    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        print(f"\n--- Episode boundary ---")
        self.ep_return = 0.0
        self.ep_len = 0

        self.socket.send_json({"command": "reset"})
        t0 = time.time()
        response = self.socket.recv_json()
        recv_latency_ms = (time.time() - t0) * 1000.0

        obs_payload = response.get("obs", response.get("obs_flat", response))
        flat_obs, meta = self._parse_obs_payload(obs_payload)

        # flat_obs, meta = self._parse_obs_payload(response)

        # Optional logging
        if wandb.run is not None:
            wandb.log({
                "env/reset_recv_latency_ms": recv_latency_ms,
                "env/episode": wandb.run.summary.get("episodes", 0) + 1,
                "obs/seq_len": meta["seq_len"],
            })

        info = {"obs_meta": meta}  # expose mask & dims downstream if needed
        return flat_obs, info

    def step(self, action):
        t0 = time.time()
        # instead of int(action)
        self.socket.send_json({"command": "step", "action": float(np.asarray(action).ravel()[0])})

        response = self.socket.recv_json()
        pull_latency_ms = (time.time() - t0) * 1000.0
        obs_payload = response.get("next_obs")
        if obs_payload is None:
            raise KeyError(f"Server step response missing 'next_obs'. Keys: {list(response.keys())}")
        flat_obs, meta = self._parse_obs_payload(obs_payload)

        
        reward = float(response["reward"])
        done = bool(response["done"])
        truncated = False

        # Stats
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward
        self.step_count += 1

        # Logging
        log_dict = {
            "train/reward": reward,
            "train/return_cumsum": self.run_return_cumsum,
            "train/action": float(np.asarray(action).ravel()[0]),
            "train/done": int(done),
            "timing/pull_latency_ms": pull_latency_ms,
            "obs/seq_len": meta["seq_len"],
        }
        # Log the last row only (most recent observation)
        self._log_last_row(log_dict, flat_obs)

        # Any extra scalar fields from sim
        for k, v in response.items():
            if k in ("reward", "done", "obs", "obs_flat", "seq_len", "feat_dim", "window_len"):
                continue
            if isinstance(v, (int, float)):
                log_dict[f"sim/{k}"] = v

        if wandb.run is not None:
            wandb.log(log_dict)

        if done:
            wandb.log({"episode/return": self.ep_return, "episode/len": self.ep_len})
            wandb.run.summary["episodes"] = wandb.run.summary.get("episodes", 0) + 1
            self.ep_return = 0.0
            self.ep_len = 0

        info = {"obs_meta": meta}
        return flat_obs, reward, done, truncated, info

    def close(self):
        self.socket.close()
        self.ctx.term()



class ZmqEnvClient(ZmqEnvClientVEC_Continuous):
    """Simpler env version that only uses the last observation (no history window)."""
    def __init__(self, trainer_ep):
        super().__init__(trainer_ep)
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(FEAT_DIM,), dtype=np.float32
        )

    @staticmethod
    def _parse_obs_payload(payload):
        """Strip history, keep only the last row."""
        if isinstance(payload, (list, np.ndarray)):
            raw = np.asarray(payload, dtype=np.float32).ravel()
            if raw.size > FEAT_DIM:
                raw = raw[-FEAT_DIM:]
            return raw.astype(np.float32, copy=False), dict(seq_len=1)
        elif isinstance(payload, dict) and "obs_flat" in payload:
            raw = np.asarray(payload["obs_flat"], dtype=np.float32)
            return raw[-FEAT_DIM:].astype(np.float32, copy=False), dict(seq_len=1)
        else:
            raise KeyError("Cannot parse obs payload")





class LastRowExtractor(BaseFeaturesExtractor):
    def __init__(self, observation_space: spaces.Box, feat_dim: int = FEAT_DIM):
        super().__init__(observation_space, features_dim=feat_dim)
        self.feat_dim = feat_dim

    def forward(self, obs: th.Tensor) -> th.Tensor:
        # obs: [B, WINDOW_LEN*FEAT_DIM] → return last row: [B, FEAT_DIM]
        return obs[:, -self.feat_dim:]


def train_over_all_combos_iter(exe: Path, combos, num_passes: int = 10):
    """
    Run multiple shuffled passes over all simulation combos.
    """
    # ---- Fixed endpoints for the entire run ----
    base_id = os.environ.get("SLURM_JOB_ID") or os.getpid()
    RUN_ID = f"{base_id}_train"

    action_ep  = f"ipc:///tmp/xr_{RUN_ID}_action"
    step_ep    = f"ipc:///tmp/xr_{RUN_ID}_step"
    trainer_ep = f"ipc:///tmp/xr_{RUN_ID}_trainer"

    # ---- Start the shared ZMQ server ----
    env_server = {
        "ZMQ_ACTION_EP":  action_ep,
        "ZMQ_STEP_EP":    step_ep,
        "ZMQ_TRAINER_EP": trainer_ep,
    }
    server, thread = start_zmq_server_thread(env_server)
    time.sleep(0.5)

    # ---- Start RL thread (same endpoints for all episodes) ----
    pool = ThreadPoolExecutor(max_workers=2)
    fut_rl = pool.submit(train_sac_single, trainer_ep)
    print(f"RL loop started on {trainer_ep}")

    time.sleep(3.0)




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
                "1", f"{sim_count}"
            ]

            env_sim = os.environ.copy()
            env_sim["ZMQ_ACTION_EP"]  = action_ep
            env_sim["ZMQ_STEP_EP"]    = step_ep
            env_sim["ZMQ_TRAINER_EP"] = trainer_ep
            env_sim["WANDB_RUN_GROUP"] = f"pass_{pass_idx}_episode_{sim_count}"

            log_path = Path("Results") / f"pass_{pass_idx}_combo_{sim_count}" / "sim.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)

            print(f"\n🚀 Pass {pass_idx}: launching combo {sim_count}/{len(combos)}")
            ret = run_sim(exe, argv, env_sim, log_path)
            print(f"✅ Pass {pass_idx}, combo {sim_count}: simulator exited with {ret}")

        print(f"🎯 Finished pass {pass_idx}/{num_passes}")

    print("🧹 All passes done — waiting for RL to finish or reach its timestep limit.")
    server.close()
    pool.shutdown(wait=False)



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


def run_episode(exe: Path, sim_args: list[str], sim_count: int):
    """Launch one Rust simulator + RL training pair for a given configuration."""
    slurm_id = os.environ.get("SLURM_JOB_ID")
    pid = os.getpid()
    rand = random.randint(1000, 9999)
    RUN_ID = f"{slurm_id or pid}_{sim_count}_{rand}"

    env = os.environ.copy()
    env["RUN_ID"] = RUN_ID
    env["ZMQ_ACTION_EP"]  = f"ipc:///tmp/xr_{RUN_ID}_action"
    env["ZMQ_STEP_EP"]    = f"ipc:///tmp/xr_{RUN_ID}_step"
    env["ZMQ_TRAINER_EP"] = f"ipc:///tmp/xr_{RUN_ID}_trainer"
    env["WANDB_RUN_GROUP"] = f"sim_{RUN_ID}"

    log_path = Path("Results") / f"episode_{RUN_ID}" / "sim.log"
    print(f"\n🚀 Launching episode {sim_count} with RUN_ID={RUN_ID}")
    print(f"ZMQ endpoints:\n  ACTION={env['ZMQ_ACTION_EP']}\n  STEP={env['ZMQ_STEP_EP']}\n  TRAINER={env['ZMQ_TRAINER_EP']}")

    # --- Launch simulator + RL concurrently ---
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_sim = pool.submit(run_sim, exe, sim_args, env, log_path)
        time.sleep(2.0)  # let sockets bind
        fut_rl = pool.submit(train_sweep_vec)
        code_sim = fut_sim.result()
        code_rl = fut_rl.result()
        print(f"✅ Episode {sim_count} done (sim_exit={code_sim}, rl_exit={code_rl})")



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


def main():


    # === Build all parameter combinations ===
    # TEST_TYPE = ["STD", "BW", "RANDOM"]                     # "BW", "JI", "PL", "RANDOM", "STD"

    results_dir = Path("Results")
    clear_results_directory(results_dir)

    # Start background thread (daemon so it ends with main process)
    thread = threading.Thread(target=periodic_clear, args=(results_dir, 60), daemon=True)
    thread.start()


    combos = list(product(
        simTime,TEST_TYPE, N_BGs, N_XR, IS_UL_BG, initial_bitrate_mbps,
        video_samples, fps_list, num_close_users, distance_close_users,
        RANDOM_SEEDS, distance_list, GoP_sizes, intrarefresh_choice,
        ABR_ENABLED, nest_profiles, rate_bps_src_BG, PL,
    ))

    random.shuffle(combos)  # optional

    print(f"***********************************\n************NUMBER OF COMBOS: {len(combos)}   ***********")
    
    # rebuild_rust_binary(EXAMPLE_NAME)
    exe = find_exe(release=True)
    train_over_all_combos_iter(exe, combos)

    # === 4️⃣ Close ZMQ server ===
    server.close()
    print("🧹 All episodes finished. Server closed.")

if __name__ == "__main__":

        
    atexit.register(cleanup_rust_processes)

    # Handle Ctrl-C / SIGTERM gracefully
    signal.signal(signal.SIGINT, lambda sig, frame: (print("\n[CTRL-C] stopping…"), cleanup_rust_processes(), exit(0)))
    signal.signal(signal.SIGTERM, lambda sig, frame: (print("\n[SIGTERM] stopping…"), cleanup_rust_processes(), exit(0)))    
    
    main()

