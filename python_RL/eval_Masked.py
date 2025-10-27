# Create compatibility shim: map old numpy.core -> numpy._core
import numpy as np
import sys
import types

import numpy as np, sys, types

# # --- Create dummy submodules expected by old pickled models ---
# --- Create dummy submodules expected by old pickled models ---
core_mod = types.SimpleNamespace()
multiarray_mod = types.SimpleNamespace()
numeric_mod = types.SimpleNamespace()

# Register them in sys.modules so unpickling finds them
sys.modules.setdefault("numpy.core", core_mod)
sys.modules.setdefault("numpy.core.multiarray", multiarray_mod)
sys.modules.setdefault("numpy.core.numeric", numeric_mod)

# --- Define old NumPy attributes expected by Stable-Baselines3 pickles ---
if not hasattr(np, "inexact"):
    np.inexact = np.floating
if not hasattr(np, "complexfloating"):
    np.complexfloating = np.complex64.__mro__[-2]
if not hasattr(np, "bool8"):
    np.bool8 = np.bool_

# Mirror these attributes into the fake modules for legacy pickles
for m in (core_mod, multiarray_mod, numeric_mod):
    m.inexact = np.inexact
    m.complexfloating = np.complexfloating
    m.bool8 = np.bool8
    
from pathlib import Path

from multiprocessing import Pool, cpu_count
############################################################
# RL CONFIG: 
N_STEPS_RL= 10_000_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   
FEAT_DIM = 14
WINDOW_LEN = 5
OBSERVATION_SHAPE = (WINDOW_LEN * FEAT_DIM, )

ACTION_DIM = 20
ACT_MIN_MBPS = 5.0
ACT_MAX_MBPS = 100.0
BITRATE_LADDER_MBPS = list(range(5, 101, 5))

policy_ppo_a2c = "MlpPolicy"  # shared by PPO and A2C

#### RL INPUT ARGS (RUST)
#################################################

observation_type = 1 ## 0-> Raw unscaled obs, 1 -> Scaled in expected bounds, 2-> Running Normalization. 
reward_mode = 0 ## normalized reward.  // 0-> naive , 1-> normalized, 2-> ??? todo shaping. 
T_ABR = 0.3 ## update every T seconds. With lower value, more frequent steps in simulation but noisier updates. 


from stable_baselines3 import SAC, TD3
import os
import time
import json
import pprint
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import zmq
from sb3_contrib import RecurrentPPO

# from sb3_contrib import RecurrentSAC
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3.common.noise import NormalActionNoise  # <-- ADD THIS
from stable_baselines3.common.type_aliases import Schedule
from typing import Any, Dict, List, Optional, Tuple, Type, Union

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

import multiprocessing as mp

# --- W&B Imports ---
import wandb
from wandb.integration.sb3 import WandbCallback

# --- Environment Setup (No changes needed here) ---
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
from absl import logging as absl_logging
import os
from stable_baselines3.common.torch_layers import BaseFeaturesExtractor
import torch
import torch.nn as nn
import torch as th
from gymnasium import spaces


from stable_baselines3.common.callbacks import CallbackList

from sb3_contrib import MaskablePPO
from sb3_contrib.common.wrappers import ActionMasker
from stable_baselines3.common.callbacks import BaseCallback
from sb3_contrib.common.maskable.policies import MaskableActorCriticPolicy
from stable_baselines3.common.policies import ActorCriticPolicy

import subprocess
import signal
import atexit
import os
import shutil
    # Keep global list of Rust child processes
RUST_PROCS = []

#CONSTS
##############################
USE_WANDB=False
# Model Configuration
WANDB_ENTITY = "wn-upf"
WANDB_PROJECT = "asynchronix-python_RL"
MODEL_ARTIFACT = "run-o1m22zqz-history:v3"  # Your trained model

# Environment Configuration
ACTION_ENDPOINT = os.environ.get("ZMQ_ACTION_EP", "ipc:///tmp/xr_eval_action")
STEP_ENDPOINT = os.environ.get("ZMQ_STEP_EP", "ipc:///tmp/xr_eval_step")



################################################

###############################3

absl_logging.set_verbosity(absl_logging.ERROR)
class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    MAGENTA = '\033[95m'
    RED = '\033[91m'
    ENDC = '\033[0m'

    CYAN    = '\033[36m'
    WHITE   = '\033[37m'

    # Bright (light) colors
    LIGHT_BLACK   = '\033[90m'
    LIGHT_RED     = '\033[91m'
    LIGHT_GREEN   = '\033[92m'
    LIGHT_YELLOW  = '\033[93m'
    LIGHT_BLUE    = '\033[94m'
    LIGHT_MAGENTA = '\033[95m'
    LIGHT_CYAN    = '\033[96m'
    LIGHT_WHITE   = '\033[97m'

    # Background colors
    BG_BLACK   = '\033[40m'
    BG_RED     = '\033[41m'
    BG_GREEN   = '\033[42m'
    BG_YELLOW  = '\033[43m'
    BG_BLUE    = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN    = '\033[46m'
    BG_WHITE   = '\033[47m'

    # Bright backgrounds
    BG_LIGHT_BLACK   = '\033[100m'
    BG_LIGHT_RED     = '\033[101m'
    BG_LIGHT_GREEN   = '\033[102m'
    BG_LIGHT_YELLOW  = '\033[103m'
    BG_LIGHT_BLUE    = '\033[104m'
    BG_LIGHT_MAGENTA = '\033[105m'
    BG_LIGHT_CYAN    = '\033[106m'
    BG_LIGHT_WHITE   = '\033[107m'

    # Style modifiers
    BOLD      = '\033[1m'
    DIM       = '\033[2m'
    ITALIC    = '\033[3m'
    UNDERLINE = '\033[4m'
    BLINK     = '\033[5m'
    INVERT    = '\033[7m'



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
    "pl_sum_period",
    "frame_size_avg_bytes",        
    "ow_delay_period_ewma",        
    "f_ow_delay_period_ewma",      
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



class DropoutActorCriticPolicy(MaskableActorCriticPolicy):
    """
    A MaskableActorCriticPolicy that adds Dropout layers to the
    policy and value networks.
    """
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        lr_schedule: Schedule,
        *args,
        **kwargs,
    ):
        # --- Solution Step 1: Pop the custom argument ---
        # Pop 'dropout_p' from kwargs before passing them to the parent.
        # Provide a default value (e.g., 0.0) if it's not passed.
        self.dropout_p = kwargs.pop("dropout_p", 0.0)
        
        # --- Solution Step 2: Call the parent constructor ---
        # Now kwargs no longer contains 'dropout_p', so this call is safe.
        super().__init__(
            observation_space,
            action_space,
            lr_schedule,
            *args,
            **kwargs,
        )

    def _build_mlp_extractor(self) -> None:
        """
        Builds the MLP extractor and adds dropout layers.
        This method is called by the parent's __init__ method.
        """
        # Build the standard MLP extractor first
        super()._build_mlp_extractor()
        
        # --- Solution Step 3: Use the stored dropout value ---
        if self.dropout_p > 0.0:
            # Add dropout to the policy network
            self.mlp_extractor.policy_net = nn.Sequential(
                *(list(self.mlp_extractor.policy_net.children()) + [nn.Dropout(p=self.dropout_p)])
            )
            # Add dropout to the value network
            self.mlp_extractor.value_net = nn.Sequential(
                *(list(self.mlp_extractor.value_net.children()) + [nn.Dropout(p=self.dropout_p)])
            )

class MaskableDiscreteZmqEnv(gym.Env):
    """
    Modified environment for Maskable PPO with discrete actions.
    Each action corresponds to a specific bitrate in BITRATE_LADDER_MBPS.
    """
    metadata = {"render_modes": []}

    def __init__(self, action_ep: str, step_ep: str, bitrate_ladder_mbps: list,
                 expansion_strategy='immediate_neighbors', expansion_param=None):
        super().__init__()
        
        self.bitrate_ladder = bitrate_ladder_mbps
        self.n_actions = len(bitrate_ladder_mbps)
        self.expansion_strategy = expansion_strategy
        self.expansion_param = expansion_param
        
        # CHANGE: Discrete action space instead of continuous Box
        self.action_space = spaces.Discrete(self.n_actions)
        
        # Observation space remains the same
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(WINDOW_LEN * FEAT_DIM,), dtype=np.float32
        )
        
        # ZMQ setup (same as before)
        self.ctx = zmq.Context()
        self.action_socket = self.ctx.socket(zmq.ROUTER)
        self.action_socket.bind(action_ep)
        self.transition_socket = self.ctx.socket(zmq.PULL)
        self.transition_socket.bind(step_ep)
        
        # Episode tracking
        self.step_count = 0
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0
        self.pending_obs_info = None
        self.last_obs_for_done = np.zeros((WINDOW_LEN * FEAT_DIM,), dtype=np.float32)
        
        # Store current action mask (binary array for each action)
        self.current_action_mask = None
        
        print(f"{Colors.MAGENTA}[Env] Using mask expansion strategy: {expansion_strategy}{Colors.ENDC}")

    def _convert_rust_mask_to_action_mask(self, rust_mask, expand_neighbors=True):
        """
        Convert the mask from Rust (which indices are valid) to 
        a boolean array for each action index.
        
        Optionally expands the mask to include neighboring actions.
        
        Args:
            rust_mask: List of booleans, one per bitrate in BITRATE_LADDER_MBPS
            expand_neighbors: If True, apply the configured expansion strategy
        
        Returns:
            numpy array of booleans, shape (n_actions,)
        """
        if rust_mask is None:
            # No mask provided - all actions valid
            return np.ones(self.n_actions, dtype=bool)
        
        # Ensure it's a numpy array
        mask = np.array(rust_mask, dtype=bool)
        
        # Ensure at least one action is valid
        if not mask.any():
            print(f"{Colors.RED}[WARNING] All actions masked! Enabling all actions.{Colors.ENDC}")
            mask = np.ones(self.n_actions, dtype=bool)
        
        # Expand mask to include neighbors if requested
        if expand_neighbors:
            mask = self._apply_expansion_strategy(mask)
        
        return mask
    
    def _apply_expansion_strategy(self, mask):
        """
        Apply the configured mask expansion strategy.
        
        Args:
            mask: Original boolean numpy array of shape (n_actions,)
        
        Returns:
            Expanded boolean numpy array of shape (n_actions,)
        """
        n_original = mask.sum()
        
        if self.expansion_strategy == 'immediate_neighbors':
            expanded = MaskExpansionStrategy.immediate_neighbors(mask)
        elif self.expansion_strategy == 'two_neighbors':
            expanded = MaskExpansionStrategy.two_neighbors(mask)
        elif self.expansion_strategy == 'range':
            distance = self.expansion_param if self.expansion_param is not None else 1
            expanded = MaskExpansionStrategy.range_expansion(mask, max_distance=distance)
        elif self.expansion_strategy == 'percentage':
            pct = self.expansion_param if self.expansion_param is not None else 10.0
            expanded = MaskExpansionStrategy.percentage_expansion(
                mask, self.bitrate_ladder, percentage=pct
            )
        elif self.expansion_strategy == 'fill_gaps':
            expanded = MaskExpansionStrategy.fill_gaps(mask)
        elif self.expansion_strategy == 'none':
            expanded = mask
        else:
            print(f"{Colors.YELLOW}[WARNING] Unknown expansion strategy: {self.expansion_strategy}. Using original mask.{Colors.ENDC}")
            expanded = mask
        
        n_expanded = expanded.sum()
        
        if n_expanded > n_original:
            valid_bitrates_original = [self.bitrate_ladder[i] for i in np.where(mask)[0]]
            valid_bitrates_expanded = [self.bitrate_ladder[i] for i in np.where(expanded)[0]]
            # print(f"{Colors.CYAN}[Mask] Expanded {n_original} -> {n_expanded} actions{Colors.ENDC}")
            # print(f"       Original: {valid_bitrates_original}")
            # print(f"       Expanded: {valid_bitrates_expanded}")
        
        return expanded
    
    def _expand_mask_to_neighbors(self, mask):
        """
        DEPRECATED: Use _apply_expansion_strategy instead.
        Kept for backwards compatibility.
        """
        return MaskExpansionStrategy.immediate_neighbors(mask)

    def reset(self, *, seed=None, options=None):
        """Reset and return initial observation with action mask in info."""
        super().reset(seed=seed)
        print(f"\n--- Episode boundary (Python) ---")
        self.ep_return = 0.0
        self.ep_len = 0
        self.pending_obs_info = None
        
        try:
            # Receive initial observation from Rust
            parts = self.action_socket.recv_multipart()
            if len(parts) == 2:
                identity, obs_msg = parts
            elif len(parts) == 3 and parts[1] == b'':
                identity, _, obs_msg = parts
            else:
                raise RuntimeError(f"Unexpected frame count: {len(parts)}")
            
            obs_request = json.loads(obs_msg)
            
            # Extract and convert action mask
            rust_mask = obs_request.get("action_mask")
            self.current_action_mask = self._convert_rust_mask_to_action_mask(rust_mask)
            
            # Parse observation
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            flat_obs, meta = self._parse_obs_payload(obs_payload)
            
            self.pending_obs_info = (identity, flat_obs, meta)
            self.last_obs_for_done = flat_obs.copy()
            
            print(f"✅ Initial obs received. Action mask: {self.current_action_mask.sum()}/{self.n_actions} valid")
            
            # IMPORTANT: info dict must contain 'action_mask' for Maskable PPO
            info = {
                "obs_meta": meta,
                "action_mask": self.current_action_mask  # Required by MaskablePPO
            }
            return flat_obs, info
            
        except Exception as e:
            print(f"❌ Error during reset: {e}")
            raise

    def step(self, action):
        """
        Step with discrete action (index into bitrate ladder).
        
        Args:
            action: int, index in range [0, n_actions)
        """
        # Convert discrete action to bitrate value
        action_idx = int(action)
        bitrate_mbps = self.bitrate_ladder[action_idx]
        
        # Send action
        if self.pending_obs_info is None:
            raise RuntimeError("step() called before reset()")
        
        identity, current_obs, current_meta = self.pending_obs_info
        self.pending_obs_info = None
        
        action_response = {"bitrate_mbps": float(bitrate_mbps)}
        
        try:
            msg = [identity, b'', json.dumps(action_response).encode('utf-8')]
            self.action_socket.send_multipart(msg)
            print(f"{Colors.CYAN}[Env] Sent action: {bitrate_mbps} Mbps{Colors.ENDC}")
        except zmq.ZMQError as e:
            print(f"❌ Error sending action: {e}")
            raise
        
        # Receive transition with timeout
        try:
            # Set a timeout so we don't hang forever
            if self.transition_socket.poll(5000):  # 5 second timeout
                transition = self.transition_socket.recv_json(flags=zmq.NOBLOCK)
                print(f"{Colors.GREEN}[Env] Received transition: reward={transition.get('reward')}, done={transition.get('done')}{Colors.ENDC}")
            else:
                print(f"{Colors.RED}[Env] Timeout waiting for transition! Assuming episode ended.{Colors.ENDC}")
                # Rust probably crashed/exited - treat as done
                transition = {"reward": 0.0, "done": True}
        except zmq.ZMQError as e:
            print(f"❌ Error receiving transition: {e}")
            # Treat as episode end
            transition = {"reward": 0.0, "done": True}
        
        reward = float(transition.get("reward", 0.0))
        done = bool(transition.get("done", True))
        truncated = False
        
        print(f"{Colors.YELLOW}[Env] Step result: reward={reward:.4f}, done={done}{Colors.ENDC}")
        
        # Update stats
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward
        self.step_count += 1
        
        # === Check done BEFORE trying to receive next obs ===
        if done:
            # Episode finished - Rust has exited or will exit soon
            print(f"{Colors.GREEN}[Env] Episode finished (done=True). Using last valid obs.{Colors.ENDC}")
            next_obs = self.last_obs_for_done.copy()
            next_meta = current_meta
            self.current_action_mask = np.ones(self.n_actions, dtype=bool)
            self.pending_obs_info = None
            
        else:
            # Episode continues - wait for next observation
            print(f"{Colors.CYAN}[Env] Episode continuing, waiting for next obs...{Colors.ENDC}")
            try:
                # Also add timeout here
                if self.action_socket.poll(5000):  # 5 second timeout
                    parts = self.action_socket.recv_multipart(flags=zmq.NOBLOCK)
                    print(f"{Colors.GREEN}[Env] Received next observation ({len(parts)} frames){Colors.ENDC}")
                    
                    if len(parts) == 2:
                        identity, obs_msg = parts
                    elif len(parts) == 3 and parts[1] == b'':
                        identity, _, obs_msg = parts
                    else:
                        raise RuntimeError(f"Unexpected frame count: {len(parts)}")
                    
                    obs_request = json.loads(obs_msg)
                    rust_mask = obs_request.get("action_mask")
                    self.current_action_mask = self._convert_rust_mask_to_action_mask(rust_mask)
                    
                    obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
                    next_obs, next_meta = self._parse_obs_payload(obs_payload)
                    
                    # Store for next step
                    self.pending_obs_info = (identity, next_obs, next_meta)
                    self.last_obs_for_done = next_obs.copy()
                else:
                    # Timeout - Rust probably exited unexpectedly
                    print(f"{Colors.RED}[Env] Timeout waiting for next obs! Treating as episode end.{Colors.ENDC}")
                    next_obs = self.last_obs_for_done.copy()
                    next_meta = current_meta
                    done = True  # Force episode to end
                    self.pending_obs_info = None
                    
            except zmq.ZMQError as e:
                print(f"❌ Error receiving next obs: {e}")
                # Use last valid observation and end episode
                next_obs = self.last_obs_for_done.copy()
                next_meta = current_meta
                done = True
                self.pending_obs_info = None
        
        # Logging
        log_dict = {
            "train_env/reward": reward,
            "train_env/return_cumsum": self.run_return_cumsum,
            "train_env/action_idx": action_idx,
            "train_env/action_bitrate_mbps": bitrate_mbps,
            "train_env/done": int(done),
            "train_env/valid_actions": self.current_action_mask.sum(),
        }
        self._log_last_row(log_dict, next_obs)
        
        if wandb.run is not None:
            wandb.log(log_dict)
        
        if done:
            if wandb.run is not None:
                wandb.log({"episode/return": self.ep_return, "episode/len": self.ep_len})
            print(f"{Colors.BOLD}{Colors.GREEN}[Env] Episode complete: return={self.ep_return:.2f}, length={self.ep_len}{Colors.ENDC}")
            self.ep_return = 0.0
            self.ep_len = 0
        
        # Return action_mask in info
        info = {
            "obs_meta": next_meta,
            "action_mask": self.current_action_mask
        }
        return next_obs, reward, done, truncated, info
    
        def close(self):
            self.action_socket.close()
            self.transition_socket.close()
            self.ctx.term()

    # Include _parse_obs_payload and other helper methods from your original code
    @staticmethod
    def _parse_obs_payload(payload):
        """Parse observation from various formats."""
        if isinstance(payload, (list, np.ndarray)):
            raw = np.asarray(payload, dtype=np.float32).ravel()
            feat_dim = FEAT_DIM
            window_len = WINDOW_LEN
            if raw.size == feat_dim:
                flat = np.zeros((window_len * feat_dim,), dtype=np.float32)
                flat[-feat_dim:] = raw
                seq_len = 1
            else:
                expect = window_len * feat_dim
                if raw.size < expect:
                    flat = np.pad(raw, (expect - raw.size, 0))
                else:
                    flat = raw[-expect:]
                seq_len = min(window_len, max(1, flat.size // feat_dim))
            mask = np.zeros(window_len, dtype=bool)
            mask[-seq_len:] = True
            meta = dict(seq_len=seq_len, feat_dim=feat_dim, window_len=window_len, mask=mask)
            return flat.astype(np.float32, copy=False), meta

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
            raise KeyError(f"Cannot parse obs payload: {payload.keys()}")

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


# =====================================================
# 2. MASK CALLBACK FUNCTION (for ActionMasker wrapper)
# =====================================================
class MaskExpansionStrategy:
    """Different strategies for expanding action masks to neighbors."""
    
    @staticmethod
    def immediate_neighbors(mask):
        """
        Expand to immediate left/right neighbors only.
        
        Example: [F, F, T, F, F] -> [F, T, T, T, F]
        """
        expanded = mask.copy()
        valid_indices = np.where(mask)[0]
        
        for idx in valid_indices:
            if idx > 0:
                expanded[idx - 1] = True
            if idx < len(mask) - 1:
                expanded[idx + 1] = True
        
        return expanded
    @staticmethod
    def range_expansion(mask, max_distance=1):
        """
        Expand by a configurable distance.
        
        Args:
            mask: Original boolean mask
            max_distance: How many steps away to expand (1 = immediate, 2 = two steps, etc.)
        
        Example with max_distance=2: [F, F, F, T, F, F, F] -> [F, T, T, T, T, T, F]
        """
        expanded = mask.copy()
        valid_indices = np.where(mask)[0]
        
        for idx in valid_indices:
            for offset in range(-max_distance, max_distance + 1):
                if offset == 0:
                    continue
                neighbor_idx = idx + offset
                if 0 <= neighbor_idx < len(mask):
                    expanded[neighbor_idx] = True
        
        return expanded

def mask_fn(env: gym.Env) -> np.ndarray:
    """
    Callback function that returns valid action mask.
    This is called by the ActionMasker wrapper.
    
    Returns:
        Boolean array of shape (n_actions,) where True = action is valid
    """
    # The environment stores the current mask
    if hasattr(env, 'current_action_mask') and env.current_action_mask is not None:
        return env.current_action_mask
    
    # Fallback: all actions valid
    return np.ones(env.action_space.n, dtype=bool)


# =====================================================
# EVALUATION LOOP
# =====================================================
# =====================================================
# EVALUATION LOOP
# =====================================================
def load_model_local(model_path: Path) -> MaskablePPO:
    # import sys, numpy as np
    # from stable_baselines3 import MaskablePPO

    # Compatibility fix for NumPy 2.x
    if 'numpy.core' not in sys.modules and hasattr(np, '_core'):
        sys.modules['numpy.core'] = np._core
        sys.modules['numpy.core.numeric'] = np._core.numeric

    model = MaskablePPO.load(str(model_path))
    print(f"{Colors.GREEN}✓ Model {model_path} loaded successfully{Colors.ENDC}")

    return model 

def load_model_local_new(model_path: Path, env: ActionMasker) -> MaskablePPO:
    """
    Loads a model, handling potential NumPy version conflicts and
    custom policy classes by using the custom_objects dictionary.
    """
    
    # (!!) DELETE THE SYS.MODULES HACK THAT WAS HERE (!!)

    print(f"{Colors.CYAN}Loading model from: {model_path}{Colors.ENDC}")

    # This dictionary tells SB3 how to handle custom classes
    # or objects that can't be unpickled (like old NumPy 1.x data).
    custom_objects = {
        "policy_class": DropoutActorCriticPolicy, 
        # Fix for NumPy 1.x models
        "observation_space": env.observation_space,
        "action_space": env.action_space,
        "_last_obs": None,
        "_last_episode_starts": None,
        
        # --- THIS IS THE FIX ---
    }

    model = MaskablePPO.load(
        str(model_path),  # Use str() to be safe
        env=env, 
        custom_objects=custom_objects
    )  
    print(f"{Colors.GREEN}✓ Model {model_path} loaded successfully{Colors.ENDC}")
    return model

# =====================================================
# RUST SIMULATION HELPERS
# =====================================================


def find_project_root(start: Path) -> Path:
    for p in [start.resolve(), *start.resolve().parents]:
        if (p / "Cargo.toml").exists():
            return p
    raise RuntimeError("Cargo.toml not found.")

def find_exe(release=True):
    root = find_project_root(Path(__file__).parent)
    target_dir = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=root
    ).decode())["target_directory"]
    suffix = ".exe" if sys.platform.startswith("win") else ""
    example_name = "XR_sim"
    return Path(target_dir) / ("release" if release else "debug") / "examples" / f"{example_name}{suffix}"

def run_sim(exe: Path, argv: list[str], env: dict[str, str], log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", buffering=1) as f:
        f.write(f"# Started: {datetime.now().isoformat()}\nCMD: {' '.join([str(exe), *argv])}\n\n")
        proc = subprocess.Popen([str(exe), *argv],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env)
        RUST_PROCS.append(proc)
        for line in proc.stdout:
            f.write(line)
            print(line, end="", flush=True)  # ensure printing during rust sim 
        return proc.wait()

# =====================================================
# MAIN
# =====================================================


def run_single_evaluation(color_model: str, deterministic: bool, combos: list, ): 
    # Load trained model
    base_id = os.environ.get("SLURM_JOB_ID") or os.getpid()
    action_ep = f"ipc:///tmp/xr_{base_id}_eval_action"
    step_ep = f"ipc:///tmp/xr_{base_id}_eval_step"
    
    print(f"\n{Colors.YELLOW}ZMQ Endpoints:{Colors.ENDC}")
    print(f"  Action: {action_ep}")
    print(f"  Step:   {step_ep}")
    
    DETERMINISTIC = deterministic  
    base_env = MaskableDiscreteZmqEnv(
                    action_ep=action_ep,
                    step_ep=step_ep,
                    bitrate_ladder_mbps=BITRATE_LADDER_MBPS,
                    expansion_strategy='immediate_neighbors',
                    expansion_param=None
                )
    env = ActionMasker(base_env, mask_fn)
    # Update local model path and evaluation string
    LOCAL_MODEL_PATH = Path(f"MaskedPPO_Models/model_{color_model}.zip")
    eval_string = f"{color_model}D{DETERMINISTIC}"

    try:
        if LOCAL_MODEL_PATH:
            print(f"{Colors.CYAN}Using local model path: {LOCAL_MODEL_PATH}{Colors.ENDC}")
            model = load_model_local(LOCAL_MODEL_PATH, env)
        else:
            model = load_model_from_wandb(WANDB_ENTITY, WANDB_PROJECT, MODEL_ARTIFACT)
    
    except Exception as e:
        print(f"{Colors.RED}Failed to load model: {e}{Colors.ENDC}")
        print(f"\n{Colors.YELLOW}Options to fix this:{Colors.ENDC}")
        print(f"  1. Set LOCAL_MODEL_PATH=/path/to/model.zip")
        print(f"  2. Fix W&B authentication with: wandb login --relogin")
        print(f"  3. Download model manually from W&B and use LOCAL_MODEL_PATH")
        sys.exit(1)
    
    # Setup endpoints
   
    # Find Rust executable
    exe = find_exe(release=True)
    print(f"\n{Colors.YELLOW}Rust executable: {exe}{Colors.ENDC}")
    
    # Generate evaluation scenarios (subset of training combos)
   

    print(f"\n{Colors.YELLOW}Will evaluate on {len(combos)} scenarios{Colors.ENDC}")
    print(f"{Colors.CYAN}Creating evaluation environment...{Colors.ENDC}")

    # Run evaluation episodes
    all_results = []
    
    try:
        for ep_idx, combo in enumerate(combos, 1):
            print(f"\n{Colors.BOLD}{Colors.BLUE}Starting Episode {ep_idx}/{len(combos)}{Colors.ENDC}")
            
            (simtime, test, nbg, nxr, is_ul, bitrate, video_sample, FPS,
            close_users, close_distance, seed, distance, gop,
            intrarefresh, ABR, nest_profile, rate_bps_src_BG, pl_prob) = combo
            
            # Build Rust arguments
            argv = [
                f"{simtime}", "12000.0", "10000", f"{distance}", f"{bitrate}",
                f"{pl_prob}", f"{nxr}", f"{nbg}", f"{rate_bps_src_BG}", f"{is_ul}",
                f"{test}", f"{video_sample}", f"{FPS}", f"{close_users}", f"{close_distance}",
                f"{seed}", f"{gop}", f"{intrarefresh}", f"{ABR}", f"{nest_profile}",
                "1", f"{ep_idx}", f"{observation_type}", f"{reward_mode}", f"{T_ABR}", f"{eval_string}", 
            ]
            
            # Debug: Print the exact command
            print(f"{Colors.YELLOW}[DEBUG] Rust command:{Colors.ENDC}")
            print(f"  {exe} {' '.join(argv)}")
            
            env_sim = os.environ.copy()
            env_sim["ZMQ_ACTION_EP"] = action_ep
            env_sim["ZMQ_STEP_EP"] = step_ep
            
            log_path = Path("EvalResults") / f"eval_ep_{ep_idx}" / "sim.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # ===== TEMPORARY DEBUG: Show Rust output live =====
            print(f"{Colors.CYAN}Launching Rust simulator (episode {ep_idx})...{Colors.ENDC}")
            print(f"{Colors.YELLOW}[DEBUG] Watching Rust output for 10 seconds...{Colors.ENDC}")
            
            # Launch WITHOUT redirecting stdout (so we can see errors)
            proc = subprocess.Popen(
                [str(exe), *argv],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env_sim,
                bufsize=1  # Line buffered
            )
            RUST_PROCS.append(proc)
            
            # Monitor Rust output for a few seconds to see if it starts properly
            import select
            import time
            
            print(f"{Colors.MAGENTA}--- Rust Output (first 10 seconds) ---{Colors.ENDC}")
            start_time = time.time()
            rust_started = False
            
            while time.time() - start_time < 10:
                # Check if process crashed
                if proc.poll() is not None:
                    print(f"{Colors.RED}[ERROR] Rust process exited early with code: {proc.poll()}{Colors.ENDC}")
                    # Read any remaining output
                    remaining = proc.stdout.read()
                    if remaining:
                        print(remaining)
                    break
                
                # Try to read output (non-blocking on Unix)
                try:
                    import fcntl
                    import os as os_module
                    fd = proc.stdout.fileno()
                    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os_module.O_NONBLOCK)
                    
                    line = proc.stdout.readline()
                    if line:
                        print(f"  [Rust] {line.rstrip()}")
                        # Look for signs that Rust is ready
                        if "waiting for" in line.lower() or "ready" in line.lower() or "connected" in line.lower():
                            rust_started = True
                            print(f"{Colors.GREEN}[DEBUG] Rust appears to be ready!{Colors.ENDC}")
                            break
                except (BlockingIOError, IOError):
                    pass
                
                time.sleep(0.1)
            
            print(f"{Colors.MAGENTA}--- End Rust Output ---{Colors.ENDC}")
            
            if not rust_started and proc.poll() is None:
                print(f"{Colors.YELLOW}[WARNING] Rust is running but hasn't printed expected startup messages.{Colors.ENDC}")
                print(f"{Colors.YELLOW}           Proceeding anyway...{Colors.ENDC}")
            
            # Now try to connect Python env
            print(f"{Colors.CYAN}Connecting to simulator...{Colors.ENDC}")
            
            try:
                
                print(f"{Colors.GREEN}✓ Environment connected{Colors.ENDC}")
                
                # Run episode
                print(f"{Colors.GREEN}Running evaluation episode {ep_idx}...{Colors.ENDC}")
                
                # Add timeout to reset() too
                import signal as signal_module
                
                def timeout_handler(signum, frame):
                    raise TimeoutError("reset() timed out")
                
                signal_module.signal(signal_module.SIGALRM, timeout_handler)
                signal_module.alarm(15)  # 15 second timeout
                
                try:
                    obs, info = env.reset()
                    signal_module.alarm(0)  # Cancel alarm
                except TimeoutError:
                    print(f"{Colors.RED}[ERROR] env.reset() timed out! Rust is not responding.{Colors.ENDC}")
                    print(f"{Colors.RED}        Check if Rust is waiting for initial observation or crashed.{Colors.ENDC}")
                    print(f"{Colors.YELLOW}        Log file: {log_path}{Colors.ENDC}")
                    proc.kill()
                    continue
                
                done = False
                ep_return = 0.0
                ep_len = 0
                
                while not done:
                    proc_status = proc.poll()
                    if proc_status is not None:
                        print(f"{Colors.RED}[Python] Rust simulator exited with code: {proc_status}{Colors.ENDC}")
                        done = True
                        break
                    
                    action, _states = model.predict(obs, deterministic=DETERMINISTIC)
                    
                    try:
                        obs, reward, done, truncated, info = env.step(action)
                        ep_return += reward
                        ep_len += 1
                    except zmq.ZMQError as e:
                        print(f"{Colors.RED}[Python] ZMQ Error: {e}{Colors.ENDC}")
                        done = True
                        break
                
                ret = proc.poll()
                if ret is None:
                    ret = proc.wait(timeout=10)
                
                env.close()
                
                print(f"{Colors.GREEN}Episode {ep_idx} completed (exit code: {ret}){Colors.ENDC}")
                print(f"  Return: {ep_return:.2f}, Length: {ep_len}")
                
                all_results.append({
                    "episode": ep_idx,
                    "return": ep_return,
                    "length": ep_len,
                    "exit_code": ret
                })
                
                if proc in RUST_PROCS:
                    RUST_PROCS.remove(proc)
                    
            except Exception as e:
                print(f"{Colors.RED}[ERROR] Exception during episode: {e}{Colors.ENDC}")
                import traceback
                traceback.print_exc()
                if proc.poll() is None:
                    proc.kill()
                continue        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Evaluation interrupted by user{Colors.ENDC}")
    except Exception as e:
        print(f"{Colors.RED}Error during evaluation: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
    finally:
        # Note: We don't need env.close() here anymore
        # because it's closed inside the loop after each episode.
        print(f"{Colors.CYAN}Cleaning up...{Colors.ENDC}")

    
    # Print summary (this part is fine)
    if all_results:
        returns = [r["return"] for r in all_results]
        lengths = [r["length"] for r in all_results]
        
        print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.GREEN}Evaluation Complete{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
        print(f"Episodes: {len(all_results)}")
        print(f"Mean Return: {np.mean(returns):.2f} ± {np.std(returns):.2f}")
    
    # Finish W&B run
    if USE_WANDB and wandb.run is not None:
        wandb.finish()


def main_single_c():

    #################################################
    ### SIMULATION PARAMS
    LOCAL_MODEL_PATH = Path(f"MaskedPPO_Models/model_red.zip")
    simTime = [80.0]
    TEST_TYPE = [ "STD", "BW", "RANDOM"]                     # "BW", "JI", "PL", "RANDOM", "STD"
    k_queue = 10000
    mean_length_BG = 12000.0
    rate_bps_src_BG = [10e6, ]
    distance_list = [1.5]
    distance_close_users = [1.5]
    num_close_users = [0]
    N_XR = [1, 2, 3, 4, 5]
    PL = [0.1]
    fps_list = [90.0]
    initial_bitrate_mbps = [10.0]
    ABR_ENABLED = [3]
    nest_profiles = [1]
    RANDOM_SEEDS = list(range(1, 10))
    video_samples = ["snow"]
    N_BGs = [0]
    IS_UL_BG = [0]
    intrarefresh_choice = [1]
    GoP_sizes = [90]
    everest_tests = 1  ## For random 24x12 grid STA placements, with velocity 5m/s in a circle. 

    
    print(f"{Colors.BOLD}{Colors.MAGENTA}")
    print("="*60)
    print("  MASKABLE PPO EVALUATION")
    print("="*60)
    print(f"{Colors.ENDC}")
    
    
    # Load trained model
    try:
        if LOCAL_MODEL_PATH:
            print(f"{Colors.CYAN}Using local model path: {LOCAL_MODEL_PATH}{Colors.ENDC}")
            model = load_model_local(LOCAL_MODEL_PATH)
        else:
            model = load_model_from_wandb(WANDB_ENTITY, WANDB_PROJECT, MODEL_ARTIFACT)
    
    except Exception as e:
        print(f"{Colors.RED}Failed to load model: {e}{Colors.ENDC}")
        print(f"\n{Colors.YELLOW}Options to fix this:{Colors.ENDC}")
        print(f"  1. Set LOCAL_MODEL_PATH=/path/to/model.zip")
        print(f"  2. Fix W&B authentication with: wandb login --relogin")
        print(f"  3. Download model manually from W&B and use LOCAL_MODEL_PATH")
        sys.exit(1)
    
    # Setup endpoints
    base_id = os.environ.get("SLURM_JOB_ID") or os.getpid()
    action_ep = f"ipc:///tmp/xr_{base_id}_eval_action"
    step_ep = f"ipc:///tmp/xr_{base_id}_eval_step"
    
    print(f"\n{Colors.YELLOW}ZMQ Endpoints:{Colors.ENDC}")
    print(f"  Action: {action_ep}")
    print(f"  Step:   {step_ep}")
    
    # Find Rust executable
    exe = find_exe(release=True)
    print(f"\n{Colors.YELLOW}Rust executable: {exe}{Colors.ENDC}")
    
    # Generate evaluation scenarios (subset of training combos)
    combos = list(product(
        simTime, TEST_TYPE, N_BGs, N_XR, IS_UL_BG, initial_bitrate_mbps,
        video_samples, fps_list, num_close_users, distance_close_users,
        RANDOM_SEEDS[:5],  # Use first 5 seeds only for eval
        distance_list, GoP_sizes, intrarefresh_choice,
        ABR_ENABLED, nest_profiles, rate_bps_src_BG, PL,
    ))
    print(f"{Colors.MAGENTA} NUMBER OF COMBOS: {len(combos)} EVAL EP: {N_EVAL_EPISODES} {Colors.ENDC}")

    random.shuffle(combos)
    combos = combos[:N_EVAL_EPISODES]  # Limit to N_EVAL_EPISODES
    

    print(f"\n{Colors.YELLOW}Will evaluate on {len(combos)} scenarios{Colors.ENDC}")
    print(f"{Colors.CYAN}Creating evaluation environment...{Colors.ENDC}")

    # Run evaluation episodes
    all_results = []
    
    try:
        for ep_idx, combo in enumerate(combos, 1):
            print(f"\n{Colors.BOLD}{Colors.BLUE}Starting Episode {ep_idx}/{len(combos)}{Colors.ENDC}")
            
            (simtime, test, nbg, nxr, is_ul, bitrate, video_sample, FPS,
            close_users, close_distance, seed, distance, gop,
            intrarefresh, ABR, nest_profile, rate_bps_src_BG, pl_prob) = combo
            
            # Build Rust arguments
            argv = [
                f"{simtime}", "12000.0", "10000", f"{distance}", f"{bitrate}",
                f"{pl_prob}", f"{nxr}", f"{nbg}", f"{rate_bps_src_BG}", f"{is_ul}",
                f"{test}", f"{video_sample}", f"{FPS}", f"{close_users}", f"{close_distance}",
                f"{seed}", f"{gop}", f"{intrarefresh}", f"{ABR}", f"{nest_profile}",
                "1", f"{ep_idx}", f"{observation_type}", f"{reward_mode}", f"{T_ABR}", f"{eval_string}", 
            ]
            
            # Debug: Print the exact command
            print(f"{Colors.YELLOW}[DEBUG] Rust command:{Colors.ENDC}")
            print(f"  {exe} {' '.join(argv)}")
            
            env_sim = os.environ.copy()
            env_sim["ZMQ_ACTION_EP"] = action_ep
            env_sim["ZMQ_STEP_EP"] = step_ep
            
            log_path = Path("EvalResults") / f"eval_ep_{ep_idx}" / "sim.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # ===== TEMPORARY DEBUG: Show Rust output live =====
            print(f"{Colors.CYAN}Launching Rust simulator (episode {ep_idx})...{Colors.ENDC}")
            print(f"{Colors.YELLOW}[DEBUG] Watching Rust output for 10 seconds...{Colors.ENDC}")
            
            # Launch WITHOUT redirecting stdout (so we can see errors)
            proc = subprocess.Popen(
                [str(exe), *argv],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env_sim,
                bufsize=1  # Line buffered
            )
            RUST_PROCS.append(proc)
            
            # Monitor Rust output for a few seconds to see if it starts properly
            import select
            import time
            
            print(f"{Colors.MAGENTA}--- Rust Output (first 10 seconds) ---{Colors.ENDC}")
            start_time = time.time()
            rust_started = False
            
            while time.time() - start_time < 10:
                # Check if process crashed
                if proc.poll() is not None:
                    print(f"{Colors.RED}[ERROR] Rust process exited early with code: {proc.poll()}{Colors.ENDC}")
                    # Read any remaining output
                    remaining = proc.stdout.read()
                    if remaining:
                        print(remaining)
                    break
                
                # Try to read output (non-blocking on Unix)
                try:
                    import fcntl
                    import os as os_module
                    fd = proc.stdout.fileno()
                    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os_module.O_NONBLOCK)
                    
                    line = proc.stdout.readline()
                    if line:
                        print(f"  [Rust] {line.rstrip()}")
                        # Look for signs that Rust is ready
                        if "waiting for" in line.lower() or "ready" in line.lower() or "connected" in line.lower():
                            rust_started = True
                            print(f"{Colors.GREEN}[DEBUG] Rust appears to be ready!{Colors.ENDC}")
                            break
                except (BlockingIOError, IOError):
                    pass
                
                time.sleep(0.1)
            
            print(f"{Colors.MAGENTA}--- End Rust Output ---{Colors.ENDC}")
            
            if not rust_started and proc.poll() is None:
                print(f"{Colors.YELLOW}[WARNING] Rust is running but hasn't printed expected startup messages.{Colors.ENDC}")
                print(f"{Colors.YELLOW}           Proceeding anyway...{Colors.ENDC}")
            
            # Now try to connect Python env
            print(f"{Colors.CYAN}Connecting to simulator...{Colors.ENDC}")
            
            try:
                base_env = MaskableDiscreteZmqEnv(
                    action_ep=action_ep,
                    step_ep=step_ep,
                    bitrate_ladder_mbps=BITRATE_LADDER_MBPS,
                    expansion_strategy='immediate_neighbors',
                    expansion_param=None
                )
                env = ActionMasker(base_env, mask_fn)
                print(f"{Colors.GREEN}✓ Environment connected{Colors.ENDC}")
                
                # Run episode
                print(f"{Colors.GREEN}Running evaluation episode {ep_idx}...{Colors.ENDC}")
                
                # Add timeout to reset() too
                import signal as signal_module
                
                def timeout_handler(signum, frame):
                    raise TimeoutError("reset() timed out")
                
                signal_module.signal(signal_module.SIGALRM, timeout_handler)
                signal_module.alarm(15)  # 15 second timeout
                
                try:
                    obs, info = env.reset()
                    signal_module.alarm(0)  # Cancel alarm
                except TimeoutError:
                    print(f"{Colors.RED}[ERROR] env.reset() timed out! Rust is not responding.{Colors.ENDC}")
                    print(f"{Colors.RED}        Check if Rust is waiting for initial observation or crashed.{Colors.ENDC}")
                    print(f"{Colors.YELLOW}        Log file: {log_path}{Colors.ENDC}")
                    proc.kill()
                    continue
                
                done = False
                ep_return = 0.0
                ep_len = 0
                
                while not done:
                    proc_status = proc.poll()
                    if proc_status is not None:
                        print(f"{Colors.RED}[Python] Rust simulator exited with code: {proc_status}{Colors.ENDC}")
                        done = True
                        break
                    
                    action, _states = model.predict(obs, deterministic=DETERMINISTIC)
                    
                    try:
                        obs, reward, done, truncated, info = env.step(action)
                        ep_return += reward
                        ep_len += 1
                    except zmq.ZMQError as e:
                        print(f"{Colors.RED}[Python] ZMQ Error: {e}{Colors.ENDC}")
                        done = True
                        break
                
                ret = proc.poll()
                if ret is None:
                    ret = proc.wait(timeout=10)
                
                env.close()
                
                print(f"{Colors.GREEN}Episode {ep_idx} completed (exit code: {ret}){Colors.ENDC}")
                print(f"  Return: {ep_return:.2f}, Length: {ep_len}")
                
                all_results.append({
                    "episode": ep_idx,
                    "return": ep_return,
                    "length": ep_len,
                    "exit_code": ret
                })
                
                if proc in RUST_PROCS:
                    RUST_PROCS.remove(proc)
                    
            except Exception as e:
                print(f"{Colors.RED}[ERROR] Exception during episode: {e}{Colors.ENDC}")
                import traceback
                traceback.print_exc()
                if proc.poll() is None:
                    proc.kill()
                continue        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Evaluation interrupted by user{Colors.ENDC}")
    except Exception as e:
        print(f"{Colors.RED}Error during evaluation: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
    finally:
        # Note: We don't need env.close() here anymore
        # because it's closed inside the loop after each episode.
        print(f"{Colors.CYAN}Cleaning up...{Colors.ENDC}")

    
    # Print summary (this part is fine)
    if all_results:
        returns = [r["return"] for r in all_results]
        lengths = [r["length"] for r in all_results]
        
        print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.GREEN}Evaluation Complete{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
        print(f"Episodes: {len(all_results)}")
        print(f"Mean Return: {np.mean(returns):.2f} ± {np.std(returns):.2f}")
    
    # Finish W&B run
    if USE_WANDB and wandb.run is not None:
        wandb.finish()
    
    print(f"\n{Colors.BOLD}{Colors.GREEN}Evaluation complete!{Colors.ENDC}")

if __name__ == "__main__":
    atexit.register(cleanup_rust_processes)
    signal.signal(signal.SIGINT, lambda sig, frame: (print("\n[CTRL-C] stopping…"), cleanup_rust_processes(), exit(0)))
    signal.signal(signal.SIGTERM, lambda sig, frame: (print("\n[SIGTERM] stopping…"), cleanup_rust_processes(), exit(0)))
    
    simTime = [80.0]
    TEST_TYPE = [ "STD", "BW", "RANDOM"]                     # "BW", "JI", "PL", "RANDOM", "STD"
    k_queue = 10000
    mean_length_BG = 12000.0
    rate_bps_src_BG = [10e6, ]
    distance_list = [1.5]
    distance_close_users = [1.5]
    num_close_users = [0]
    N_XR = [1, 2, 3, 4, 5]
    PL = [0.1]
    fps_list = [90.0]
    initial_bitrate_mbps = [10.0]
    ABR_ENABLED = [3]
    nest_profiles = [1]
    RANDOM_SEEDS = list(range(1, 10))
    video_samples = ["snow"]
    N_BGs = [0]
    IS_UL_BG = [0]
    intrarefresh_choice = [1]
    GoP_sizes = [90]
    everest_tests = 1  ## For random 24x12 grid STA placements, with velocity 5m/s in a circle. 

    # Define the constants for the grid
    # color_list = ["red", "brown", "green", "cinnamon", "purple"]

    color_list = [  "purple"]
    deterministic_choices = [ False]

    # Evaluation Parameters
    N_EVAL_EPISODES = 2000  # Number of episodes to evaluate

    for color in color_list:
        for choice in deterministic_choices: 
            eval_string = f"{color}D{choice}"
            print(f"Eval on {eval_string}")

            DETERMINISTIC = choice
            main_single_c()    
    # param_grid = list(product(color_list, deterministic_choices))

    # combos = list(product(
    #     simTime, TEST_TYPE, N_BGs, N_XR, IS_UL_BG, initial_bitrate_mbps,
    #     video_samples, fps_list, num_close_users, distance_close_users,
    #     RANDOM_SEEDS[:5],  # Use first 5 seeds only for eval
    #     distance_list, GoP_sizes, intrarefresh_choice,
    #     ABR_ENABLED, nest_profiles, rate_bps_src_BG, PL,
    # ))
    # print(f"{Colors.MAGENTA} NUMBER OF COMBOS: {len(combos)} {Colors.ENDC}")

    # random.shuffle(combos)
    # # combos = combos[:N_EVAL_EPISODES]  # Limit to N_EVAL_EPISODES
    


    # print(f"{Colors.BOLD}{Colors.MAGENTA}")
    # print("="*60)
    # print("  MASKABLE PPO EVALUATION")
    # print("="*60)
    # print(f"{Colors.ENDC}")


    # # color_list = ["red", "brown", "green", "cinnamon", "purple"]
    # # deterministic_choices = [True, False]
    # # param_grid = list(product(color_list, deterministic_choices))

    # # Determine the number of processes to use.
    # # Max of (grid size, CPU count) to avoid oversubscribing.
    # num_processes = 5
    
    # print(f"\n{Colors.BOLD}{Colors.MAGENTA}")
    # print("="*60)
    # print(f"  STARTING PARALLEL EVALUATION ({len(param_grid)} jobs on {num_processes} cores)")
    # print("="*60)
    # print(f"{Colors.ENDC}")

    # for color_model in color_list: 
    #     for deterministic in deterministic_choices: 
    #         run_single_evaluation(color_model, deterministic, combos)
    # Use a Pool to manage the parallel execution
    # 'initializer' is important to handle resources like ZMQ sockets/Rust processes
    # correctly in each child process.
    # with Pool(processes=num_processes) as pool:
    #     # pool.starmap applies the function to each tuple in the param_grid
    #     # The result is a list of results returned by run_single_evaluation
    #     all_parallel_results = pool.starmap(run_single_evaluation, param_grid, combos)

    # -----------------------------------------------------------------
    # FINAL SUMMARY
    # -----------------------------------------------------------------
    print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.GREEN}OVERALL PARALLEL EVALUATION COMPLETE{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.ENDC}")
    
    # Print a clean summary table
    print(f"{'Color':<10} | {'Deterministic':<15} | {'Mean Return':<15} | {'Std Dev':<10} | {'Episodes':<10}")
    print("-" * 65)


