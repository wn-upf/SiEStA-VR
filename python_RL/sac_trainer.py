#sac_trainer.py
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
import torch as th
from gymnasium import spaces

import torch.nn as nn

from stable_baselines3.common.callbacks import CallbackList

from sb3_contrib import MaskablePPO
from sb3_contrib.common.wrappers import ActionMasker


import subprocess
import signal
import atexit
import os
import shutil

# Keep global list of Rust child processes
RUST_PROCS = []

#CONSTS
##############################

ACTION_ENDPOINT  = os.environ.get("ZMQ_ACTION_EP",  "ipc:///tmp/xr_default_action")
STEP_ENDPOINT    = os.environ.get("ZMQ_STEP_EP",    "ipc:///tmp/xr_default_step")
TRAINER_ENDPOINT = os.environ.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")


################################################
# RL PARAMS

N_STEPS_RL= 10_000_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   
FEAT_DIM = 14
WINDOW_LEN = 5
OBSERVATION_SHAPE = (WINDOW_LEN * FEAT_DIM, )

ACTION_DIM = 20
ACT_MIN_MBPS = 5.0
ACT_MAX_MBPS = 100.0
BITRATE_LADDER_MBPS = list(range(5, 101, 5))


TIMEOUT_ZMQSERVER=2000
policy_ppo_a2c = "MlpPolicy"  # shared by PPO and A2C


#### RL INPUT ARGS (RUST)
observation_type = [2] ## 0-> Raw unscaled obs, 1 -> Scaled in expected bounds, 2-> Running Normalization. 
reward_mode = 0 ## normalized reward.  // 0-> naive , 1-> normalized, 2-> ??? todo shaping. 
T_ABR = 0.3 ## update every T seconds. With lower value, more frequent steps in simulation but noisier updates. 
#################################################
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

###############################3
from stable_baselines3.common.callbacks import BaseCallback
from sb3_contrib.common.maskable.policies import MaskableActorCriticPolicy

# =====================================================
# 1. CHANGE ACTION SPACE TO DISCRETE
# =====================================================
MASKABLE_PPO_CONFIG_IMMEDIATE = {
    "algo": "MaskablePPO",
    "learning_rate": 3e-4,
    "gamma": 0.99,
    "gae_lambda": 0.95,
    "n_steps": 2048,
    "batch_size": 64,
    "n_epochs": 10,
    "clip_range": 0.2,
    "ent_coef": 0.01,
    "vf_coef": 0.5,
    "max_grad_norm": 0.5,
    "net_arch": [256, 256],
    "use_vectorized_obs": True,
    "mask_expansion_strategy": "immediate_neighbors",  # Expand to ±1 neighbor
}

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
        except zmq.ZMQError as e:
            print(f"❌ Error sending action: {e}")
            raise
        
        # Receive transition
        try:
            transition = self.transition_socket.recv_json()
        except zmq.ZMQError as e:
            print(f"❌ Error receiving transition: {e}")
            raise
        
        reward = float(transition["reward"])
        done = bool(transition["done"])
        truncated = False
        
        # Update stats
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward
        self.step_count += 1
        
        if done:
            next_obs = self.last_obs_for_done
            next_meta = current_meta
            self.current_action_mask = np.ones(self.n_actions, dtype=bool)
            self.pending_obs_info = None
        else:
            # Receive next observation
            parts = self.action_socket.recv_multipart()
            if len(parts) == 2:
                identity, obs_msg = parts
            elif len(parts) == 3 and parts[1] == b'':
                identity, _, obs_msg = parts
            else:
                raise RuntimeError(f"Unexpected frame count: {len(parts)}")
            
            obs_request = json.loads(obs_msg)
            
            # Extract and convert next action mask
            rust_mask = obs_request.get("action_mask")
            self.current_action_mask = self._convert_rust_mask_to_action_mask(rust_mask)
            
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            next_obs, next_meta = self._parse_obs_payload(obs_payload)
            
            self.pending_obs_info = (identity, next_obs, next_meta)
            self.last_obs_for_done = next_obs.copy()
        
        # Logging
        log_dict = {
            "train/reward": reward,
            "train/return_cumsum": self.run_return_cumsum,
            "train/action_idx": action_idx,
            "train/action_bitrate_mbps": bitrate_mbps,
            "train/done": int(done),
            "train/valid_actions": self.current_action_mask.sum(),
        }
        
        if wandb.run is not None:
            wandb.log(log_dict)
        
        if done:
            if wandb.run is not None:
                wandb.log({"episode/return": self.ep_return, "episode/len": self.ep_len})
            self.ep_return = 0.0
            self.ep_len = 0
        
        # IMPORTANT: Return action_mask in info
        info = {
            "obs_meta": next_meta,
            "action_mask": self.current_action_mask  # Required by MaskablePPO
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
# 3. UPDATED TRAINING FUNCTION
# =====================================================

def train_maskable_ppo(action_ep: str, step_ep: str):
    """Train using Maskable PPO with discrete actions."""
    
    run = wandb.init(
        project=os.environ.get("WANDB_PROJECT", "xr-abr-maskable"),
        entity=os.environ.get("WANDB_ENTITY"),
        save_code=True,
    )
    
    # Choose expansion strategy from config
    expansion_strategy = wandb.config.get("mask_expansion_strategy", "immediate_neighbors")
    expansion_param = wandb.config.get("mask_expansion_param", None)
    
    # Create base environment
    base_env = MaskableDiscreteZmqEnv(
        action_ep=action_ep,
        step_ep=step_ep,
        bitrate_ladder_mbps=BITRATE_LADDER_MBPS,
        expansion_strategy=expansion_strategy,
        expansion_param=expansion_param
    )
    
    # Wrap with ActionMasker - this applies the mask before each action
    env = ActionMasker(base_env, mask_fn)
    
    print(f"{Colors.GREEN}Environment created with {len(BITRATE_LADDER_MBPS)} discrete actions{Colors.ENDC}")
    print(f"{Colors.GREEN}Mask expansion: {expansion_strategy}{Colors.ENDC}")
    
    # Configure policy
    policy_kwargs = dict(
        net_arch=dict(
            pi=list(wandb.config.net_arch),  # Actor network
            vf=list(wandb.config.net_arch)   # Critic network
        ),
    )
    
    # If using windowed observations with feature extractor:
    if wandb.config.get("use_vectorized_obs", True):
        policy_kwargs['features_extractor_class'] = LastRowExtractor
        policy_kwargs['features_extractor_kwargs'] = dict(feat_dim=FEAT_DIM)
    
    # Create MaskablePPO model
    model = MaskablePPO(
        policy=MaskableActorCriticPolicy,
        env=env,
        learning_rate=wandb.config.learning_rate,
        n_steps=wandb.config.get("n_steps", 2048),
        batch_size=wandb.config.batch_size,
        n_epochs=wandb.config.get("n_epochs", 10),
        gamma=wandb.config.gamma,
        gae_lambda=wandb.config.get("gae_lambda", 0.95),
        clip_range=wandb.config.get("clip_range", 0.2),
        ent_coef=wandb.config.get("ent_coef", 0.01),
        vf_coef=wandb.config.get("vf_coef", 0.5),
        max_grad_norm=wandb.config.get("max_grad_norm", 0.5),
        policy_kwargs=policy_kwargs,
        verbose=1,
        tensorboard_log=f"runs/{run.id}",
    )
    
    print(f"{Colors.GREEN}MaskablePPO model created{Colors.ENDC}")
    
    # Setup callbacks
    callback = CallbackList([
        MetricsLoggerCallback(),
        WandbCallback(
            model_save_path=f"models/{run.id}",
            model_save_freq=50_000,
            verbose=1,
            log="parameters",
        )
    ])
    
    # Train
    print(f"{Colors.YELLOW}{Colors.BLINK}Starting training...{Colors.ENDC}")
    model.learn(total_timesteps=N_STEPS_RL, callback=callback)
    
    # Save final model
    final_model_path = f"models/{run.id}/final_model.zip"
    model.save(final_model_path)
    
    art = wandb.Artifact(
        name=f"MaskablePPO-{run.id}-final",
        type="model",
        description=f"Final Maskable PPO after {N_STEPS_RL} steps"
    )
    art.add_file(final_model_path)
    run.log_artifact(art)
    wandb.finish()

# =====================================================
# 4. EXAMPLE WANDB CONFIG FOR MASKABLE PPO
# =====================================================

MASKABLE_PPO_CONFIG = {
    "algo": "MaskablePPO",
    "learning_rate": 3e-4,
    "gamma": 0.99,
    "gae_lambda": 0.95,
    "n_steps": 2048,  # Steps per environment per update
    "batch_size": 64,
    "n_epochs": 10,   # Number of epochs per update
    "clip_range": 0.2,
    "ent_coef": 0.01,  # Entropy coefficient
    "vf_coef": 0.5,    # Value function coefficient
    "max_grad_norm": 0.5,
    "net_arch": [256, 256],
    "use_vectorized_obs": True,
}



# --- START NEW CLASS ---
class HeuristicActionWrapper(gym.Wrapper):
    """
    A wrapper that uses the `action_mask` from the `info` dict to modify
    the continuous action from the agent before it's passed to the environment.
    
    It implements the "Hard Snapping" strategy.
    """
    def __init__(self, env: gym.Env, bitrate_ladder_mbps: list[float]):
        super().__init__(env)
        self.bitrate_ladder = bitrate_ladder_mbps
        # This will hold the mask for the *current* observation
        self.current_mask = None
        # Store original action for logging
        self.last_raw_action = None

    def reset(self, **kwargs):
        """
        Reset the underlying environment and store the first action mask.
        """
        obs, info = self.env.reset(**kwargs)
        self.current_mask = info.get("action_mask")
        self.last_raw_action = None
        
        if self.current_mask:
            print(f"{Colors.MAGENTA}[Wrapper] Got initial mask: {self.current_mask}{Colors.ENDC}")
            
        return obs, info

    def step(self, action):
        """
        1. Snap the agent's `action` using the `current_mask`.
        2. Pass the `snapped_action` to the real environment.
        3. Get the `info` dict and store the *next* mask.
        """
        # `action` is the raw output from the SAC/TD3 policy
        raw_action_mbps = float(np.asarray(action).ravel()[0])
        self.last_raw_action = raw_action_mbps
        
        snapped_action = self._snap_action(raw_action_mbps)
        
        # Pass the *modified* action to the environment
        next_obs, reward, done, truncated, info = self.env.step(snapped_action)
        
        # Store the mask for the *next* step
        self.current_mask = info.get("action_mask")
        
        # Log the raw vs. snapped action
        if wandb.run is not None:
            wandb.log({
                "train/action_raw": raw_action_mbps,
                "train/action_snapped": snapped_action[0]
            })

        return next_obs, reward, done, truncated, info

    def _snap_action(self, raw_action_mbps: float) -> np.ndarray:
        """
        Applies the masking logic.
        You can edit this method to implement your other ideas.
        """
        if self.current_mask is None:
            # No mask available (e.g., first step, or Rust didn't send one)
            return np.array([raw_action_mbps], dtype=np.float32)

        print( f'current_mask is:\t{self.current_mask}')
        # Get all allowed bitrate values from the ladder
        allowed_mbps = [
            mbps for mbps, is_allowed in zip(self.bitrate_ladder, self.current_mask)
            if is_allowed
        ]

        if not allowed_mbps:
            # Fallback: Mask is all zeros (shouldn't happen) or empty.
            # Let the agent's raw action pass through.
            return np.array([raw_action_mbps], dtype=np.float32)

        # --- STRATEGY 1: Hard Snapping (Default) ---
        # Find the allowed bitrate closest to the agent's raw action.
        # snapped_mbps = min(allowed_mbps, key=lambda x: abs(x - raw_action_mbps))
        # final_action = snapped_mbps
        
        # if abs(raw_action_mbps - snapped_mbps) > 0.1: # Log if snapping occurred
        #      print(f"{Colors.RED}[Wrapper] Snap: raw {raw_action_mbps:.2f} -> {snapped_mbps:.2f} (Mask: {self.current_mask}){Colors.ENDC}")

        # --- STRATEGY 2: Soft Guidance (Proximal Policy) ---
        # Uncomment this block to "pull" the agent's action toward the heuristic.
        alpha = 0.5 # 0.0 = full agent, 1.0 = full heuristic
        nearest_heuristic = min(allowed_mbps, key=lambda x: abs(x - raw_action_mbps))
        final_action = (1.0 - alpha) * raw_action_mbps + alpha * nearest_heuristic
        final_action = np.clip(final_action, ACT_MIN_MBPS, ACT_MAX_MBPS)

        print(f'{Colors.BLUE}raw: {raw_action_mbps}, nearest heuristic: {nearest_heuristic}, final_action: {final_action} {Colors.ENDC}')

        # --- STRATEGY 3: Noisy Heuristic ---
        # Uncomment this block to pick the nearest heuristic and add noise.
        # noise_std_dev = 1.0 # In Mbps
        # nearest_heuristic = min(allowed_mmbps, key=lambda x: abs(x - raw_action_mbps))
        # final_action = nearest_heuristic + np.random.normal(0.0, noise_std_dev)
        # final_action = np.clip(final_action, ACT_MIN_MBPS, ACT_MAX_MBPS)


        # Return the final action in the correct gym shape
        return np.array([final_action], dtype=np.float32)

class MetricsLoggerCallback(BaseCallback):
    def __init__(self, verbose=0):
        super().__init__(verbose)
    
    def _on_step(self) -> bool:
        # Only log periodically (every 1000 steps)
        if self.n_calls % 1000 == 0:
            logs = {}

            # --- Entropy loss & KL divergence ---
            if hasattr(self.model, "entropy_loss"):
                logs["train/entropy_loss"] = float(self.model.entropy_loss)
            if hasattr(self.model, "kl_divergence"):
                logs["train/kl_divergence"] = float(self.model.kl_divergence)

            # --- Policy loss, value loss, etc. (SAC/TD3 specific) ---
            if hasattr(self.model, "logger"):
                kv = self.model.logger.name_to_value
                for key in ["train/value_loss", "train/policy_loss", "train/entropy_loss"]:
                    if key in kv:
                        logs[key] = kv[key]

            # --- Explained variance ---
            try:
                ev = self.model.logger.name_to_value.get("train/explained_variance", None)
                if ev is not None:
                    logs["train/explained_variance"] = ev
            except Exception:
                pass

            if len(logs) > 0:
                wandb.log(logs, step=self.num_timesteps)
        return True


def train_agent_single(action_ep: str, step_ep: str):  # Renamed for clarity
    # print(f"TRAINER THREAD: Started. Connecting to {trainer_ep}", )
    run = wandb.init(
        project=os.environ.get("WANDB_PROJECT", "xr-abr"),
        entity=os.environ.get("WANDB_ENTITY"),
        save_code=True,
    )

    # --- 1. Set up Environment and Base Policy Kwargs ---
    use_vec = wandb.config.get("use_vectorized_obs", True)
    # if use_vec:
    print(f"{Colors.BLUE}Using windowed observations (last row via extractor).{Colors.ENDC}",  flush = True)
    env = SimpleDirectZmqEnv(action_ep, step_ep)

    print(f"{Colors.MAGENTA}Wrapping environment with HeuristicActionWrapper.{Colors.ENDC}", flush=True)
    env = HeuristicActionWrapper(env, BITRATE_LADDER_MBPS)    
    policy = "MlpPolicy"
    policy_kwargs = dict(
        features_extractor_class=LastRowExtractor,
        net_arch=list(wandb.config.net_arch),
    )
    # else:
    #     print(f"{Colors.BLUE}Using single-frame observations.{Colors.ENDC}")
    #     env = ZmqEnvClient(trainer_ep)
    #     policy = "MlpPolicy"
    #     policy_kwargs = dict(net_arch=list(wandb.config.net_arch))

    gradnorm = wandb.config.get("max_grad_norm", 1.0)
    # --- 2. Build Common Model Parameters ---
    # These are shared by both SAC and TD3
    model_kwargs = {
        "policy": policy,
        "env": env,
        "learning_rate": wandb.config.learning_rate,
        "gamma": wandb.config.gamma,
        "tau": wandb.config.tau,
        "buffer_size": wandb.config.buffer_size,
        "learning_starts": wandb.config.learning_starts,
        "batch_size": wandb.config.batch_size,
        "train_freq": wandb.config.train_freq,
        "gradient_steps": wandb.config.gradient_steps,
        "verbose": 1,
        "tensorboard_log": f"runs/{run.id}",
        # "max_grad_norm": gradnorm, # Used by both
    }

    # --- 3. Add Algorithm-Specific Parameters ---
    algo = wandb.config.get("algo", "SAC") # Default to SAC if not specified
    if algo == "SAC":
        model_class = SAC
        # print(f"{Colors.YELLOW}saaac")

        # Add SAC-specific params to policy_kwargs
        policy_kwargs['log_std_init'] = wandb.config.log_std_init
        
        # Add SAC-specific params to model_kwargs
        model_kwargs['ent_coef'] = wandb.config.ent_coef
        model_kwargs['target_entropy'] = wandb.config.target_entropy
        # model_kwargs['max_grad_norm'] = wandb.config.max_grad_norm

        print(f"{Colors.GREEN}Creating SAC model.{Colors.ENDC}", flush = True)
        
    elif algo == "TD3":
        model_class = TD3
        # print(f"{Colors.YELLOW}td33")
        # TD3 requires action noise. We can make its standard deviation tunable.
        n_actions = env.action_space.shape[-1]
        noise_sigma = wandb.config.get("action_noise_sigma", 0.4) # Get from config or use 0.1
        model_kwargs['action_noise'] = NormalActionNoise(
            mean=np.zeros(n_actions), sigma=noise_sigma * np.ones(n_actions)
        )
        # model_kwargs['max_grad_norm'] = wandb.config.max_grad_norm

        print(f"{Colors.GREEN}Creating TD3 model.{Colors.ENDC}",  flush = True)
        
        # Note: TD3 will ignore ent_coef, target_entropy, log_std_init
        # from the wandb.config, which is fine.
        # print("TRAINER THREAD: 2. Model created.")
    else:
        raise ValueError(f"Unknown algorithm: {algo}. Must be 'SAC' or 'TD3'.")
    
    
    print("TRAINER THREAD: 2. Model created.")

    wandb.config.update({
        "reward_mode": reward_mode,        # e.g. 0 naive, 1 normalized, 2 shaping
        "T_ABR": T_ABR,                    # e.g. 0.3
        "observation_type": observation_type,  # 0,1,2 etc.
    }, allow_val_change=True)

    # Add the final policy_kwargs to the model_kwargs
    model_kwargs['policy_kwargs'] = policy_kwargs

    # --- 4. Instantiate the Model ---
    model = model_class(**model_kwargs)

    # --- 5. Set up Callback and Learn ---
    # callback = WandbCallback(
    #     model_save_path=f"models/{run.id}",
    #     model_save_freq=50_000,
    #     verbose=1,
    #     log="parameters", # Be careful: "all" logs gradients and can be very slow/large.
    #                # Consider setting to log=None or log="parameters".
    # )

    callback = CallbackList([
    MetricsLoggerCallback(),
    WandbCallback(
        model_save_path=f"models/{run.id}",
        model_save_freq=50_000,
        verbose=1,
        log="parameters",
    )
])

    print(f"{Colors.YELLOW}TRAINER THREAD: 3. Calling model.learn()...")
    model.learn(total_timesteps=N_STEPS_RL, callback=callback)

    # --- 6. Save Final Model ---
    final_model_path = f"models/{run.id}/final_model.zip"
    model.save(final_model_path)
    art = wandb.Artifact(
        name=f"{algo.upper()}-{run.id}-final", # Use algo in artifact name
        type="model",
        description=f"Final {algo.upper()} after {N_STEPS_RL} steps"
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
    MAGENTA = '\033[95m'

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

class SimpleDirectZmqEnv(gym.Env):
    """
    FIXED: Handles the 3-frame ROUTER/DEALER pattern correctly.
    
    - Rust (DEALER) sends: [Payload]
    - Python (ROUTER) receives: [Identity, EmptyFrame, Payload]
    
    - Python (ROUTER) sends: [Identity, EmptyFrame, Payload]
    - Rust (DEALER) receives: [Payload]
    """
    metadata = {"render_modes": []}

    def __init__(self, action_ep: str, transition_ep: str):
        super().__init__()
        print(f'initializing SimpleDirectZMQENV', flush=True)

        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32
        )
        self.action_space = spaces.Box(
            low=np.array([ACT_MIN_MBPS], dtype=np.float32),
            high=np.array([ACT_MAX_MBPS], dtype=np.float32),
            dtype=np.float32,
            shape=(1,),
        )

        self.ctx = zmq.Context()
        
        # ROUTER socket - receives obs from Rust DEALER, sends actions back
        self.action_socket = self.ctx.socket(zmq.ROUTER)
        # self.action_socket.set_rcvtimeo(1000)  # 30 second timeout
        # self.action_socket.setsockopt(zmq.LINGER, 0)
        self.action_socket.bind(action_ep)
        
        # PULL socket - receives transitions from Rust PUSH
        self.transition_socket = self.ctx.socket(zmq.PULL)
        # self.transition_socket.set_rcvtimeo(1000)  # 30 second timeout
        # self.transition_socket.setsockopt(zmq.LINGER, 0)
        self.transition_socket.bind(transition_ep)

        self.step_count = 0
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0
        
        # This will hold the (identity, obs, meta) for the *next* step
        # It is populated by reset() and by the end of step()
        self.pending_obs_info = None  
        self.last_obs_for_done = np.zeros(OBSERVATION_SHAPE, dtype=np.float32)

        print(f"✅ Python Server listening:", flush = True)
        print(f"   Action ROUTER ← {action_ep}", flush= True)
        print(f"   Transition PULL ← {transition_ep}", flush = True)

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

    def reset(self, *, seed=None, options=None):
        """
        Reset waits for Rust to send its first observation,
        then returns it so the RL agent can compute the first action.
        """
        super().reset(seed=seed)
        print(f"\n--- Episode boundary (Python) ---")
        self.ep_return = 0.0
        self.ep_len = 0
        
        # If reset is called after a `done`, pending_obs_info might be None.
        # If reset is called mid-episode (by a wrapper), we clear the old one.
        self.pending_obs_info = None 
        
        # Wait for Rust to send initial observation
        try:
            print("🔄 Waiting for Rust to send initial observation...")
            
            # --- FIX: Receive all 3 frames ---
            parts = self.action_socket.recv_multipart()
            # dump_frames("ROUTER RECV reset", parts)

            # parts is a list of frames. Router/Dealer patterns can be
            # either [identity, payload] or [identity, b'', payload].
            if len(parts) == 2:
                identity, obs_msg = parts
            elif len(parts) == 3 and parts[1] == b'':
                identity, _, obs_msg = parts
            else:
                # Unexpected shape — log and try to be informative
                raise RuntimeError(f"Unexpected multipart frame count: {len(parts)} frames: {parts}")
            # dump_frames("ROUTER SEND reset-response", [identity, b'', json.dumps(action_response).encode()])

            obs_request = json.loads(obs_msg)
            action_mask = obs_request.get("action_mask")
            
            # Parse and store
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            flat_obs, meta = self._parse_obs_payload(obs_payload)
            
            # Store for step() to use
            self.pending_obs_info = (identity, flat_obs, meta)
            self.last_obs_for_done = flat_obs.copy()
            
            print("✅ Received initial observation from Rust")
            info = {"obs_meta": meta, "action_mask": action_mask}
            return flat_obs, info
            
        except zmq.ZMQError as e:
            print(f"❌ Error during reset: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"❌ JSON Error during reset: {e}. Received: {obs_msg!r}")
            raise

    def step(self, action):
        """
        Send the action for the pending observation,
        then wait for the *transition* first.
        If not done, *then* wait for the next observation.
        """
        t0 = time.time()
        
        # Clamp action
        action_val = float(np.asarray(action).ravel()[0])
        action_val = max(ACT_MIN_MBPS, min(ACT_MAX_MBPS, action_val))

        # Send action for the pending observation
        if self.pending_obs_info is None:
            raise RuntimeError("step() called before reset() or after an episode finished.")
        
        identity, current_obs, current_meta = self.pending_obs_info
        self.pending_obs_info = None # Clear it, we've used it
        
        # Send action response to Rust
        action_response = {"bitrate_mbps": action_val}

        try:
            # --- FIX: Send all 3 frames ---
            # self.action_socket.send(identity, zmq.SNDMORE)      # Frame 1: Identity
            # self.action_socket.send(b'', zmq.SNDMORE)           # Frame 2: Empty Delimiter
            # self.action_socket.send_json(action_response)       # Frame 3: Payload
            msg = [
                identity, 
                b'', 
                json.dumps(action_response).encode('utf-8')
            ]
            self.action_socket.send_multipart(msg)
            
            # --- End Fix ---
        except zmq.ZMQError as e:
            print(f"❌ Error sending action: {e}")
            raise

        action_latency_ms = (time.time() - t0) * 1000.0
        next_action_mask = None

        t2 = time.time()
        try:
            transition = self.transition_socket.recv_json()
        except zmq.ZMQError as e:
            print(f"❌ Error receiving transition: {e}")
            raise
        
        transition_latency_ms = (time.time() - t2) * 1000.0

        # Extract transition data
        reward = float(transition["reward"])
        done = bool(transition["done"])
        truncated = False # Assuming no truncation from sim

        # Stats
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward
        self.step_count += 1

        # Now, handle logic based on `done`
        if done:

            print("✅ Received final transition (done=True)")
            next_obs = self.last_obs_for_done # Return last valid obs
            next_meta = current_meta
            self.pending_obs_info = None # Ensure it's clear for reset()
        
        else:
            # --- EPISODE CONTINUES ---
            # The Rust sim is still running and *will* send a new obs.
            # Now we wait for the NEXT observation.
            t1 = time.time()
            try:
                # --- FIX: Receive all 3 frames ---
                parts = self.action_socket.recv_multipart()
                # dump_frames("ROUTER RECV next-obs", parts)
                # parts is a list of frames. Router/Dealer patterns can be
                # either [identity, payload] or [identity, b'', payload].
                if len(parts) == 2:
                    identity, obs_msg = parts
                elif len(parts) == 3 and parts[1] == b'':
                    identity, _, obs_msg = parts
                else:
                    # Unexpected shape — log and try to be informative
                    raise RuntimeError(f"Unexpected multipart frame count: {len(parts)} frames: {parts}")
        
                obs_request = json.loads(obs_msg)
                next_action_mask = obs_request.get("action_mask")
                
            except zmq.ZMQError as e:
                print(f"❌ Error receiving next observation: {e}")
                raise
            except json.JSONDecodeError as e:
                print(f"❌ JSON Error in step: {e}. Received: {obs_msg!r}")
                raise
            except Exception as e:
                print(f"[JSON-ERROR] Failed to parse obs_msg during step: {e}. raw={obs_msg!r}")
                raise
            obs_recv_latency = (time.time() - t1) * 1000.0

            # Parse next observation
            obs_payload = obs_request.get("obs_flat") or obs_request.get("obs", obs_request)
            next_obs, next_meta = self._parse_obs_payload(obs_payload)

            # Store next obs for the *next* step() call
            self.pending_obs_info = (identity, next_obs, next_meta)
            self.last_obs_for_done = next_obs.copy()
            
            log_dict = {
                "timing/obs_recv_latency_ms": obs_recv_latency,
                "obs/seq_len": next_meta["seq_len"],
            }
            if wandb.run is not None:
                wandb.log(log_dict)


        # Logging (common to both paths)
        log_dict = {
            "train/reward": reward,
            "train/return_cumsum": self.run_return_cumsum,
            "train/action": action_val,
            "train/done": int(done),
            "timing/action_latency_ms": action_latency_ms,
            "timing/transition_latency_ms": transition_latency_ms,
        }

        self._log_last_row(log_dict, next_obs)


        for k, v in transition.items():
            if k in ("reward", "done", "next_obs", "prev_obs", "action", "sim_id"):
                continue
            if isinstance(v, (int, float)):
                log_dict[f"sim/{k}"] = v

        if wandb.run is not None:
            wandb.log(log_dict)

        if done:
            if wandb.run is not None:
                wandb.log({"episode/return": self.ep_return, "episode/len": self.ep_len})
                wandb.run.summary["episodes"] = wandb.run.summary.get("episodes", 0) + 1
            self.ep_return = 0.0
            self.ep_len = 0

        info = {"obs_meta": next_meta, "action_mask": next_action_mask}
        return next_obs, reward, done, truncated, info

    def close(self):
        self.action_socket.close()
        self.transition_socket.close()
        self.ctx.term()





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
    # trainer_ep = f"ipc:///tmp/xr_{RUN_ID}_trainer"

    trainer_process = mp.Process(
        target=train_maskable_ppo, 
        args=(action_ep, step_ep),
        daemon=True # Make it a daemon so it exits when the main script exits
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
             intrarefresh, ABR, nest_profile, rate_bps_src_BG, pl_prob, observation_type) = combo

            argv = [
                f"{simtime}", "12000.0", "10000", f"{distance}", f"{bitrate}",
                f"{pl_prob}", f"{nxr}", f"{nbg}", f"{rate_bps_src_BG}", f"{is_ul}",
                f"{test}", f"{video_sample}", f"{FPS}", f"{close_users}", f"{close_distance}",
                f"{seed}", f"{gop}", f"{intrarefresh}", f"{ABR}", f"{nest_profile}",
                "1", f"{sim_count}", f"{observation_type}", f"{reward_mode}", f"{T_ABR}", 
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


import threading

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
        ABR_ENABLED, nest_profiles, rate_bps_src_BG, PL, observation_type, 
    ))

    random.shuffle(combos)  # optional

    print(f"***********************************\n************NUMBER OF COMBOS: {len(combos)}   ***********")
    
    rebuild_rust_binary(EXAMPLE_NAME)
    exe = find_exe(release=True)
    train_over_all_combos_iter(exe, combos)

    # === 4️⃣ Close ZMQ server ===
    # server.close()
    print("🧹 All episodes finished. Server closed.")

if __name__ == "__main__":

        
    atexit.register(cleanup_rust_processes)

    # Handle Ctrl-C / SIGTERM gracefully
    signal.signal(signal.SIGINT, lambda sig, frame: (print("\n[CTRL-C] stopping…"), cleanup_rust_processes(), exit(0)))
    signal.signal(signal.SIGTERM, lambda sig, frame: (print("\n[SIGTERM] stopping…"), cleanup_rust_processes(), exit(0)))    
    
    main()

