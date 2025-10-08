import os
import time
import json
import pprint
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import zmq

# --- Stable Baselines 3 Imports ---
from stable_baselines3 import PPO, DQN, A2C

# --- W&B Imports ---
import wandb
from wandb.integration.sb3 import WandbCallback

# --- Environment Setup (No changes needed here) ---
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
from absl import logging as absl_logging
absl_logging.set_verbosity(absl_logging.ERROR)

class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'

OBSERVATION_SHAPE = (11,)
ACTION_DIM = 20
ACTION_ENDPOINT = "tcp://*:5555"
STEP_ENDPOINT = "tcp://*:5556"

# Endpoint for Python training clients
TRAINER_ENDPOINT = "tcp://*:5557"

TRAINER_ENDPOINT = "tcp://localhost:5557"

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


# -------------------------------------------------------------------
# NEW: Gym-compatible ZMQ Client
# This replaces the old ZmqEnvServer class.
# -------------------------------------------------------------------
class ZmqEnvClient(gym.Env):
    """A gymnasium.Env that acts as a client to the standalone ZmqServer.
    It connects to the server and handles the request-reply communication."""
    metadata = {"render_modes": []}

    def __init__(self):
        super().__init__()
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32)
        self.action_space = spaces.Discrete(ACTION_DIM)

        # This is a REQ socket that connects, not binds
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(TRAINER_ENDPOINT)
        self.step_count = 0 
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0


        print("✅ Python ZMQ Client connected to server.")

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        print(f"\n{Colors.YELLOW}--- Episode boundary ---{Colors.ENDC}")
        self.ep_return = 0.0
        self.ep_len = 0
        self.socket.send_json({"command": "reset"})
        t0 = time.time()
        response = self.socket.recv_json()
        recv_latency_ms = (time.time() - t0) * 1000.0
        initial_obs = np.array(response["obs"], dtype=np.float32)

        # Log reset latency like in DQN-only
        if wandb.run is not None:
            wandb.log({
                "env/reset_recv_latency_ms": recv_latency_ms,
                "env/episode": wandb.run.summary.get("episodes", 0) + 1
            })

        # ✅ return MUST be here, at the end, not inside any 'if'
        return initial_obs, {}

    
    def step(self, action):
        # --- 1) Send step command and receive response ---
        t0 = time.time()
        self.socket.send_json({"command": "step", "action": int(action)})
        response = self.socket.recv_json()
        pull_latency_ms = (time.time() - t0) * 1000.0

        # --- 2) Parse response ---
        next_obs = np.array(response["next_obs"], dtype=np.float32)
        reward = float(response["reward"])
        done = bool(response["done"])
        truncated = False
        info = {}

        # --- 3) Update local stats ---
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward
        self.step_count += 1

        # --- 4) Build log dictionary ---
        log_dict = {
            "train/reward": reward,
            "train/return_cumsum": self.run_return_cumsum,
            "train/action": int(action),
            "train/done": int(done),
            "timing/pull_latency_ms": pull_latency_ms,
        }

        # Add named observation metrics
        for i, v in enumerate(next_obs):
            key = OBSERVATION_KEYS[i] if i < len(OBSERVATION_KEYS) else f"extra_{i}"
            log_dict[f"obs/{key}"] = float(v)

        # Add any other scalar fields the simulator might send
        for k, v in response.items():
            if k in ("reward", "done", "next_obs"):
                continue
            if isinstance(v, (int, float)):
                log_dict[f"sim/{k}"] = v

        # --- 5) Log every step or every N steps ---
        if wandb.run is not None:
            wandb.log(log_dict)

        # --- 6) Episode summary if done ---
        if done:
            wandb.log({
                "episode/return": self.ep_return,
                "episode/len": self.ep_len,
            })
            wandb.run.summary["episodes"] = wandb.run.summary.get("episodes", 0) + 1

            # Reset episode counters
            self.ep_return = 0.0
            self.ep_len = 0

        return next_obs, reward, done, truncated, info

    def close(self):
        self.socket.close()
        self.ctx.term()




# --- Main Training Function for W&B Sweep ---
def train_sweep():
    # 1) Initialize W&B run. The agent will automatically fill config.
    run = wandb.init(
        project=os.environ.get("WANDB_PROJECT", "xr-abr"),
        entity=os.environ.get("WANDB_ENTITY"),
        save_code=True,
    )

    # 2) Build env
    env = ZmqEnvClient()    
    # 3) Select and configure the model based on wandb.config
    model = None
    algo = wandb.config.algorithm

    print(f"{Colors.GREEN}--- Starting run for algorithm: {algo} ---{Colors.ENDC}")
    print(f"{Colors.BLUE}{pprint.pformat(dict(wandb.config))}{Colors.ENDC}")

    if algo == "PPO":
        model = PPO(
            "MlpPolicy", env,
            learning_rate=wandb.config.learning_rate,
            n_steps=wandb.config.n_steps,
            batch_size=wandb.config.batch_size_ppo,
            n_epochs=wandb.config.n_epochs,
            gamma=wandb.config.gamma,
            gae_lambda=wandb.config.gae_lambda,
            clip_range=wandb.config.clip_range,
            policy_kwargs=dict(net_arch=list(wandb.config.net_arch)),
            verbose=1,
        )
    elif algo == "DQN":
        model = DQN(
            "MlpPolicy", env,
            learning_rate=wandb.config.learning_rate,
            buffer_size=wandb.config.buffer_size,
            learning_starts=1000,
            batch_size=wandb.config.batch_size_dqn,
            gamma=wandb.config.gamma,
            train_freq=(1, "step"),
            target_update_interval=wandb.config.target_update_interval,
            exploration_fraction=wandb.config.exploration_fraction,
            exploration_final_eps=wandb.config.exploration_final_eps,
            policy_kwargs=dict(net_arch=list(wandb.config.net_arch)),
            verbose=1,
        )
    elif algo == "A2C":
        model = A2C(
            "MlpPolicy", env,
            learning_rate=wandb.config.learning_rate,
            n_steps=wandb.config.n_steps_a2c,
            gamma=wandb.config.gamma,
            vf_coef=wandb.config.vf_coef,
            ent_coef=wandb.config.ent_coef,
            policy_kwargs=dict(net_arch=list(wandb.config.net_arch)),
            verbose=1,
        )

    # 4) Set up callback and start learning
    callback = WandbCallback(
        model_save_path=f"models/{run.id}",
        model_save_freq=50_000, # Saving less frequently during a sweep is fine
        verbose=2,
        log="all", 
    )

    total_steps = 1_500_000 # Keep this fixed for fair comparison across runs
    model.learn(total_timesteps=total_steps, callback=callback)

    # --- NEW: Save and log the final model artifact ---
    print(f"{Colors.GREEN}--- Training complete. Saving final model. ---{Colors.ENDC}")
    final_model_path = f"models/{run.id}/final_model.zip"
    model.save(final_model_path)
    
    final_artifact = wandb.Artifact(
        name=f"{algo}-{run.id}-final", 
        type="model",
        description=f"Final model for a {algo} run after {total_steps} steps."
    )
    final_artifact.add_file(final_model_path)
    run.log_artifact(final_artifact)
    # --- End of new section ---

    wandb.finish()

if __name__ == "__main__":
    train_sweep()