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
class ZmqEnvServer(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self, log_to_wandb: bool = True):
        super().__init__()
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32)
        self.action_space = spaces.Discrete(ACTION_DIM)

        self.ctx = zmq.Context.instance()
        self.router = self.ctx.socket(zmq.ROUTER); self.router.bind(ACTION_ENDPOINT)
        self.pull_socket = self.ctx.socket(zmq.PULL); self.pull_socket.bind(STEP_ENDPOINT)

        self.active_sim_id = None
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0
        self.log_to_wandb = log_to_wandb and (wandb.run is not None)

        print("✅ Python ZMQ Server (ROUTER/PULL) is ready. Start the Rust simulation(s).")

    def _obs_from_json(self, obs_json):
        return np.array([obs_json[k] for k in sorted(obs_json)], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        print(f"\n{Colors.YELLOW}--- Episode boundary ---{Colors.ENDC}")
        print("PY: Waiting for FIRST action request from Rust...")

        self.ep_return = 0.0
        self.ep_len = 0

        t0 = time.time()
        parts = self.router.recv_multipart()
        recv_latency_ms = (time.time() - t0) * 1000.0

        sim_id, payload = parts[0], parts[-1]
        req = json.loads(payload.decode("utf-8"))
        initial_obs_arr = self._obs_from_json(req["obs"])

        self.active_sim_id = sim_id
        self.router.send_multipart([self.active_sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])

        print(f"{Colors.BLUE}PY: Initial observation received from {sim_id!r}{Colors.ENDC}")
        print(f"{Colors.GREEN}PY: Sent dummy action 0 to unblock Rust.{Colors.ENDC}")

        if self.log_to_wandb:
            wandb.log({
                "env/reset_recv_latency_ms": recv_latency_ms,
                "env/episode": wandb.run.summary.get("episodes", 0) + 1
            })

        return initial_obs_arr, {}

    def step(self, action):
        print("PY: Waiting for transition from Rust...")
        t0 = time.time()
        transition = self.pull_socket.recv_json()
        pull_latency_ms = (time.time() - t0) * 1000.0

        sim_id_field = transition.get("sim_id")
        if sim_id_field is not None and self.active_sim_id is not None:
            active = self.active_sim_id.decode("utf-8", errors="ignore")
            while sim_id_field != active:
                transition = self.pull_socket.recv_json()
                sim_id_field = transition.get("sim_id")

        reward = float(transition["reward"])
        done = bool(transition["done"])
        next_obs_dict = transition["next_obs"]
        next_obs_arr = self._obs_from_json(next_obs_dict)
        truncated = False
        info = {}

        print(f"{Colors.BLUE}PY: Received Step Data:{Colors.ENDC}")
        print(f"{Colors.BLUE}{pprint.pformat({'sim_id': sim_id_field, 'reward': reward, 'done': done})}{Colors.ENDC}")

        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward

        router_roundtrip_ms = None
        if not done:
            t1 = time.time()
            parts = self.router.recv_multipart()
            sim_id, payload = parts[0], parts[-1]
            while self.active_sim_id is not None and sim_id != self.active_sim_id:
                self.router.send_multipart([sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])
                parts = self.router.recv_multipart()
                sim_id, payload = parts[0], parts[-1]
            self.router.send_multipart([sim_id, json.dumps({"action_idx": int(action)}).encode("utf-8")])
            router_roundtrip_ms = (time.time() - t1) * 1000.0
        else:
            print(f"{Colors.YELLOW}PY: Episode finished (done=True).{Colors.ENDC}")

        if self.log_to_wandb:
            log_dict = {
                "train/reward": reward,
                "train/return_cumsum": self.run_return_cumsum,
                "train/done": int(done),
                "train/action": int(action),
                "timing/pull_latency_ms": pull_latency_ms,
            }
            if sim_id_field is not None:
                log_dict["sim/id"] = sim_id_field
            for k, v in transition.items():
                if k in ("reward", "done", "next_obs", "sim_id"):
                    continue
                if isinstance(v, (int, float)):
                    log_dict[f"sim/{k}"] = v
            try:
                for i, (k, v) in enumerate(sorted(next_obs_dict.items())):
                    # if i >= 3: break  # un-comment to reduce verbosity
                    log_dict[f"obs_preview/{k}"] = float(v)
            except Exception:
                pass
            # optionally include router RTT:
            # if router_roundtrip_ms is not None:
            #     log_dict["timing/router_roundtrip_ms"] = router_roundtrip_ms

            wandb.log(log_dict)

        if done and self.log_to_wandb:
            wandb.log({"episode/return": self.ep_return, "episode/len": self.ep_len})
            wandb.run.summary["episodes"] = wandb.run.summary.get("episodes", 0) + 1

        return next_obs_arr, reward, done, truncated, info

    def close(self):
        self.router.close(0)
        self.pull_socket.close(0)
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
    env = ZmqEnvServer(log_to_wandb=True)
    
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