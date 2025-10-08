import zmq
import json
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import pprint
from stable_baselines3 import DQN

from stable_baselines3 import PPO

# --- NEW: W&B imports ---
import os
import time
import wandb
from wandb.integration.sb3 import WandbCallback
import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"   # 0=all, 1=INFO off, 2=INFO+WARN off, 3=INFO+WARN+ERROR off
# Optional: removes the oneDNN message by disabling it (may reduce perf if you were using TF)
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

ACTION_ENDPOINT = "tcp://*:5555"  # Python BINDs a ROUTER here (was REP)
STEP_ENDPOINT   = "tcp://*:5556"  # Python BINDs a PULL here

import os, socket, json, zmq, tempfile

def bind_pair(ctx, base_dir=None, use_ipc=True):
    if use_ipc and "SLURM_TMPDIR" in os.environ:
        base_dir = os.environ["SLURM_TMPDIR"]
        action_ep = f"ipc://{base_dir}/zmq_action_{os.getpid()}.sock"
        step_ep   = f"ipc://{base_dir}/zmq_step_{os.getpid()}.sock"
        router = ctx.socket(zmq.ROUTER); router.bind(action_ep)
        pull   = ctx.socket(zmq.PULL);   pull.bind(step_ep)
    else:
        # tcp fallback (multi-node)
        host = socket.gethostbyname(socket.gethostname())
        router = ctx.socket(zmq.ROUTER)
        action_port = router.bind_to_random_port(f"tcp://{host}")
        pull   = ctx.socket(zmq.PULL)
        step_port   = pull.bind_to_random_port(f"tcp://{host}")
        action_ep = f"tcp://{host}:{action_port}"
        step_ep   = f"tcp://{host}:{step_port}"

    return router, pull, action_ep, step_ep

def write_manifest(action_ep, step_ep, path=None):
    if path is None:
        # job-scoped temp area is best
        base = os.environ.get("SLURM_TMPDIR", tempfile.gettempdir())
        path = os.path.join(base, f"zmq_manifest_{os.getpid()}.json")
    with open(path, "w") as f:
        json.dump({"ACTION_ENDPOINT": action_ep, "STEP_ENDPOINT": step_ep}, f)
    print(f"[ZMQ] Wrote manifest: {path}")
    return path







class ZmqEnvServer(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self, log_to_wandb: bool = True):
        super().__init__()
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32)
        self.action_space = spaces.Discrete(ACTION_DIM)

        self.ctx = zmq.Context.instance()

        # ROUTER instead of REP
        self.router = self.ctx.socket(zmq.ROUTER)
        self.router.bind(ACTION_ENDPOINT)

        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.bind(STEP_ENDPOINT)

        # Will hold the identity of the sim this Gym env talks to
        self.active_sim_id = None

        # --- NEW: local counters for episode stats + step index ---
        self.global_step = 0
        self.ep_return = 0.0
        self.ep_len = 0
        self.run_return_cumsum = 0.0
        self.log_to_wandb = log_to_wandb and (wandb.run is not None)

        print("✅ Python ZMQ Server (ROUTER/PULL) is ready. Start the Rust simulation(s).")

    def _obs_from_json(self, obs_json):
        # Keep original order if you like, but SB3 only needs a flat np.array
        return np.array([obs_json[k] for k in sorted(obs_json)], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        print(f"\n{Colors.YELLOW}--- Episode boundary ---{Colors.ENDC}")
        print("PY: Waiting for FIRST action request from Rust...")

        # Reset episode counters
        self.ep_return = 0.0
        self.ep_len = 0

        # ROUTER: receive [identity, payload]
        t0 = time.time()
        parts = self.router.recv_multipart()
        recv_latency_ms = (time.time() - t0) * 1000.0

        sim_id, payload = parts[0], parts[-1]
        req = json.loads(payload.decode("utf-8"))
        initial_obs_dict = req["obs"]
        initial_obs_arr = self._obs_from_json(initial_obs_dict)

        # Remember which sim this env instance is serving
        self.active_sim_id = sim_id

        # ROUTER reply must include identity
        self.router.send_multipart([self.active_sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])

        print(f"{Colors.BLUE}PY: Initial observation received from {sim_id!r}:{Colors.ENDC}")
        print(f"{Colors.BLUE}{pprint.pformat(initial_obs_dict)}{Colors.ENDC}")
        print(f"{Colors.GREEN}PY: Sent dummy action 0 to unblock Rust.{Colors.ENDC}")

        # --- NEW: log reset/handshake timing ---
        if self.log_to_wandb:
            wandb.log(
                {
                    "env/reset_recv_latency_ms": recv_latency_ms,
                    "env/episode": wandb.run.summary.get("episodes", 0) + 1
                },
                # step=self.global_step
            )

        return initial_obs_arr, {}

    def step(self, action):
        # 1) Wait for transition from Rust (PULL)
        print("PY: Waiting for transition from Rust...")
        t0 = time.time()
        transition = self.pull_socket.recv_json()
        pull_latency_ms = (time.time() - t0) * 1000.0

        # If multiple sims push transitions, ignore those not matching this env
        sim_id_field = transition.get("sim_id")
        if sim_id_field is not None and self.active_sim_id is not None:
            # active_sim_id is bytes (ROUTER identity); normalize for compare
            active = self.active_sim_id.decode("utf-8", errors="ignore")
            if sim_id_field != active:
                # Ignore and keep waiting for the correct sim
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

        # Accumulate episode stats
        self.ep_return += reward
        self.ep_len += 1
        self.global_step += 1
        self.run_return_cumsum += reward  # NEW

        # 2) If not done, receive next observation request on ROUTER and reply with chosen action
        router_roundtrip_ms = None
        if not done:
            # ROUTER receive must bring the same identity for this env's sim
            t1 = time.time()
            parts = self.router.recv_multipart()
            sim_id, payload = parts[0], parts[-1]

            # If multiple sims are connected, loop until we get our active one
            while self.active_sim_id is not None and sim_id != self.active_sim_id:
                # Keep other sims alive with a noop
                self.router.send_multipart([sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])
                parts = self.router.recv_multipart()
                sim_id, payload = parts[0], parts[-1]

            # Reply with the agent's chosen action
            print(f"{Colors.GREEN}PY: Replying to {sim_id!r} with action: {action}{Colors.ENDC}")
            self.router.send_multipart([sim_id, json.dumps({"action_idx": int(action)}).encode("utf-8")])
            router_roundtrip_ms = (time.time() - t1) * 1000.0
        else:
            print(f"{Colors.YELLOW}PY: Episode finished (done=True).{Colors.ENDC}")

        # --- NEW: log to W&B each step ---
        if self.log_to_wandb:
            log_dict = {
                "train/reward": reward,
                "train/return_cumsum": self.run_return_cumsum,   # NEW
                "train/done": int(done),
                "train/action": int(action),
                "timing/pull_latency_ms": pull_latency_ms,
            }
            # if router_roundtrip_ms is not None:
            #     log_dict["timing/router_roundtrip_ms"] = router_roundtrip_ms

            # Include sim_id and any extra transition fields from Rust under "sim/*"
            if sim_id_field is not None:
                log_dict["sim/id"] = sim_id_field
            for k, v in transition.items():
                if k in ("reward", "done", "next_obs", "sim_id"):
                    continue
                # pass through scalars
                if isinstance(v, (int, float)):
                    log_dict[f"sim/{k}"] = v

            # Optional: log a few next_obs entries for debugging (won't spam with all 9 every step)
            # Comment out if too chatty:
            try:
                for i, (k, v) in enumerate(sorted(next_obs_dict.items())):
                    # if i >= 3: break
                    log_dict[f"obs_preview/{k}"] = float(v)
            except Exception:
                pass

            wandb.log(log_dict)

        # If episode ended, push episode summary
        if done and self.log_to_wandb:
            wandb.log(
                {
                    "episode/return": self.ep_return,
                    "episode/len": self.ep_len,
                },
                # step=self.global_step,
            )
            # update run summary counters
            wandb.run.summary["episodes"] = wandb.run.summary.get("episodes", 0) + 1

        return next_obs_arr, reward, done, truncated, info

    def close(self):
        self.router.close(0)
        self.pull_socket.close(0)
        self.ctx.term()


def main():
    # 1) Start W&B run
    run = wandb.init(
        project=os.environ.get("WANDB_PROJECT", "xr-abr"),
        entity=os.environ.get("WANDB_ENTITY"),
        name=os.environ.get("WANDB_NAME", f"dqn_zmq_{int(time.time())}"),
        config={
            "algo": "DQN",
            "policy": "MlpPolicy",
            "action_dim": ACTION_DIM,
            "obs_shape": OBSERVATION_SHAPE,
            "learning_starts": 1000,
            "train_freq": "1 step",
            "batch_size": 256,
            "buffer_size": 100_000,
            "net_arch": [256, 256],
        },
        save_code=True,
    )

    # 2) Define TB logdir BEFORE patching
    tb_logdir = os.path.join("tb", run.id)
    os.makedirs(tb_logdir, exist_ok=True)

    # 3) Tell W&B to watch that TB dir
    # wandb.tensorboard.patch(root_logdir=tb_logdir)

    # 4) Build env/model using that TB dir
    env = ZmqEnvServer(log_to_wandb=True)

    model = DQN(
        "MlpPolicy",
        env,
        learning_starts=wandb.config.learning_starts,
        train_freq=(1, "step"),
        batch_size=wandb.config.batch_size,
        buffer_size=wandb.config.buffer_size,
        policy_kwargs=dict(net_arch=list(wandb.config.net_arch)),
        verbose=1,
        # tensorboard_log=tb_logdir,  # SB3 → TensorBoard
    )

    callback = WandbCallback(
        gradient_save_freq=0,
        model_save_path=f"models/{run.id}",
        model_save_freq=10_000,
        verbose=2,
        log="all",
    )

    total_steps = 7_500_000

    model.learn(total_timesteps=total_steps, callback=callback)
    model.save("dqn_bitrate_agent")

    art = wandb.Artifact("dqn_bitrate_agent", type="model")
    art.add_file("dqn_bitrate_agent.zip")
    wandb.log_artifact(art)

    wandb.finish()


if __name__ == "__main__":
    main()
