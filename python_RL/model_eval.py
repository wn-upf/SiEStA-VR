import os
import time
import numpy as np
import torch
import gymnasium as gym
import zmq
from stable_baselines3 import PPO, DQN, A2C
from sb3_contrib import RecurrentPPO
import re
# -------------------- SETTINGS --------------------
# RUN_ID = "sv7wpfqy"  # your best-performing run
# ALGO   = "PPO"       # or "RNN_PPO", "DQN", "A2C"
# MODEL_PATH = f"/home/boris/Desktop/Rust_MG1/asynchronix/python_RL/wandb/run-20251010_180918-sv7wpfqy/files/model.zip"  # or final_model.zip if you saved it that way

# Use same endpoints as in training
TRAINER_ENDPOINT = os.environ.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")

# Observation and action sizes must match training
OBSERVATION_SHAPE = (11,)
ACTION_DIM = 20

WINNING_DIR = "/home/boris/Desktop/Rust_MG1/asynchronix/python_RL/winning_runs"

SELECT_INDEX = 3  # to get another run


# -------------------- ENVIRONMENT --------------------
class ZmqEnvClient(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self):
        super().__init__()
        self.observation_space = gym.spaces.Box(low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32)
        self.action_space = gym.spaces.Discrete(ACTION_DIM)
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(TRAINER_ENDPOINT)
        print("✅ Connected to simulator (ZMQ).")

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self.socket.send_json({"command": "reset"})
        response = self.socket.recv_json()
        return np.array(response["obs"], dtype=np.float32), {}

    def step(self, action):
        self.socket.send_json({"command": "step", "action": int(action)})
        response = self.socket.recv_json()
        next_obs = np.array(response["next_obs"], dtype=np.float32)
        reward   = float(response["reward"])
        done     = bool(response["done"])
        truncated = False
        info = {}
        return next_obs, reward, done, truncated, info

    def close(self):
        self.socket.close()
        self.ctx.term()

# -------------------- MAIN EVALUATION --------------------
# def evaluate_model(model_path, algo, run_id):
#     print(f"\n🔍 Loading {algo} model ({run_id}) from {model_path}")
#     env = ZmqEnvClient()

#     # choose algo loader
#     algo_map = {
#         "PPO": PPO,
#         "A2C": A2C,
#         "DQN": DQN,
#         "RNN_PPO": RecurrentPPO
#     }
#     if algo not in algo_map:
#         raise ValueError(f"Unknown algorithm: {algo}")

#     model_cls = algo_map[algo]
#     model = model_cls.load(model_path, env=env, device="cuda" if torch.cuda.is_available() else "cpu")

#     print("✅ Model loaded successfully!!")
#     n_eval_episodes = 10
#     all_returns = []

#     for ep in range(n_eval_episodes):
#         obs, _ = env.reset()
#         done, ep_return = False, 0.0
#         while not done:
#             action, _ = model.predict(obs, deterministic=True)
#             obs, reward, done, _, _ = env.step(action)
#             ep_return += reward
#         print(f"Episode {ep+1}/{n_eval_episodes}: Return = {ep_return:.3f}")
#         all_returns.append(ep_return)

#     env.close()
#     mean_return = np.mean(all_returns)
#     print(f"\n🎯 Average Return over {n_eval_episodes} episodes: {mean_return:.3f}")




if __name__ == "__main__":
    pattern = re.compile(r"([a-z0-9]+)_([A-Za-z0-9_]+)")  # matches runID_algo pattern
    entries = [e for e in os.listdir(WINNING_DIR) if pattern.match(e)]
    entries.sort()  # deterministic order
    print(f"ENTRIEES: {entries}")
    if not entries:
        raise RuntimeError("No valid winning runs found!")

    if SELECT_INDEX < 0 or SELECT_INDEX >= len(entries):
        raise IndexError(f"SELECT_INDEX {SELECT_INDEX} out of range (0-{len(entries)-1})")

    chosen_entry = entries[SELECT_INDEX]
    match = pattern.match(chosen_entry)
    run_id, algo = match.groups()
    model_path = os.path.join(WINNING_DIR, chosen_entry, "model.zip")

    print(f"\n🎯 Selected #{SELECT_INDEX}: {chosen_entry}")
    print(f"    → Run ID: {run_id}")
    print(f"    → Algorithm: {algo}")
    print(f"    → Model path: {model_path}")

    # Load the model
    env = ZmqEnvClient()
    algo_map = {
        "PPO": PPO,
        "A2C": A2C,
        "DQN": DQN,
        "RNN_PPO": RecurrentPPO,
    }

    if algo not in algo_map:
        raise ValueError(f"Unknown algorithm type: {algo}")

    model_cls = algo_map[algo]
    model = model_cls.load(model_path, env=env, device="cuda" if torch.cuda.is_available() else "cpu")

    # Evaluate for 10 episodes
    n_eval_episodes = 1000
    returns = []

    for ep in range(n_eval_episodes):
        obs, _ = env.reset()
        done, ep_return = False, 0.0
        while not done:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, done, _, _ = env.step(action)
            ep_return += reward
        print(f"Episode {ep+1}/{n_eval_episodes}: Return = {ep_return:.3f}")
        returns.append(ep_return)

    env.close()
    mean_return = np.mean(returns)
    print(f"\n✅ {algo} ({run_id}) → Average Return over {n_eval_episodes} episodes: {mean_return:.3f}")