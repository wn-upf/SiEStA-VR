import zmq
import json
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import pprint
from stable_baselines3 import DQN

class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'

OBSERVATION_SHAPE = (9,)
ACTION_DIM = 10

ACTION_ENDPOINT = "tcp://*:5555"  # Python BINDs a ROUTER here (was REP)
STEP_ENDPOINT   = "tcp://*:5556"  # Python BINDs a PULL here

class ZmqEnvServer(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self):
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

        print("✅ Python ZMQ Server (ROUTER/PULL) is ready. Start the Rust simulation(s).")

    def _obs_from_json(self, obs_json):
        return np.array([obs_json[k] for k in sorted(obs_json)], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        print(f"\n{Colors.YELLOW}--- Episode boundary ---{Colors.ENDC}")
        print("PY: Waiting for FIRST action request from Rust...")

        # ROUTER: receive [identity, payload]
        parts = self.router.recv_multipart()
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

        return initial_obs_arr, {}

    def step(self, action):
        # 1) Wait for transition from Rust (PULL)
        print("PY: Waiting for transition from Rust...")
        transition = self.pull_socket.recv_json()

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

        reward = transition["reward"]
        done = transition["done"]
        next_obs_dict = transition["next_obs"]
        next_obs_arr = self._obs_from_json(next_obs_dict)
        truncated = False
        info = {}

        print(f"{Colors.BLUE}PY: Received Step Data:{Colors.ENDC}")
        print(f"{Colors.BLUE}{pprint.pformat({'sim_id': sim_id_field, 'reward': reward, 'done': done})}{Colors.ENDC}")

        # 2) If not done, receive next observation request on ROUTER and reply with chosen action
        if not done:
            # ROUTER receive must bring the same identity for this env's sim
            parts = self.router.recv_multipart()
            sim_id, payload = parts[0], parts[-1]

            # If multiple sims are connected, loop until we get our active one
            while self.active_sim_id is not None and sim_id != self.active_sim_id:
                # Optionally queue/ignore others for now
                # Just consume and send a noop action back so their REQ/DEALER doesn't stall
                self.router.send_multipart([sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])
                parts = self.router.recv_multipart()
                sim_id, payload = parts[0], parts[-1]

            # Reply with the agent's chosen action
            print(f"{Colors.GREEN}PY: Replying to {sim_id!r} with action: {action}{Colors.ENDC}")
            self.router.send_multipart([sim_id, json.dumps({"action_idx": int(action)}).encode("utf-8")])
        else:
            print(f"{Colors.YELLOW}PY: Episode finished (done=True).{Colors.ENDC}")

        return next_obs_arr, reward, done, truncated, info

    def close(self):
        self.router.close(0)
        self.pull_socket.close(0)
        self.ctx.term()

def main():
    env = ZmqEnvServer()
    model = DQN(
        "MlpPolicy",
        env,
        learning_starts=1000,
        train_freq=(1, "step"),
        batch_size=256,
        buffer_size=100_000,
        policy_kwargs=dict(net_arch=[256, 256]),
        verbose=1,
    )
    model.learn(total_timesteps=500_000)
    model.save("dqn_bitrate_agent")

if __name__ == "__main__":
    main()
