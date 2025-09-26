import zmq
import json
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import pprint  # <-- CORRECTED IMPORT
from stable_baselines3 import DQN


class Colors:
    """ANSI color codes"""
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m' # Resets the color


# The observation shape should match the flattened array from your Rust struct.
# Your RLObservation has 9 fields.
OBSERVATION_SHAPE = (9,)
ACTION_DIM = 10  # Your bitrate ladder size

# NOTE: The socket roles are now FLIPPED
ACTION_ENDPOINT = "tcp://*:5555"  # Python BINDs a REP socket here
STEP_ENDPOINT = "tcp://*:5556"    # Python BINDs a PULL socket here


class ZmqEnvServer(gym.Env):
    metadata = {"render_modes": []}

    def __init__(self):
        super().__init__()
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=OBSERVATION_SHAPE, dtype=np.float32
        )
        self.action_space = spaces.Discrete(ACTION_DIM)

        self.ctx = zmq.Context()
        self.rep_socket = self.ctx.socket(zmq.REP)
        self.rep_socket.bind(ACTION_ENDPOINT)
        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.bind(STEP_ENDPOINT)
        print("✅ Python ZMQ Server is ready. Start the Rust simulation.")

    def _obs_from_json(self, obs_json):
        # Using sorted keys ensures the numpy array order is always consistent.
        return np.array([obs_json[k] for k in sorted(obs_json)], dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        """
        Handles the start of a new episode.
        """
        super().reset(seed=seed)
        print(f"\n{Colors.YELLOW}--- Episode boundary ---{Colors.ENDC}")
        print("PY: Waiting for the FIRST action request from Rust to start the episode...")
        
        # 1. Wait for Rust's very first 'select_action' request.
        # This provides the initial observation for the episode.
        request = self.rep_socket.recv_json()
        initial_obs_dict = request["obs"]      
        initial_obs_arr = self._obs_from_json(initial_obs_dict)

        # 2. Rust's REQ socket REQUIRES a reply to unblock.
        # We send a dummy/default action (e.g., action 0) just to complete the ZMQ cycle.
        # Rust will perform this action, and the resulting transition will be
        # handled by the first call to step().
        self.rep_socket.send_json({"action_idx": 0})
        
        print(f"{Colors.BLUE}PY: Initial observation received:{Colors.ENDC}")
        # This line will now work correctly
        print(f"{Colors.BLUE}{pprint.pformat(initial_obs_dict)}{Colors.ENDC}") 
        print(f"{Colors.GREEN}PY: Sent dummy action 0 to unblock Rust.{Colors.ENDC}")

        # 3. Return the initial observation to the Stable Baselines 3 agent.
        return initial_obs_arr, {}

    def step(self, action):
        """
        Handles one step of the environment interaction.
        """
        # By the time we are here, the agent has chosen a real 'action' based on
        # the observation returned by `reset` or the previous `step`.

        # 1. Wait for the transition data that resulted from the *previous* action.
        # On the first step, this is the transition from the dummy action 0.
        print("PY: Waiting for transition from Rust...")
        transition = self.pull_socket.recv_json()
        
        reward = transition["reward"]
        done = transition["done"]
        next_obs_dict = transition["next_obs"] 
        next_obs_arr = self._obs_from_json(next_obs_dict)
        truncated = False 
        info = {}

        print(f"{Colors.BLUE}PY: Received Step Data:{Colors.ENDC}")
        step_data_to_print = {
            "reward": reward,
            "done": done,
            "next_obs": next_obs_dict
        }
        # This line will also work correctly now
        print(f"{Colors.BLUE}{pprint.pformat(step_data_to_print)}{Colors.ENDC}")

        # 2. If the episode is not over, Rust is now waiting for its next action.
        # We must complete the REQ/REP cycle by receiving its request and
        # sending the new action the agent has chosen.
        if not done:
            # Receive the new observation from Rust (we discard it because the
            # transition data from the PULL socket is more up-to-date).
            _ = self.rep_socket.recv_json()
            
            # Reply with the agent's chosen action to unblock Rust.
            print(f"{Colors.GREEN}PY: Replying with agent's chosen action: {action}{Colors.ENDC}")
            self.rep_socket.send_json({"action_idx": int(action)})
        else:
            # If the episode IS done, we do nothing. The training loop will call
            # `reset`, which will handle the start of the next episode.
            print(f"{Colors.YELLOW}PY: Episode finished (done=True).{Colors.ENDC}")

        return next_obs_arr, reward, done, truncated, info

    def close(self):
        self.rep_socket.close()
        self.pull_socket.close()
        self.ctx.term()

def main():
    # Use the new ZmqEnvServer
    env = ZmqEnvServer()
    
    model = DQN(
        "MlpPolicy",
        env,
        learning_starts=1000,
        train_freq=(1, "step"), # Train after every step
        batch_size=256,
        buffer_size=100_000,
        policy_kwargs=dict(net_arch=[256, 256]),
        verbose=1,
    )
    model.learn(total_timesteps=500_000)
    model.save("dqn_bitrate_agent")

if __name__ == "__main__":
    main()