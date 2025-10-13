# zmq_server.py

import zmq
import json
import numpy as np
import time
import os
# --- Constants ---
# Endpoints for Rust simulations to connect to
# ACTION_ENDPOINT = "tcp://*:5555"
# STEP_ENDPOINT = "tcp://*:5556"
# Endpoint for Python training clients to connect to
# TRAINER_ENDPOINT = "tcp://*:5557"
ACTION_ENDPOINT  = os.environ.get("ZMQ_ACTION_EP",  "ipc:///tmp/xr_default_action")
STEP_ENDPOINT    = os.environ.get("ZMQ_STEP_EP",    "ipc:///tmp/xr_default_step")
TRAINER_ENDPOINT = os.environ.get("ZMQ_TRAINER_EP", "ipc:///tmp/xr_default_trainer")

def _obs_from_payload_dict(d):
    if "obs_flat" in d:
        return d["obs_flat"]
    if "obs" in d:
        return d["obs"]
    raise KeyError("Neither 'obs_flat' nor 'obs' in payload")



class Colors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'

class ZmqServer:
    """A standalone ZMQ server that acts as a bridge between Rust simulations
    and a Python-based RL training agent."""
    
    def __init__(self):
        self.ctx = zmq.Context()
        # Sockets for Rust Simulations (ROUTER for req/rep, PULL for one-way data)
        self.router = self.ctx.socket(zmq.ROUTER)
        self.router.bind(ACTION_ENDPOINT)
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(STEP_ENDPOINT)
        
        # Socket for Python Trainer Client (REP for req/rep)
        self.rep_socket = self.ctx.socket(zmq.REP)
        self.rep_socket.bind(TRAINER_ENDPOINT)
        
        self.active_sim_id = None
        print(f"{Colors.GREEN}✅ ZMQ Server Bridge is running.{Colors.ENDC}")
        print(f"📡 Listening for Rust sims on {ACTION_ENDPOINT} and {STEP_ENDPOINT}")
        print(f"🤖 Listening for Python trainer on {TRAINER_ENDPOINT}")

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
            print(f"\n{Colors.YELLOW}SERVER: Waiting for command from trainer...{Colors.ENDC}")
            req = self.rep_socket.recv_json()
            command = req.get("command")
            
            if command == "reset":
                print(f"{Colors.BLUE}SERVER: Received 'reset' command.{Colors.ENDC}")
                # 1. Get the very first observation from a new Rust sim
                print("SERVER: Waiting for initial observation from a Rust simulation...")
                sim_id, payload = self.router.recv_multipart()
                req_obs = json.loads(payload.decode("utf-8"))
                initial_obs = _obs_from_payload_dict(req_obs)
                
                self.active_sim_id = sim_id
                # 2. Send a dummy action to unblock the Rust sim
                self.router.send_multipart([self.active_sim_id, json.dumps({"action_idx": 0}).encode("utf-8")])
                
                # 3. Reply to the trainer with the initial observation
                self.rep_socket.send_json({"obs": initial_obs})
                print(f"{Colors.GREEN}SERVER: Reset complete for sim {sim_id.decode()}. Sent initial obs to trainer.{Colors.ENDC}")

            elif command == "step":
                action = req.get("action")
                # print(f"SERVER: Received 'step' command with action {action}.")
                
                # 1. Wait for the transition data from Rust (PULL socket)
                transition = self.pull.recv_json()
                # Ensure we have the right simulation's data if multiple sims are running
                while self.active_sim_id is not None and transition.get("sim_id") != self.active_sim_id.decode():
                    print(f"SERVER: Skipping transition from {transition.get('sim_id')}, waiting for {self.active_sim_id.decode()}")
                    transition = self.pull.recv_json()

                reward = float(transition["reward"])
                done = bool(transition["done"])
                # next_obs = self._obs_from_json(transition["next_obs"])
                # next_obs = _obs_from_payload_dict(transition)
                next_obs_field = transition.get("next_obs")
                if next_obs_field is None:
                    raise KeyError(f"Transition missing 'next_obs'; got keys: {list(transition.keys())}")

                # If Rust sends a plain list -> use it directly.
                # If Rust sends a dict like {"obs_flat": [...], "seq_len": ..., ...} -> normalize it.
                if isinstance(next_obs_field, dict):
                    next_obs = _obs_from_payload_dict(next_obs_field)
                else:
                    next_obs = next_obs_field
                
                # 2. If not done, sync with Rust and send the new action
                if not done:
                    # This recv is just to sync with the Rust sim's next action request
                    sim_id, _ = self.router.recv_multipart() 
                    # Send the real action from the agent
                    self.router.send_multipart([sim_id, json.dumps({"action_idx": int(action)}).encode("utf-8")])

                # 3. Reply to the trainer with the step result
                self.rep_socket.send_json({
                    "next_obs": next_obs,
                    "reward": reward,
                    "done": done
                })
                # print(f"SERVER: Step complete. Sent transition data to trainer.")
                if done:
                    print(f"{Colors.YELLOW}SERVER: Episode finished for sim {self.active_sim_id.decode()}.{Colors.ENDC}")
                    self.active_sim_id = None # Ready for a new episode/sim
            
    def close(self):
        """Cleanly close all sockets and terminate the context."""
        self.router.close()
        self.pull.close()
        self.rep_socket.close()
        self.ctx.term()

if __name__ == "__main__":
    server = ZmqServer()
    try:
        server.run_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
    finally:
        server.close()