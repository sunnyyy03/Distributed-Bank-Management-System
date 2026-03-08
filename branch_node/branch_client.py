import os
import time
import requests
import random

# Read configuration from the Docker environment variables
HQ_URL = os.getenv("HQ_URL", "http://hq-server:8000")
BRANCH_ID = os.getenv("BRANCH_ID", "101")

def run_branch():
    print(f"Starting Branch {BRANCH_ID} Node...")
    
    # Initialize Branch Lamport Clock
    lamport_clock = 0
    
    while True:
        # An event occurred (cash changed)
        lamport_clock += 1
        current_cash = round(random.uniform(5000, 50000), 2)
        
        # Add the clock to the payload
        payload = {
            "branch_id": BRANCH_ID,
            "cash_amount": current_cash,
            "lamport_clock": lamport_clock
        }
        
        try:
            # Send the data to the HQ server
            response = requests.post(f"{HQ_URL}/update_cash", json=payload)
            print(f"Branch {BRANCH_ID} (Clock: {lamport_clock}) sent update: ${current_cash} | HQ Response: {response.status_code}")
            
            # Keep branch clock synchronized with HQ clock if it is further ahead
            if response.status_code == 200:
                hq_clock = response.json().get("hq_clock", 0)
                if hq_clock > 0:
                    lamport_clock = max(lamport_clock, hq_clock)
                    
        except requests.exceptions.RequestException as e:
            print(f"Branch {BRANCH_ID} failed to connect to HQ: {e}")
        
        # Wait 5 seconds before syncing again
        time.sleep(5)

if __name__ == "__main__":
    run_branch()