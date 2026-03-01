import os
import time
import requests
import random

# Read configuration from the Docker environment variables
HQ_URL = os.getenv("HQ_URL", "http://hq-server:8000")
BRANCH_ID = os.getenv("BRANCH_ID", "101")

def run_branch():
    print(f"Starting Branch {BRANCH_ID} Node...")
    
    while True:
        # Simulate local transactions changing the cash reserve
        current_cash = round(random.uniform(5000, 50000), 2)
        payload = {
            "branch_id": BRANCH_ID,
            "cash_amount": current_cash
        }
        
        try:
            # Send the data to the HQ server
            response = requests.post(f"{HQ_URL}/update_cash", json=payload)
            print(f"Branch {BRANCH_ID} sent update: ${current_cash} | HQ Response: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Branch {BRANCH_ID} failed to connect to HQ: {e}")
        
        # Wait 5 seconds before syncing again
        time.sleep(5)

if __name__ == "__main__":
    run_branch()