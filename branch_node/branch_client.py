"""
Branch Client — autonomous node that simulates local cash transactions.

Periodically generates a random cash-reserve balance and reports it to the
HQ Central Server over HTTP.  Implements a Lamport Logical Clock to maintain
distributed event ordering with the HQ node.
"""

import os
import random
import time

import requests

# Read configuration from Docker environment variables
HQ_URL = os.getenv("HQ_URL", "http://hq-server:8000")
BRANCH_ID = os.getenv("BRANCH_ID", "101")


def run_branch():
    """Run the branch sync loop indefinitely.

    On each iteration the branch:
    1. Increments its local Lamport clock (a local event occurred).
    2. Generates a random cash balance to simulate transactions.
    3. Sends the update to HQ and synchronizes its clock with the
       HQ clock returned in the response.
    """
    print(f"Starting Branch {BRANCH_ID} Node...")

    # Initialize Branch Lamport Clock
    lamport_clock = 0

    while True:
        # A local event occurred (cash changed) — increment clock
        lamport_clock += 1
        current_cash = round(random.uniform(5000, 50000), 2)

        payload = {
            "branch_id": BRANCH_ID,
            "cash_amount": current_cash,
            "lamport_clock": lamport_clock,
        }

        try:
            response = requests.post(f"{HQ_URL}/update_cash", json=payload)
            print(
                f"Branch {BRANCH_ID} (Clock: {lamport_clock}) sent update: "
                f"${current_cash} | HQ Response: {response.status_code}"
            )

            # Keep branch clock synchronized with HQ clock
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