"""
Branch Client — autonomous node that simulates local cash transactions.

Periodically generates a random cash-reserve balance and publishes it to a
RabbitMQ queue for the HQ Central Server to consume.  Implements a Lamport
Logical Clock to maintain distributed event ordering.
"""

import json
import os
import random
import time

import pika

# Read configuration from Docker environment variables
BRANCH_ID = os.getenv("BRANCH_ID", "101")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "branch_updates"


def connect_to_rabbitmq():
    """Establish a connection to RabbitMQ with retry logic.

    RabbitMQ may take a few extra seconds to boot inside Docker, so
    this function retries with exponential backoff up to ~60 seconds.
    """
    max_retries = 10
    delay = 2  # initial delay in seconds

    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            print(f"Branch {BRANCH_ID}: Connected to RabbitMQ on attempt {attempt}.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(
                f"Branch {BRANCH_ID}: RabbitMQ not ready "
                f"(attempt {attempt}/{max_retries}). Retrying in {delay}s..."
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)  # cap at 30 seconds

    raise RuntimeError(
        f"Branch {BRANCH_ID}: Could not connect to RabbitMQ "
        f"after {max_retries} attempts."
    )


def run_branch():
    """Run the branch publish loop indefinitely.

    On each iteration the branch:
    1. Increments its local Lamport clock (a local event occurred).
    2. Generates a random cash balance to simulate transactions.
    3. Publishes the update to the ``branch_updates`` RabbitMQ queue.
    """
    print(f"Starting Branch {BRANCH_ID} Node...")

    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

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
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(payload),
            )
            print(
                f"Branch {BRANCH_ID} (Clock: {lamport_clock}) published update: "
                f"${current_cash}"
            )
        except pika.exceptions.AMQPError as e:
            print(f"Branch {BRANCH_ID} publish error: {e}. Reconnecting...")
            connection = connect_to_rabbitmq()
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME)

        # Wait 5 seconds before publishing again
        time.sleep(5)


if __name__ == "__main__":
    run_branch()