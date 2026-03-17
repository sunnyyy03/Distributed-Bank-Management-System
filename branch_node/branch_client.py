"""
Branch Client — autonomous node that simulates local cash transactions.

Periodically generates a random cash-reserve balance and publishes it to a
RabbitMQ queue for the HQ Central Server to consume.  Implements a Lamport
Logical Clock to maintain distributed event ordering.

Also acts as a Two-Phase Commit (2PC) Participant, listening on a
dedicated coordinator queue for PREPARE / COMMIT / ABORT messages.
"""

import json
import os
import random
import threading
import time

import pika

# Read configuration from Docker environment variables
BRANCH_ID = os.getenv("BRANCH_ID", "101")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "branch_updates"

# 2PC-related queues
COORDINATOR_QUEUE = f"branch_{BRANCH_ID}_coordinator"
TRANSFER_QUEUE = "transfer_requests"

# Shared mutable state guarded by a lock
_state_lock = threading.Lock()
current_cash = 0.0  # latest known cash balance (updated by the publish loop)


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


# =================================================================
# 2PC Participant — Coordinator Queue Consumer
# =================================================================

def _on_coordinator_message(ch, method, properties, body):
    """Handle PREPARE, COMMIT, and ABORT messages from the HQ Coordinator."""
    global current_cash

    data = json.loads(body)
    msg_type = data.get("type")
    tx_id = data.get("tx_id", "unknown")
    sender_id = str(data.get("sender_id"))
    receiver_id = str(data.get("receiver_id"))
    amount = data.get("amount", 0)

    short_tx = tx_id[:8]

    # ----------------------------------------------------------
    # PREPARE — evaluate and vote
    # ----------------------------------------------------------
    if msg_type == "PREPARE":
        with _state_lock:
            local_cash = current_cash

        # Determine vote based on role in the transfer
        if str(BRANCH_ID) == sender_id:
            # Sender: check if we have sufficient funds
            if local_cash >= amount:
                vote = "YES"
                print(
                    f"Branch {BRANCH_ID} 2PC: Vote YES "
                    f"(balance ${local_cash} >= ${amount})  "
                    f"(tx={short_tx}...)"
                )
            else:
                vote = "NO"
                print(
                    f"Branch {BRANCH_ID} 2PC: Vote NO "
                    f"(balance ${local_cash} < ${amount})  "
                    f"(tx={short_tx}...)"
                )
        else:
            # Receiver: always vote YES
            vote = "YES"
            print(
                f"Branch {BRANCH_ID} 2PC: Vote YES (receiver)  "
                f"(tx={short_tx}...)"
            )

        # Publish vote back to HQ via the reply_to queue
        vote_payload = json.dumps({
            "branch_id": BRANCH_ID,
            "vote": vote,
            "tx_id": tx_id,
        })

        reply_to = properties.reply_to
        corr_id = properties.correlation_id

        if reply_to:
            ch.basic_publish(
                exchange="",
                routing_key=reply_to,
                body=vote_payload,
                properties=pika.BasicProperties(correlation_id=corr_id),
            )

    # ----------------------------------------------------------
    # COMMIT — permanently apply the transfer to local state
    # ----------------------------------------------------------
    elif msg_type == "COMMIT":
        with _state_lock:
            if str(BRANCH_ID) == sender_id:
                current_cash -= amount
                print(
                    f"Branch {BRANCH_ID} 2PC: COMMIT received — "
                    f"deducted ${amount}, new balance ${current_cash}  "
                    f"(tx={short_tx}...)"
                )
            else:
                current_cash += amount
                print(
                    f"Branch {BRANCH_ID} 2PC: COMMIT received — "
                    f"credited ${amount}, new balance ${current_cash}  "
                    f"(tx={short_tx}...)"
                )

    # ----------------------------------------------------------
    # ABORT — discard, no state change
    # ----------------------------------------------------------
    elif msg_type == "ABORT":
        print(
            f"Branch {BRANCH_ID} 2PC: ABORT received — "
            f"transfer discarded  (tx={short_tx}...)"
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def _coordinator_listener():
    """Background thread: consume PREPARE/COMMIT/ABORT from the coordinator queue."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=COORDINATOR_QUEUE)

    channel.basic_consume(
        queue=COORDINATOR_QUEUE, on_message_callback=_on_coordinator_message
    )

    print(f"Branch {BRANCH_ID}: Listening on {COORDINATOR_QUEUE} for 2PC messages...")
    channel.start_consuming()


# =================================================================
# Main Publishing Loop
# =================================================================

def run_branch():
    """Run the branch publish loop indefinitely.

    On each iteration the branch:
    1. Increments its local Lamport clock (a local event occurred).
    2. Generates a random cash balance to simulate transactions.
    3. Publishes the update to the ``branch_updates`` RabbitMQ queue.

    Every ~15 seconds (every 3rd iteration), Branch 101 also publishes
    a TransferRequest to simulate a live inter-branch transfer.
    """
    global current_cash

    print(f"Starting Branch {BRANCH_ID} Node...")

    # Start the 2PC coordinator listener in a background thread
    threading.Thread(target=_coordinator_listener, daemon=True).start()

    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_declare(queue=TRANSFER_QUEUE)

    # Initialize Branch Lamport Clock
    lamport_clock = 0
    iteration = 0

    while True:
        # A local event occurred (cash changed) — increment clock
        lamport_clock += 1
        iteration += 1

        with _state_lock:
            current_cash = round(random.uniform(5000, 50000), 2)
            local_cash = current_cash

        payload = {
            "branch_id": BRANCH_ID,
            "cash_amount": local_cash,
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
                f"${local_cash}"
            )
        except pika.exceptions.AMQPError as e:
            print(f"Branch {BRANCH_ID} publish error: {e}. Reconnecting...")
            connection = connect_to_rabbitmq()
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME)
            channel.queue_declare(queue=TRANSFER_QUEUE)

        # ---------------------------------------------------------
        # Transfer trigger: Branch 101 requests a transfer every
        # ~15 seconds (every 3rd loop iteration at 5s intervals).
        # ---------------------------------------------------------
        if str(BRANCH_ID) == "101" and iteration % 3 == 0:
            transfer_amount = 5000.0
            transfer_payload = {
                "sender_id": "101",
                "receiver_id": "102",
                "amount": transfer_amount,
            }
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key=TRANSFER_QUEUE,
                    body=json.dumps(transfer_payload),
                )
                print(
                    f"Branch {BRANCH_ID}: Requesting transfer: "
                    f"${transfer_amount} → Branch 102"
                )
            except pika.exceptions.AMQPError as e:
                print(f"Branch {BRANCH_ID} transfer publish error: {e}")

        # Wait 5 seconds before publishing again
        time.sleep(5)


if __name__ == "__main__":
    run_branch()