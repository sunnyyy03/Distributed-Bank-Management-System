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
current_cash = 10000.0  # latest known cash balance
local_clock = 0         # branch Lamport clock


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

def _send_branch_update():
    """Publish current state to branch_updates queue so HQ status captures clock."""
    update_payload = json.dumps({
        "branch_id": BRANCH_ID,
        "cash_amount": current_cash,
        "lamport_clock": local_clock
    })
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=update_payload)
        connection.close()
    except Exception as e:
        print(f"Failed to send branch update: {e}")


def _on_coordinator_message(ch, method, properties, body):
    """Handle PREPARE, COMMIT, and ABORT messages from the HQ Coordinator."""
    global current_cash, local_clock

    data = json.loads(body)
    msg_type = data.get("type")
    tx_id = data.get("tx_id", "unknown")
    sender_id = str(data.get("sender_id"))
    receiver_id = str(data.get("receiver_id"))
    amount = data.get("amount", 0)
    incoming_clock = data.get("clock", 0)

    # Distributed Lamport logical clock synchronization
    if msg_type == "HARD_RESET":
        local_clock = 0
    else:
        local_clock = max(local_clock, incoming_clock) + 1

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
            "clock": local_clock,
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

    # ----------------------------------------------------------
    # SYNC or HARD_RESET — hard reset of local state
    # ----------------------------------------------------------
    elif msg_type in ["SYNC", "HARD_RESET"]:
        with _state_lock:
            current_cash = amount
            print(
                f"Branch {BRANCH_ID} Chaos: {msg_type} received — "
                f"forced balance reset to ${current_cash}"
            )

    # ----------------------------------------------------------
    # LOCAL_TX — apply a local deposit or withdrawal
    # ----------------------------------------------------------
    elif msg_type == "LOCAL_TX":
        tx_type = data.get("tx_type")
        with _state_lock:
            if tx_type == "DEPOSIT":
                current_cash += amount
            elif tx_type == "WITHDRAWAL":
                current_cash -= amount
            print(
                f"Branch {BRANCH_ID} Chaos: LOCAL_TX ({tx_type}) received — "
                f"applied ${amount}, new balance ${current_cash}"
            )

    if msg_type in ["COMMIT", "ABORT", "LOCAL_TX", "SYNC", "HARD_RESET"]:
        _send_branch_update()

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
    """Run the branch purely as a listener.

    The branch spins up a background thread to listen on its coordinator
    queue for instructions (PREPARE, COMMIT, ABORT, SYNC, LOCAL_TX), and
    blocks the main thread indefinitely.
    """
    global current_cash

    print(f"Starting Branch {BRANCH_ID} Node (Chaos Listener Mode)...")

    # Start the 2PC coordinator listener in a background thread
    threading.Thread(target=_coordinator_listener, daemon=True).start()

    # Emit initial startup state back to HQ database immediately
    _send_branch_update()

    # Block main thread indefinitely
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print(f"Branch {BRANCH_ID} shutting down.")


if __name__ == "__main__":
    run_branch()