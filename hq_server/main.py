"""
HQ Central Server — FastAPI application for the Bank HQ node.

Consumes cash-level updates from branch nodes via a RabbitMQ queue,
persists them in SQLite, and exposes a network-wide status dashboard
via REST API.

Implements a Lamport Logical Clock for distributed event ordering.
"""

import json
import threading
import time

import pika
from fastapi import FastAPI

import database

app = FastAPI(title="Bank HQ Server")

# Initialize the SQLite database as soon as the API server starts
database.init_db()

# Initialize HQ Lamport Clock
hq_lamport_clock = 0

RABBITMQ_HOST = "rabbitmq"
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
            print(f"HQ: Connected to RabbitMQ on attempt {attempt}.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(
                f"HQ: RabbitMQ not ready "
                f"(attempt {attempt}/{max_retries}). Retrying in {delay}s..."
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)  # cap at 30 seconds

    raise RuntimeError(
        f"HQ: Could not connect to RabbitMQ after {max_retries} attempts."
    )


def _on_message(ch, method, properties, body):
    """Callback invoked for each message consumed from the queue.

    Applies the same Lamport clock synchronization and database write
    logic that was previously handled by the /update_cash POST endpoint.
    """
    global hq_lamport_clock

    data = json.loads(body)
    branch_id = data["branch_id"]
    cash_amount = data["cash_amount"]
    received_clock = data["lamport_clock"]

    # Lamport Logical Clock Algorithm: take the max of local and
    # received clocks, then increment by one.
    hq_lamport_clock = max(hq_lamport_clock, received_clock) + 1

    # Save the data persistently to SQLite
    database.update_branch_cash(branch_id, cash_amount)

    print(
        f"HQ Log: Branch {branch_id} synced. "
        f"HQ Clock updated to {hq_lamport_clock} | "
        f"Recorded: ${cash_amount}"
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def _consume_loop():
    """Background thread: connect to RabbitMQ and consume messages forever."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_on_message)

    print("HQ: Waiting for branch updates on RabbitMQ...")
    channel.start_consuming()


@app.on_event("startup")
def start_rabbitmq_consumer():
    """Launch the RabbitMQ consumer in a daemon background thread."""
    consumer_thread = threading.Thread(target=_consume_loop, daemon=True)
    consumer_thread.start()


@app.get("/status")
def get_status():
    """Return the aggregated cash status of all branches."""
    status = database.get_all_cash_status()
    return {"network_status": status, "hq_clock": hq_lamport_clock}