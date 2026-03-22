"""
HQ Central Server — FastAPI application for the Bank HQ node.

Consumes cash-level updates from branch nodes via a RabbitMQ queue,
persists them in SQLite, and exposes a network-wide status dashboard
via REST API.

Acts as the Transaction Coordinator for the Two-Phase Commit (2PC)
protocol used for inter-branch cash transfers.

Implements a Lamport Logical Clock for distributed event ordering.
"""

import json
import threading
import time
import uuid

import pika
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import database
import scheduler

app = FastAPI(title="Bank HQ Server")

# Allow the frontend to fetch data without browser security blocks
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"], 
)

# Initialize the SQLite database as soon as the API server starts
database.init_db()

# Initialize HQ Lamport Clock
hq_lamport_clock = 0
branch_clocks = {"101": 0, "102": 0}

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "branch_updates"

# -----------------------------------------------------------------
# 2PC Configuration
# -----------------------------------------------------------------
TRANSFER_QUEUE = "transfer_requests"     # Branch → HQ transfer requests
HQ_TX_REPLIES = "hq_tx_replies"          # Branch → HQ vote replies
VOTE_TIMEOUT = 3                         # Max seconds to wait for votes

# Thread-safe storage for incoming votes keyed by correlation_id
_vote_lock = threading.Lock()
_votes: dict[str, list[dict]] = {}


# -----------------------------------------------------------------
# Pydantic Models
# -----------------------------------------------------------------

class EmployeeCreate(BaseModel):
    branch_id: str
    name: str
    role: str

class TransferRequest(BaseModel):
    source_branch: str
    dest_branch: str
    amount: float

class LocalTransactionRequest(BaseModel):
    branch_id: str
    type: str
    amount: float


# =================================================================
# RabbitMQ Connection Helper
# =================================================================

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


# =================================================================
# Branch-Updates Consumer (existing AMQP pipeline)
# =================================================================

def _on_branch_update(ch, method, properties, body):
    """Callback for each message consumed from the branch_updates queue.

    Applies Lamport clock synchronization and writes to the database.
    """
    global hq_lamport_clock

    data = json.loads(body)
    branch_id = data["branch_id"]
    cash_amount = data["cash_amount"]
    received_clock = data["lamport_clock"]
    
    branch_clocks[branch_id] = received_clock

    # Lamport Logical Clock Algorithm
    hq_lamport_clock = max(hq_lamport_clock, received_clock) + 1

    # Save the data persistently to SQLite
    database.update_branch_cash(branch_id, cash_amount)

    print(
        f"HQ Log: Branch {branch_id} synced. "
        f"HQ Clock updated to {hq_lamport_clock} | "
        f"Recorded: ${cash_amount}"
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def _branch_updates_loop():
    """Background thread: consume from branch_updates forever."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_on_branch_update)

    print("HQ: Waiting for branch updates on RabbitMQ...")
    channel.start_consuming()


# =================================================================
# 2PC — Vote Reply Consumer
# =================================================================

def _on_vote_reply(ch, method, properties, body):
    """Callback for vote replies arriving on hq_tx_replies.

    Stores each vote in the _votes dict keyed by correlation_id so
    the coordinator thread can collect them.
    """
    global hq_lamport_clock
    
    vote = json.loads(body)
    corr_id = properties.correlation_id
    
    # Process clock sync from branch
    incoming_clock = vote.get("clock", 0)
    branch_clocks[vote.get("branch_id")] = incoming_clock
    hq_lamport_clock = max(hq_lamport_clock, incoming_clock) + 1

    with _vote_lock:
        if corr_id not in _votes:
            _votes[corr_id] = []
        _votes[corr_id].append(vote)

    print(
        f"HQ 2PC: Received vote from Branch {vote.get('branch_id')}: "
        f"{vote.get('vote')} (tx={corr_id[:8]}...)"
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def _vote_reply_loop():
    """Background thread: consume vote replies from hq_tx_replies."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=HQ_TX_REPLIES)

    channel.basic_consume(queue=HQ_TX_REPLIES, on_message_callback=_on_vote_reply)

    print("HQ 2PC: Listening for vote replies on hq_tx_replies...")
    channel.start_consuming()


# =================================================================
# 2PC — Transfer Request Consumer  (Transaction Coordinator)
# =================================================================

def _on_transfer_request(ch, method, properties, body):
    """Callback for each transfer request.

    Orchestrates the full Two-Phase Commit protocol:
      Phase 1 — PREPARE: send PREPARE to both branches, collect votes.
      Phase 2 — COMMIT or ABORT based on votes.
    """
    global hq_lamport_clock

    data = json.loads(body)
    sender_id = str(data["sender_id"])
    receiver_id = str(data["receiver_id"])
    amount = data["amount"]
    tx_id = str(uuid.uuid4())  # unique transaction identifier

    print(
        f"\nHQ 2PC: Transfer request received — "
        f"Branch {sender_id} → Branch {receiver_id}: ${amount}  "
        f"(tx={tx_id[:8]}...)"
    )

    # Initialize vote collection for this transaction
    with _vote_lock:
        _votes[tx_id] = []

    # ----------------------------------------------------------
    # Phase 1 — PREPARE
    # ----------------------------------------------------------
    # We need a separate connection for publishing because pika
    # BlockingConnection is not thread-safe; this callback runs on
    # the transfer-request consumer thread.
    pub_conn = connect_to_rabbitmq()
    pub_channel = pub_conn.channel()

    # Phase 1: Internal PREPARE action
    hq_lamport_clock += 1
    
    prepare_payload = json.dumps({
        "type": "PREPARE",
        "tx_id": tx_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "amount": amount,
        "clock": hq_lamport_clock,
    })

    sender_queue = f"branch_{sender_id}_coordinator"
    receiver_queue = f"branch_{receiver_id}_coordinator"

    # Declare the participant queues (idempotent)
    pub_channel.queue_declare(queue=sender_queue)
    pub_channel.queue_declare(queue=receiver_queue)

    # Send PREPARE to both branches with reply_to and correlation_id
    for queue in [sender_queue, receiver_queue]:
        pub_channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=prepare_payload,
            properties=pika.BasicProperties(
                reply_to=HQ_TX_REPLIES,
                correlation_id=tx_id,
            ),
        )

    print(
        f"HQ 2PC: PREPARE sent to {sender_queue} and {receiver_queue}  "
        f"(tx={tx_id[:8]}...)"
    )

    # ----------------------------------------------------------
    # Collect votes (wait up to VOTE_TIMEOUT seconds)
    # ----------------------------------------------------------
    deadline = time.time() + VOTE_TIMEOUT
    while time.time() < deadline:
        with _vote_lock:
            if len(_votes.get(tx_id, [])) >= 2:
                break
        time.sleep(0.2)  # poll interval

    # Gather collected votes
    with _vote_lock:
        collected = _votes.pop(tx_id, [])

    # ----------------------------------------------------------
    # Phase 2 — Decision
    # ----------------------------------------------------------
    all_yes = (
        len(collected) == 2
        and all(v.get("vote") == "YES" for v in collected)
    )

    if all_yes:
        # Execute the atomic transfer in SQLite
        success = database.atomic_transfer(sender_id, receiver_id, amount)

        if success:
            decision = "COMMIT"
            print(
                f"HQ 2PC: All votes YES — COMMIT. Atomic transfer executed.  "
                f"(tx={tx_id[:8]}...)"
            )
        else:
            decision = "ABORT"
            print(
                f"HQ 2PC: DB transfer failed — ABORT.  "
                f"(tx={tx_id[:8]}...)"
            )
    else:
        decision = "ABORT"
        vote_summary = [
            f"Branch {v.get('branch_id')}={v.get('vote')}" for v in collected
        ]
        print(
            f"HQ 2PC: ABORT — votes received: {vote_summary} "
            f"(needed 2× YES)  (tx={tx_id[:8]}...)"
        )

    # Broadcast the decision to both participant branches
    hq_lamport_clock += 1
    
    decision_payload = json.dumps({
        "type": decision,
        "tx_id": tx_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "amount": amount,
        "clock": hq_lamport_clock,
    })

    for queue in [sender_queue, receiver_queue]:
        pub_channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=decision_payload,
        )

    print(
        f"HQ 2PC: {decision} broadcast to {sender_queue} and {receiver_queue}  "
        f"(tx={tx_id[:8]}...)"
    )

    pub_conn.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def _transfer_request_loop():
    """Background thread: consume from transfer_requests queue."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=TRANSFER_QUEUE)

    channel.basic_consume(
        queue=TRANSFER_QUEUE, on_message_callback=_on_transfer_request
    )

    print("HQ 2PC: Listening for transfer requests...")
    channel.start_consuming()


# =================================================================
# FastAPI Startup & Endpoints
# =================================================================

@app.on_event("startup")
def start_background_consumers():
    """Launch all RabbitMQ consumer threads as daemons at startup."""
    # 1. Branch-updates consumer (existing pipeline)
    threading.Thread(target=_branch_updates_loop, daemon=True).start()
    # 2. Vote-reply consumer for 2PC
    threading.Thread(target=_vote_reply_loop, daemon=True).start()
    # 3. Transfer-request consumer (2PC coordinator)
    threading.Thread(target=_transfer_request_loop, daemon=True).start()


@app.get("/status")
def get_status():
    """Return the aggregated cash status of all branches."""
    status = database.get_all_cash_status()
    return {
        "network_status": status, 
        "hq_clock": hq_lamport_clock,
        "branch_101_clock": branch_clocks.get("101", 0),
        "branch_102_clock": branch_clocks.get("102", 0)
    }


@app.get("/schedule")
def get_schedule():
    """Return the dynamically generated weekly shift schedule."""
    return scheduler.generate_weekly_schedule()


# =================================================================
# Chaos Controller Endpoints
# =================================================================

@app.post("/transfer")
def trigger_transfer(payload: TransferRequest):
    """Trigger a 2PC transfer by pushing to the transfer_requests queue."""
    if payload.source_branch == payload.dest_branch:
        raise HTTPException(status_code=400, detail="Source and destination branches cannot be the same.")

    try:
        payload_dict = payload.dict()
        amount = float(payload_dict["amount"])
        amount = round(amount, 2)
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Ensure all required fields are present and amount is a valid number.")

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Transaction amount must be greater than $0.")

    status = database.get_all_cash_status()
    source_branch_info = next((b for b in status if b["branch_id"] == payload.source_branch), None)
    if source_branch_info and source_branch_info["current_balance"] - amount < 0:
        raise HTTPException(status_code=400, detail="Source branch has insufficient funds.")

    dest_branch_info = next((b for b in status if b["branch_id"] == payload.dest_branch), None)
    if dest_branch_info and dest_branch_info["current_balance"] + amount > 50000:
        raise HTTPException(status_code=400, detail="Branch maximum capacity ($50,000) exceeded.")

    global hq_lamport_clock
    hq_lamport_clock += 1
    
    pub_conn = connect_to_rabbitmq()
    pub_channel = pub_conn.channel()
    
    msg_payload = json.dumps({
        "sender_id": payload.source_branch,
        "receiver_id": payload.dest_branch,
        "amount": amount
    })
    
    pub_channel.queue_declare(queue=TRANSFER_QUEUE)
    pub_channel.basic_publish(
        exchange="",
        routing_key=TRANSFER_QUEUE,
        body=msg_payload,
    )
    pub_conn.close()
    return {"message": "Transfer initiated via 2PC"}


@app.post("/local-transaction")
def trigger_local_tx(payload: LocalTransactionRequest):
    """Execute a local deposit or withdrawal."""
    try:
        payload_dict = payload.dict()
        amount = float(payload_dict["amount"])
        amount = round(amount, 2)
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payload. Ensure all required fields are present and amount is a valid number.")

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Transaction amount must be greater than $0.")
        
    status = database.get_all_cash_status()
    branch_info = next((b for b in status if b["branch_id"] == payload.branch_id), None)
    if not branch_info:
        raise HTTPException(status_code=404, detail="Branch not found")
        
    current_cash = branch_info["current_balance"]
    if payload.type == "DEPOSIT":
        if current_cash + amount > 50000:
            raise HTTPException(status_code=400, detail="Branch maximum capacity ($50,000) exceeded.")
        new_cash = current_cash + amount
    elif payload.type == "WITHDRAWAL":
        if current_cash - amount < 0:
            raise HTTPException(status_code=400, detail="Insufficient funds. Balance cannot drop below $0.")
        new_cash = current_cash - amount
    else:
        raise HTTPException(status_code=400, detail="Invalid transaction type")

    global hq_lamport_clock
    hq_lamport_clock += 1
        
    database.update_branch_cash(payload.branch_id, new_cash)
    
    pub_conn = connect_to_rabbitmq()
    pub_channel = pub_conn.channel()
    msg_payload = json.dumps({
        "type": "LOCAL_TX",
        "tx_type": payload.type,
        "amount": amount,
        "clock": hq_lamport_clock
    })
    queue_name = f"branch_{payload.branch_id}_coordinator"
    pub_channel.queue_declare(queue=queue_name)
    pub_channel.basic_publish(exchange="", routing_key=queue_name, body=msg_payload)
    pub_conn.close()
    
    return {"message": f"Local transaction {payload.type} successful"}


@app.post("/reset")
def trigger_system_reset():
    """Reset the whole system back to $10,000 for each branch."""
    global hq_lamport_clock, branch_clocks
    hq_lamport_clock = 0
    branch_clocks = {"101": 0, "102": 0}
    
    database.update_branch_cash("101", 10000.0)
    database.update_branch_cash("102", 10000.0)
    
    pub_conn = connect_to_rabbitmq()
    pub_channel = pub_conn.channel()
    msg_payload = json.dumps({
        "type": "HARD_RESET",
        "amount": 10000.0,
        "clock": hq_lamport_clock
    })
    
    for queue_name in ["branch_101_coordinator", "branch_102_coordinator"]:
        pub_channel.queue_declare(queue=queue_name)
        pub_channel.basic_publish(exchange="", routing_key=queue_name, body=msg_payload)
        
    pub_conn.close()
    
    return {"message": "System reset to $10000.0 per branch"}


# =================================================================
# Employee CRUD Endpoints
# =================================================================

@app.get("/employees")
def list_employees():
    """Return the list of all current employees."""
    return database.get_employees()


@app.post("/employee")
def hire_employee(payload: EmployeeCreate):
    """Add a new employee record and return the generated ID."""
    try:
        new_id = database.add_employee(
            branch_id=payload.branch_id,
            name=payload.name,
            role=payload.role,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"message": f"Employee {new_id} added successfully.", "employee_id": new_id}


@app.delete("/employee/{employee_id}")
def fire_employee(employee_id: str):
    """Remove an employee by ID. Returns 404 if the ID does not exist or is a Manager."""
    deleted = database.remove_employee(employee_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Employee {employee_id} not found or is a protected Manager.",
        )
    return {"message": f"Employee {employee_id} removed successfully."}