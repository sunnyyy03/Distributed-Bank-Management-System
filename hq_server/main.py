"""
HQ Central Server — FastAPI application for the Bank HQ node.

Receives cash-level updates from branch nodes, persists them in SQLite,
and exposes a network-wide status dashboard via REST API.

Implements a Lamport Logical Clock for distributed event ordering.
"""

from fastapi import FastAPI
from pydantic import BaseModel

import database

app = FastAPI(title="Bank HQ Server")

# Initialize the SQLite database as soon as the API server starts
database.init_db()

# Initialize HQ Lamport Clock
hq_lamport_clock = 0


class CashUpdate(BaseModel):
    """Schema for incoming cash-level reports from branch nodes."""

    branch_id: str
    cash_amount: float
    lamport_clock: int


@app.post("/update_cash")
def update_cash(data: CashUpdate):
    """Receive and persist a cash-level update from a branch node."""
    global hq_lamport_clock

    # Lamport Logical Clock Algorithm: take the max of local and
    # received clocks, then increment by one.
    hq_lamport_clock = max(hq_lamport_clock, data.lamport_clock) + 1

    # Save the data persistently to SQLite
    database.update_branch_cash(data.branch_id, data.cash_amount)

    print(
        f"HQ Log: Branch {data.branch_id} synced. "
        f"HQ Clock updated to {hq_lamport_clock} | "
        f"Recorded: ${data.cash_amount}"
    )
    return {
        "status": "success",
        "recorded_amount": data.cash_amount,
        "hq_clock": hq_lamport_clock,
    }


@app.get("/status")
def get_status():
    """Return the aggregated cash status of all branches."""
    status = database.get_all_cash_status()
    return {"network_status": status, "hq_clock": hq_lamport_clock}