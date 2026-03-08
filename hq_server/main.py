from fastapi import FastAPI
from pydantic import BaseModel
import database

app = FastAPI(title="Bank HQ Server")

# Initialize the SQLite database as soon as the API server starts
database.init_db()

# Initialize HQ Lamport Clock
hq_lamport_clock = 0

# Define the data structure the HQ expects to receive
class CashUpdate(BaseModel):
    branch_id: str
    cash_amount: float
    lamport_clock: int

@app.post("/update_cash")
def update_cash(data: CashUpdate):
    """Endpoint for branches to report their current cash levels."""
    global hq_lamport_clock
    
    # Lamport Logical Clock Algorithm
    hq_lamport_clock = max(hq_lamport_clock, data.lamport_clock) + 1
    
    # Save the data persistently to SQLite 
    database.update_branch_cash(data.branch_id, data.cash_amount)
    
    print(f"HQ Log: Branch {data.branch_id} synced. HQ Clock updated to {hq_lamport_clock} | Recorded: ${data.cash_amount}")
    return {"status": "success", "recorded_amount": data.cash_amount, "hq_clock": hq_lamport_clock}

@app.get("/status")
def get_status():
    """Endpoint to view the aggregated status of all branches."""
    # Query the SQLite database for the live status
    status = database.get_all_cash_status()
    return {"network_status": status, "hq_clock": hq_lamport_clock}