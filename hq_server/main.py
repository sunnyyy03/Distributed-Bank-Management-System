from fastapi import FastAPI
from pydantic import BaseModel
import database

app = FastAPI(title="Bank HQ Server")

# Initialize the SQLite database as soon as the API server starts
database.init_db()

class CashUpdate(BaseModel):
    branch_id: str
    cash_amount: float

@app.post("/update_cash")
def update_cash(data: CashUpdate):
    """Endpoint for branches to report their current cash levels."""
    # Save the data persistently to SQLite instead of a dictionary
    database.update_branch_cash(data.branch_id, data.cash_amount)
    
    print(f"HQ Log: Branch {data.branch_id} updated cash to ${data.cash_amount} in DB")
    return {"status": "success", "recorded_amount": data.cash_amount}

@app.get("/status")
def get_status():
    """Endpoint to view the aggregated status of all branches."""
    # Query the SQLite database for the live status
    status = database.get_all_cash_status()
    return {"network_status": status}