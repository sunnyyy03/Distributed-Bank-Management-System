from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Bank HQ Server")

# Very basic in-memory storage for our MVP.
global_cash_reserves = {}

# Define the data structure the HQ expects to receive
class CashUpdate(BaseModel):
    branch_id: str
    cash_amount: float

@app.post("/update_cash")
def update_cash(data: CashUpdate):
    """Endpoint for branches to report their current cash levels."""
    global_cash_reserves[data.branch_id] = data.cash_amount
    print(f"HQ Log: Branch {data.branch_id} updated cash to ${data.cash_amount}")
    return {"status": "success", "recorded_amount": data.cash_amount}

@app.get("/status")
def get_status():
    """Endpoint to view the aggregated status of all branches."""
    return {"network_status": global_cash_reserves}