import sqlite3

DB_FILE = "bank_data.db"

def init_db():
    """Initializes the database and creates the required tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create Branches table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Branches (
            branch_id TEXT PRIMARY KEY,
            location_name TEXT,
            branch_size TEXT
        )
    ''')
    
    # Create Cash Reserves table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Cash_Reserves (
            reserve_id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id TEXT,
            current_balance REAL,
            minimum_threshold REAL,
            FOREIGN KEY(branch_id) REFERENCES Branches(branch_id)
        )
    ''')
    
    # Create Employees table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Employees (
            employee_id TEXT PRIMARY KEY,
            branch_id TEXT,
            name TEXT,
            role TEXT,
            FOREIGN KEY(branch_id) REFERENCES Branches(branch_id)
        )
    ''')
    
    # Insert a dummy branch so our Branch Node '101' has a valid foreign key
    cursor.execute('''
        INSERT OR IGNORE INTO Branches (branch_id, location_name, branch_size)
        VALUES ('101', 'HQ Local Branch', 'Large')
    ''')
    
    # Insert an initial cash record for branch 101 with a $10,000 minimum threshold
    cursor.execute('''
        INSERT OR IGNORE INTO Cash_Reserves (branch_id, current_balance, minimum_threshold)
        VALUES ('101', 0.0, 10000.0)
    ''')

    conn.commit()
    conn.close()

def update_branch_cash(branch_id: str, amount: float):
    """Updates the cash balance for a specific branch."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE Cash_Reserves 
        SET current_balance = ? 
        WHERE branch_id = ?
    ''', (amount, branch_id))
    conn.commit()
    conn.close()

def get_all_cash_status():
    """Retrieves the current cash status for the dashboard."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT branch_id, current_balance, minimum_threshold FROM Cash_Reserves')
    rows = cursor.fetchall()
    conn.close()
    
    # Format the SQL rows into a list of dictionaries for our JSON API
    return [{"branch_id": r[0], "current_balance": r[1], "minimum_threshold": r[2]} for r in rows]