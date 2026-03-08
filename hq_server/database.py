"""
Database module — SQLite persistence layer for the Bank HQ Server.

Manages the Branches, Cash_Reserves, and Employees tables and provides
helper functions for reading and writing cash-reserve data.
"""

import sqlite3

DB_FILE = "bank_data.db"


def init_db():
    """Initialize the database and create the required tables.

    Seeds Branch 101 with a starting balance of $0.00 and a minimum
    cash-reserve threshold of $10,000.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create Branches table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Branches (
            branch_id TEXT PRIMARY KEY,
            location_name TEXT,
            branch_size TEXT
        )
    """)

    # Create Cash Reserves table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Cash_Reserves (
            reserve_id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id TEXT,
            current_balance REAL,
            minimum_threshold REAL,
            FOREIGN KEY(branch_id) REFERENCES Branches(branch_id)
        )
    """)

    # Create Employees table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Employees (
            employee_id TEXT PRIMARY KEY,
            branch_id TEXT,
            name TEXT,
            role TEXT,
            FOREIGN KEY(branch_id) REFERENCES Branches(branch_id)
        )
    """)

    # Seed Branch 101 so the branch node has a valid foreign key
    cursor.execute("""
        INSERT OR IGNORE INTO Branches (branch_id, location_name, branch_size)
        VALUES ('101', 'HQ Local Branch', 'Large')
    """)

    # Insert an initial cash record with a $10,000 minimum threshold
    cursor.execute("""
        INSERT OR IGNORE INTO Cash_Reserves (branch_id, current_balance, minimum_threshold)
        VALUES ('101', 0.0, 10000.0)
    """)

    conn.commit()
    conn.close()


def update_branch_cash(branch_id: str, amount: float):
    """Update the current cash balance for a specific branch."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE Cash_Reserves
        SET current_balance = ?
        WHERE branch_id = ?
        """,
        (amount, branch_id),
    )
    conn.commit()
    conn.close()


def get_all_cash_status():
    """Retrieve the current cash status for every branch.

    Returns a list of dictionaries suitable for JSON serialization.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT branch_id, current_balance, minimum_threshold FROM Cash_Reserves"
    )
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "branch_id": r[0],
            "current_balance": r[1],
            "minimum_threshold": r[2],
        }
        for r in rows
    ]