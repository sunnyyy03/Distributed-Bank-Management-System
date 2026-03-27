"""
Database module — SQLite persistence layer for the Bank HQ Server.

Manages the Branches, Cash_Reserves, and Employees tables and provides
helper functions for reading and writing cash-reserve data, including
atomic inter-branch transfers for the Two-Phase Commit protocol.
"""

import sqlite3
import uuid

DB_FILE = "bank_data.db"

# Single source of truth for the baseline employee roster.
# Constraint: 1 Manager per branch, seeded on first boot and on HR reset.
BASELINE_EMPLOYEES = [
    ("E1001", "101", "Alice", "Manager"),
    ("E2001", "102", "Bob",   "Manager"),
]


def init_db():
    """Initialize the database and create the required tables.

    Seeds Branch 101 and Branch 102 with starting balances of $0.00 and
    a minimum cash-reserve threshold of $10,000.
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

    # Seed Branch 101
    cursor.execute("""
        INSERT OR IGNORE INTO Branches (branch_id, location_name, branch_size)
        VALUES ('101', 'HQ Local Branch', 'Large')
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO Cash_Reserves (branch_id, current_balance, minimum_threshold)
        VALUES ('101', 10000.0, 10000.0)
    """)

    # Seed Branch 102
    cursor.execute("""
        INSERT OR IGNORE INTO Branches (branch_id, location_name, branch_size)
        VALUES ('102', 'Downtown Branch', 'Medium')
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO Cash_Reserves (branch_id, current_balance, minimum_threshold)
        VALUES ('102', 10000.0, 10000.0)
    """)

    # ------------------------------------------------------------------
    # Seed Employees
    # ------------------------------------------------------------------
    for emp in BASELINE_EMPLOYEES:
        cursor.execute(
            "INSERT OR IGNORE INTO Employees (employee_id, branch_id, name, role) "
            "VALUES (?, ?, ?, ?)",
            emp,
        )

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


def reset_hr_rosters():
    """Clear all employees and reseed the baseline rosters explicitly."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Employees")
    
    for emp in BASELINE_EMPLOYEES:
        cursor.execute(
            "INSERT INTO Employees (employee_id, branch_id, name, role) "
            "VALUES (?, ?, ?, ?)",
            emp,
        )
        
    conn.commit()
    conn.close()


def atomic_transfer(sender_id: str, receiver_id: str, amount: float) -> bool:
    """Execute an atomic inter-branch cash transfer.

    Deducts ``amount`` from the sender's current_balance and adds it to
    the receiver's current_balance inside a single SQLite transaction.

    Returns True on success, False if the sender has insufficient funds
    or any database error occurs.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        # Begin an explicit transaction for atomicity
        cursor.execute("BEGIN")

        # Read the sender's current balance
        cursor.execute(
            "SELECT current_balance FROM Cash_Reserves WHERE branch_id = ?",
            (sender_id,),
        )
        sender_row = cursor.fetchone()
        if sender_row is None or sender_row[0] < amount:
            conn.rollback()
            return False

        # Deduct from the sender
        cursor.execute(
            "UPDATE Cash_Reserves SET current_balance = current_balance - ? WHERE branch_id = ?",
            (amount, sender_id),
        )

        # Credit the receiver
        cursor.execute(
            "UPDATE Cash_Reserves SET current_balance = current_balance + ? WHERE branch_id = ?",
            (amount, receiver_id),
        )

        conn.commit()
        return True

    except sqlite3.Error:
        conn.rollback()
        return False
    finally:
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


def get_employees():
    """Return every employee record as a list of dictionaries."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT employee_id, branch_id, name, role FROM Employees")
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "employee_id": r[0],
            "branch_id": r[1],
            "name": r[2],
            "role": r[3],
        }
        for r in rows
    ]


def add_employee(branch_id: str, name: str, role: str) -> str:
    """Insert a new employee into the Employees table.

    Generates a unique employee_id using uuid4 and returns it.
    Rejects the creation of Manager roles (read-only seed only).

    Raises ValueError if the role is Manager.
    """
    if role.upper() == "MANAGER":
        raise ValueError("Manager role is read-only and cannot be created.")

    employee_id = uuid.uuid4().hex[:6].upper()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO Employees (employee_id, branch_id, name, role) "
        "VALUES (?, ?, ?, ?)",
        (employee_id, branch_id, name, role),
    )
    conn.commit()
    conn.close()
    return employee_id


def remove_employee(employee_id: str) -> bool:
    """Delete an employee by their employee_id.

    Returns True if a row was deleted, False otherwise.
    Managers are protected and cannot be removed.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Check if the employee is a Manager — abort if so
    cursor.execute(
        "SELECT role FROM Employees WHERE employee_id = ?",
        (employee_id,),
    )
    row = cursor.fetchone()
    if row and row[0] == "Manager":
        conn.close()
        return False

    cursor.execute(
        "DELETE FROM Employees WHERE employee_id = ?",
        (employee_id,),
    )
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def get_branches_with_volume():
    """Join Branches and Cash_Reserves to return branch info with live cash data.

    Returns a list of dicts with keys:
        branch_id, branch_size, current_balance, minimum_threshold
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT b.branch_id,
               b.branch_size,
               cr.current_balance,
               cr.minimum_threshold
        FROM   Branches b
        JOIN   Cash_Reserves cr ON b.branch_id = cr.branch_id
    """)
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "branch_id": r[0],
            "branch_size": r[1],
            "current_balance": r[2],
            "minimum_threshold": r[3],
        }
        for r in rows
    ]