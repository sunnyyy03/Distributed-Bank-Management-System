"""
Scheduler module — Dynamic Workforce Allocation Algorithm.

Generates optimised weekly shift schedules for every branch based on
branch size (base staffing) and live cash volume (dynamic allocation).
"""

import database

# Days for which shifts are generated (Mon–Fri)
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# Default shift window
SHIFT_START = "8:00 AM"
SHIFT_END = "4:00 PM"

# Base staffing requirements per branch size
#   { role: count_needed }
BASE_STAFFING = {
    "Large": {"Manager": 1, "Teller": 2, "Security": 1},
    "Medium": {"Manager": 1, "Teller": 1, "Security": 0},
    "Small": {"Manager": 1, "Teller": 1, "Security": 0},
}

# If a branch's current_balance exceeds this multiplier × minimum_threshold,
# an extra "Floating Teller" shift is injected.
HIGH_VOLUME_MULTIPLIER = 2.5


def _build_employee_pool(employees: list[dict]) -> dict[str, dict[str, list[dict]]]:
    """Organise employees into a nested dict: branch_id → role → [employee].

    Returns a mutable structure so the scheduler can pop employees as they
    are assigned to avoid double-booking within the same day.
    """
    pool: dict[str, dict[str, list[dict]]] = {}
    for emp in employees:
        bid = emp["branch_id"]
        role = emp["role"]
        pool.setdefault(bid, {}).setdefault(role, []).append(emp)
    return pool


def _assign_shift(
    pool: dict[str, list[dict]],
    role: str,
    label: str,
) -> dict:
    """Try to pop an available employee for *role* from the branch pool.

    Returns a shift dict.  If no employee is available the shift is
    marked UNASSIGNED so the edge case is visible in the output.
    """
    available = pool.get(role, [])
    if available:
        emp = available[0]  # pick the first available employee
        return {
            "shift": label,
            "time": f"{SHIFT_START} - {SHIFT_END}",
            "role": role,
            "assigned_to": emp["name"],
            "employee_id": emp["employee_id"],
        }
    # Edge case: not enough employees for this role
    return {
        "shift": label,
        "time": f"{SHIFT_START} - {SHIFT_END}",
        "role": role,
        "assigned_to": "UNASSIGNED – insufficient staff",
        "employee_id": None,
    }


def generate_weekly_schedule() -> dict:
    """Generate and return an optimised weekly shift schedule.

    Algorithm
    ---------
    1. Fetch branches (with live cash volume) and all employees.
    2. For each branch determine **base staffing** from branch_size.
    3. Apply **dynamic allocation**: if current_balance > 2.5× the
       minimum_threshold, inject an extra "Floating Teller" shift.
    4. Match available employees in that branch to the required shifts.
    5. Repeat for every weekday (Mon–Fri).

    Returns a structured dictionary ready for JSON serialisation.
    """
    branches = database.get_branches_with_volume()
    employees = database.get_employees()

    # Build the per-branch employee pool
    full_pool = _build_employee_pool(employees)

    schedule: dict = {}

    for branch in branches:
        bid = branch["branch_id"]
        size = branch["branch_size"]
        balance = branch["current_balance"]
        threshold = branch["minimum_threshold"]

        # Determine base staffing needs
        staffing = dict(BASE_STAFFING.get(size, BASE_STAFFING["Small"]))

        # Dynamic allocation — high cash volume ⇒ extra Floating Teller
        high_volume = balance > HIGH_VOLUME_MULTIPLIER * threshold
        if high_volume:
            staffing["Floating Teller"] = 1

        branch_schedule: dict = {
            "branch_id": bid,
            "branch_size": size,
            "current_balance": balance,
            "minimum_threshold": threshold,
            "high_volume_flag": high_volume,
            "days": {},
        }

        for day in WEEKDAYS:
            day_shifts: list[dict] = []
            shift_counter = 1

            # The same employees are available each day (weekly roster)
            branch_pool = {}
            for role, emps in full_pool.get(bid, {}).items():
                branch_pool[role] = list(emps)  # shallow copy per day

            for role, count in staffing.items():
                for _ in range(count):
                    label = f"Shift {shift_counter}"
                    day_shifts.append(
                        _assign_shift(branch_pool, role, label)
                    )
                    shift_counter += 1

            branch_schedule["days"][day] = day_shifts

        schedule[f"Branch {bid}"] = branch_schedule

    return {"weekly_schedule": schedule}
