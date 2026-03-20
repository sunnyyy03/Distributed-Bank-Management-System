"""
Scheduler module — Dynamic Workforce Allocation Algorithm.

Generates optimised weekly shift schedules for every branch based on
branch size (base staffing) and live cash volume (dynamic allocation).

Uses a **Two-Pass Minimum Viable Coverage** strategy:
  Pass 1 — Guarantee base Manager + Teller seats for ALL branches.
  Pass 2 — Distribute remaining Backup staff to high-volume branches.

Branches are sorted by live cash balance (descending) each day so the
busiest branch gets first pick during each pass (Priority by Volume).
"""

import database

# Days for which shifts are generated (Mon–Fri)
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# Default shift window
SHIFT_START = "8:00 AM"
SHIFT_END = "4:00 PM"

# Base Teller staffing requirements per branch size (Managers are hardcoded)
BASE_TELLER_COUNT = {
    "Large": 2,
    "Medium": 1,
    "Small": 1,
}

# If a branch's current_balance exceeds this multiplier × minimum_threshold,
# an extra "Backup" shift is injected in Pass 2.
HIGH_VOLUME_MULTIPLIER = 2.5


def _get_all_backups(employees: list[dict]) -> list[dict]:
    """Return a flat list of every employee whose role is 'Backup'.

    Backups are not restricted by branch_id; they can be assigned
    to any branch that needs extra coverage.
    """
    return [e for e in employees if e["role"] == "Backup"]


def _assign_base_shift(
    employees: list[dict],
    role: str,
    label: str,
    branch_id: str,
    working_today: set,
    backup_pool: list[dict],
) -> dict:
    """Assign an employee for a **base** Teller shift.

    Fallback hierarchy:
      1. Local match — same branch_id AND same role.
      2. Backup fallback — if the role is 'Teller' and no local match
         was found, search the global backup_pool (ignoring branch_id).
      3. UNASSIGNED — no staff available.
    """
    # ── 1. Primary: branch-local employee ─────────────────────────
    for emp in employees:
        if (
            emp["role"] == role
            and emp["branch_id"] == branch_id
            and emp["employee_id"] not in working_today
        ):
            working_today.add(emp["employee_id"])
            return {
                "shift": label,
                "time": f"{SHIFT_START} - {SHIFT_END}",
                "role": role,
                "assigned_to": emp["name"],
                "employee_id": emp["employee_id"],
            }

    # ── 2. Fallback: Teller seat → Backup staff ──────────────────
    if role == "Teller":
        for emp in backup_pool:
            if emp["employee_id"] not in working_today:
                working_today.add(emp["employee_id"])
                return {
                    "shift": label,
                    "time": f"{SHIFT_START} - {SHIFT_END}",
                    "role": role,
                    "assigned_to": emp["name"],
                    "employee_id": emp["employee_id"],
                }

    # ── 3. Nobody available ───────────────────────────────────────
    return {
        "shift": label,
        "time": f"{SHIFT_START} - {SHIFT_END}",
        "role": role,
        "assigned_to": "UNASSIGNED – NEED BACKUP",
        "employee_id": None,
    }


def _assign_backup_shift(
    label: str,
    working_today: set,
    backup_pool: list[dict],
) -> dict:
    """Assign an employee for a **high-volume Backup** shift.

    Searches the global backup pool (branch-agnostic).
    Falls back to UNASSIGNED if every Backup is already working today.
    """
    for emp in backup_pool:
        if emp["employee_id"] not in working_today:
            working_today.add(emp["employee_id"])
            return {
                "shift": label,
                "time": f"{SHIFT_START} - {SHIFT_END}",
                "role": "Backup",
                "assigned_to": emp["name"],
                "employee_id": emp["employee_id"],
            }

    return {
        "shift": label,
        "time": f"{SHIFT_START} - {SHIFT_END}",
        "role": "Backup",
        "assigned_to": "UNASSIGNED – NEED BACKUP",
        "employee_id": None,
    }


def generate_weekly_schedule() -> dict:
    """Generate and return an optimised weekly shift schedule.

    Two-Pass Minimum Viable Coverage Algorithm
    -------------------------------------------
    Managers are hardcoded (1 per branch, guaranteed by the DB).
    For each day (Mon–Fri):

    1. Initialise a day-level ``working_today`` set shared across
       ALL branches to prevent cross-branch double-booking.
    2. Sort branches by ``current_balance`` descending (Priority by
       Volume) so busier branches pick first in each pass.
    3. Hardcode the branch Manager into Shift 1 (always guaranteed).
    4. **Pass 1 — Teller Coverage**: assign Teller seats for every
       branch.  Uses local staff first, then Backup fallback.
    5. **Pass 2 — Bonus Coverage**: assign high-volume Backup seats
       (``balance > 2.5× threshold``) from whoever is still free.

    Returns a structured dictionary ready for JSON serialisation.
    """
    branches = database.get_branches_with_volume()
    employees = database.get_employees()

    # Global pool of Backups (branch-agnostic)
    backup_pool = _get_all_backups(employees)

    # Build a lookup: branch_id → Manager employee dict
    branch_managers: dict = {}
    for emp in employees:
        if emp["role"] == "Manager":
            branch_managers[emp["branch_id"]] = emp

    # Pre-build branch metadata keyed by branch_id
    branch_meta: dict = {}
    for branch in branches:
        bid = branch["branch_id"]
        branch_meta[bid] = {
            "branch_id": bid,
            "branch_size": branch["branch_size"],
            "current_balance": branch["current_balance"],
            "minimum_threshold": branch["minimum_threshold"],
            "high_volume_flag": False,
            "days": {},
        }

    # ── Day-first loop ────────────────────────────────────────────
    for day in WEEKDAYS:

        # STATE: one set per day, shared across ALL branches
        working_today: set[str] = set()

        # SMART SORT: branches ordered by current_balance descending
        sorted_branches = sorted(
            branches,
            key=lambda b: b["current_balance"],
            reverse=True,
        )

        # Track shifts per branch to merge results
        day_shifts_map: dict[str, list[dict]] = {
            b["branch_id"]: [] for b in branches
        }
        shift_counters: dict[str, int] = {
            b["branch_id"]: 1 for b in branches
        }

        # ── Hardcoded Manager (Shift 1, always guaranteed) ────────
        for branch in sorted_branches:
            bid = branch["branch_id"]
            mgr = branch_managers.get(bid)
            if mgr:
                working_today.add(mgr["employee_id"])
                day_shifts_map[bid].append({
                    "shift": f"Shift {shift_counters[bid]}",
                    "time": f"{SHIFT_START} - {SHIFT_END}",
                    "role": "Manager",
                    "assigned_to": mgr["name"],
                    "employee_id": mgr["employee_id"],
                })
                shift_counters[bid] += 1

        # ── Pass 1: Base Teller Coverage (Card Dealer Round Robin) ─
        # Build a remaining-seats tracker: { branch_id: tellers_still_needed }
        remaining: dict[str, int] = {}
        for branch in sorted_branches:
            bid = branch["branch_id"]
            size = branch["branch_size"]
            remaining[bid] = BASE_TELLER_COUNT.get(size, 1)

        # Deal one seat per branch per round until all are filled
        while any(r > 0 for r in remaining.values()):
            for branch in sorted_branches:
                bid = branch["branch_id"]
                if remaining[bid] <= 0:
                    continue

                label = f"Shift {shift_counters[bid]}"
                day_shifts_map[bid].append(
                    _assign_base_shift(
                        employees, "Teller", label,
                        bid, working_today, backup_pool,
                    )
                )
                shift_counters[bid] += 1
                remaining[bid] -= 1

        # ── Pass 2: Bonus Coverage (High-Volume Backup) ───────────
        for branch in sorted_branches:
            bid = branch["branch_id"]
            balance = branch["current_balance"]
            threshold = branch["minimum_threshold"]

            high_volume = balance > HIGH_VOLUME_MULTIPLIER * threshold
            if high_volume:
                branch_meta[bid]["high_volume_flag"] = True

                label = f"Shift {shift_counters[bid]}"
                day_shifts_map[bid].append(
                    _assign_backup_shift(
                        label, working_today, backup_pool,
                    )
                )
                shift_counters[bid] += 1

        # Write day results into branch metadata
        for bid, shifts in day_shifts_map.items():
            branch_meta[bid]["days"][day] = shifts

    # ── Assemble final output ─────────────────────────────────────
    schedule: dict = {}
    for bid, meta in branch_meta.items():
        schedule[f"Branch {bid}"] = meta

    return {"weekly_schedule": schedule}
