"""
Reconciliation logic for extracted bank statement data.

Runs a series of checks against the extracted data structure and returns
a ReconciliationResult that summarises all findings.  The caller is
responsible for persisting exceptions and updating job/document status.

Expected shape of extracted_data (all keys optional at the top level;
reconciliation degrades gracefully when fields are absent):

    {
        "opening_balance": 1000.00,        # Decimal-coercible number or None
        "closing_balance": 1500.00,        # Decimal-coercible number or None
        "transactions": [
            {
                "transaction_id": "txn_001",  # optional unique identifier
                "date": "2024-01-15",          # ISO date string
                "description": "Direct Debit",
                "debit":  null,                # Decimal-coercible or None
                "credit": 500.00,             # Decimal-coercible or None
                "balance": 1500.00            # running balance after this txn
            },
            ...
        ]
    }
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ReconciliationFinding:
    """A single issue detected during reconciliation."""
    check: str                           # machine-readable check name
    severity: str                        # "Critical" | "High" | "Medium" | "Low"
    title: str
    description: str
    transaction_id: Optional[str] = None  # set when the finding relates to a specific transaction


@dataclass
class ReconciliationResult:
    """Aggregated result of all reconciliation checks."""
    passed: bool
    findings: list = field(default_factory=list)

    @property
    def has_critical(self) -> bool:
        return any(f.severity == "Critical" for f in self.findings)

    @property
    def outcome(self) -> str:
        """
        Returns one of:
          "passed"  – no findings at all
          "warning" – findings present but none are Critical
          "failed"  – at least one Critical finding
        """
        if not self.findings:
            return "passed"
        if self.has_critical:
            return "failed"
        return "warning"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_decimal(value: Any) -> Optional[Decimal]:
    """Safely coerce *value* to a Decimal. Returns None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _txn_id(txn: dict, index: int) -> str:
    """Return the transaction identifier, falling back to a positional label."""
    return txn.get("transaction_id") or f"index_{index}"


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_opening_balance(extracted_data: dict) -> Optional[ReconciliationFinding]:
    """Verify that an opening balance is present in the extracted data."""
    if extracted_data.get("opening_balance") is None:
        return ReconciliationFinding(
            check="opening_balance_present",
            severity="High",
            title="Opening balance missing",
            description=(
                "The extracted data does not contain an opening balance. "
                "Running-balance consistency cannot be verified."
            ),
        )
    return None


def check_closing_balance(extracted_data: dict) -> Optional[ReconciliationFinding]:
    """Verify that a closing balance is present in the extracted data."""
    if extracted_data.get("closing_balance") is None:
        return ReconciliationFinding(
            check="closing_balance_present",
            severity="High",
            title="Closing balance missing",
            description=(
                "The extracted data does not contain a closing balance. "
                "Final balance verification cannot be performed."
            ),
        )
    return None


def check_amount_parse_consistency(extracted_data: dict) -> list:
    """
    Verify that all monetary values in the extraction are numeric.

    Top-level opening/closing balances that cannot be parsed are Critical
    (the whole extraction is unreliable).  Per-transaction field failures
    are High (the affected transaction cannot be trusted).
    """
    findings = []

    for field_name in ("opening_balance", "closing_balance"):
        raw = extracted_data.get(field_name)
        if raw is not None and _to_decimal(raw) is None:
            findings.append(ReconciliationFinding(
                check="amount_parse_consistency",
                severity="Critical",
                title=f"Unparseable value for '{field_name}'",
                description=(
                    f"The value '{raw}' for '{field_name}' could not be parsed "
                    "as a numeric amount."
                ),
            ))

    for i, txn in enumerate(extracted_data.get("transactions", [])):
        txn_id = _txn_id(txn, i)
        for field_name in ("debit", "credit", "balance"):
            raw = txn.get(field_name)
            if raw is not None and _to_decimal(raw) is None:
                findings.append(ReconciliationFinding(
                    check="amount_parse_consistency",
                    severity="High",
                    title=f"Unparseable amount in field '{field_name}' for transaction {txn_id}",
                    description=(
                        f"The value '{raw}' in field '{field_name}' could not be "
                        "parsed as a numeric amount."
                    ),
                    transaction_id=txn_id,
                ))

    return findings


def check_running_balance_consistency(extracted_data: dict) -> list:
    """
    Walk through transactions and verify that each stated running balance
    matches the calculated running balance (opening + credits − debits).

    A per-transaction mismatch is High; a mismatch against the stated
    closing balance is Critical (the statement totals do not reconcile).
    """
    findings = []
    transactions = extracted_data.get("transactions", [])

    opening = _to_decimal(extracted_data.get("opening_balance"))
    if opening is None:
        # Cannot check without a parseable opening balance
        return findings

    closing = _to_decimal(extracted_data.get("closing_balance"))
    tolerance = Decimal("0.01")
    running = opening

    for i, txn in enumerate(transactions):
        txn_id = _txn_id(txn, i)
        debit = _to_decimal(txn.get("debit"))
        credit = _to_decimal(txn.get("credit"))
        stated_balance = _to_decimal(txn.get("balance"))

        if debit is not None:
            running -= debit
        if credit is not None:
            running += credit

        if stated_balance is not None and abs(running - stated_balance) > tolerance:
            findings.append(ReconciliationFinding(
                check="running_balance_consistency",
                severity="High",
                title=f"Running balance mismatch at transaction {txn_id}",
                description=(
                    f"Calculated running balance is {running:.2f} but the statement "
                    f"shows {stated_balance:.2f} after transaction {txn_id}."
                ),
                transaction_id=txn_id,
            ))
            # Re-anchor to the stated balance so subsequent errors are independent
            running = stated_balance

    if closing is not None and abs(running - closing) > tolerance:
        findings.append(ReconciliationFinding(
            check="running_balance_consistency",
            severity="Critical",
            title="Final running balance does not match closing balance",
            description=(
                f"Running total after all transactions ({running:.2f}) does not "
                f"match the stated closing balance ({closing:.2f})."
            ),
        ))

    return findings


def check_missing_lines(extracted_data: dict) -> list:
    """
    Detect suspicious balance jumps between consecutive transactions.

    If the difference between two consecutive stated balances cannot be
    explained by the debit/credit on the later transaction, one or more
    lines may have been missed during extraction.
    """
    findings = []
    transactions = extracted_data.get("transactions", [])

    if len(transactions) < 2:
        return findings

    tolerance = Decimal("0.01")

    for i in range(1, len(transactions)):
        prev = transactions[i - 1]
        curr = transactions[i]
        txn_id = curr.get("transaction_id") or f"index_{i}"

        prev_balance = _to_decimal(prev.get("balance"))
        curr_balance = _to_decimal(curr.get("balance"))
        debit = _to_decimal(curr.get("debit"))
        credit = _to_decimal(curr.get("credit"))

        if prev_balance is None or curr_balance is None:
            continue

        balance_change = curr_balance - prev_balance
        stated_change = Decimal("0")
        if credit is not None:
            stated_change += credit
        if debit is not None:
            stated_change -= debit

        if abs(balance_change - stated_change) > tolerance:
            findings.append(ReconciliationFinding(
                check="missing_line_suspicion",
                severity="High",
                title=f"Unexplained balance change before transaction {txn_id}",
                description=(
                    f"Balance changed by {balance_change:.2f} but the transaction "
                    f"amount accounts for only {stated_change:.2f}. One or more "
                    "lines may be missing from the extraction."
                ),
                transaction_id=txn_id,
            ))

    return findings


def check_duplicate_transactions(extracted_data: dict) -> list:
    """
    Detect transactions that share the same date, debit/credit amounts,
    and description — a strong signal of accidental duplication.
    """
    findings = []
    transactions = extracted_data.get("transactions", [])
    seen: dict = {}

    for i, txn in enumerate(transactions):
        txn_id = _txn_id(txn, i)
        key = (
            txn.get("date"),
            str(txn.get("debit")),
            str(txn.get("credit")),
            (txn.get("description") or "").strip().lower(),
        )
        if key in seen:
            findings.append(ReconciliationFinding(
                check="duplicate_transaction_suspicion",
                severity="Medium",
                title=f"Possible duplicate transaction: {txn_id}",
                description=(
                    f"Transaction {txn_id} has the same date, amount, and "
                    f"description as transaction {seen[key]}."
                ),
                transaction_id=txn_id,
            ))
        else:
            seen[key] = txn_id

    return findings


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_reconciliation(extracted_data: dict) -> ReconciliationResult:
    """
    Run all reconciliation checks against *extracted_data* and return a
    consolidated ReconciliationResult.

    Checks are executed in a deliberate order so that upstream failures
    (e.g. unparseable amounts) are captured before downstream checks that
    depend on those values.
    """
    findings: list = []

    # 1. Presence checks
    finding = check_opening_balance(extracted_data)
    if finding:
        findings.append(finding)

    finding = check_closing_balance(extracted_data)
    if finding:
        findings.append(finding)

    # 2. Parse consistency (must run before numeric checks)
    findings.extend(check_amount_parse_consistency(extracted_data))

    # 3. Numeric integrity checks
    findings.extend(check_running_balance_consistency(extracted_data))
    findings.extend(check_missing_lines(extracted_data))

    # 4. Duplication checks
    findings.extend(check_duplicate_transactions(extracted_data))

    return ReconciliationResult(passed=len(findings) == 0, findings=findings)
