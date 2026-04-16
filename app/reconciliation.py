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
from datetime import date
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

# Divisor used by check_transaction_count_plausibility to compute a minimum
# expected transaction count from the statement period length in days.
# A value of 10 means "at least 1 transaction per 10 days" — deliberately
# conservative so that very quiet personal accounts are not falsely flagged.
# Only applied to statements of 14+ days (shorter periods have too little
# signal to be useful).
_MIN_TXNS_PERIOD_DIVISOR = 10

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
                severity="Medium",
                title=f"Unexplained balance change before transaction {txn_id}",
                description=(
                    f"Balance changed by {balance_change:.2f} but the transaction "
                    f"amount accounts for only {stated_change:.2f}. One or more "
                    "lines may be missing from the extraction."
                ),
                transaction_id=txn_id,
            ))

    return findings


def check_transaction_count_plausibility(extracted_data: dict) -> list:
    """
    Heuristic check: flag if the extracted transaction count is suspiciously low
    relative to the PDF page count or the statement period length.

    This detects silent under-extraction — for example, when Document AI groups
    multiple same-date transactions into a single entity — that would otherwise
    pass all balance-chain checks because the subset of extracted running balances
    can still be self-consistent.

    Two independent sub-checks are run:

    1. **Page count**: real bank statements contain at least one transaction per
       page, so ``count < page_count`` is a strong signal of under-extraction.
    2. **Period length**: for statements of 14+ days, we expect at least
       ``max(2, period_days // 10)`` transactions.  This is deliberately
       conservative (a very quiet account may have few transactions).

    Both sub-checks produce Medium severity findings so they are surfaced as
    warnings rather than blocking failures.
    """
    findings = []
    transactions = extracted_data.get("transactions", [])
    count = len(transactions)

    # Sub-check 1: fewer transactions than pages is suspicious for any real statement.
    page_count = extracted_data.get("page_count")
    if isinstance(page_count, int) and page_count > 0 and count < page_count:
        findings.append(ReconciliationFinding(
            check="transaction_count_plausibility",
            severity="Medium",
            title="Fewer transactions than PDF pages",
            description=(
                f"Only {count} transaction(s) were extracted from a {page_count}-page "
                "PDF. Real bank statements typically contain at least one transaction "
                "per page. This may indicate incomplete extraction. Manual review "
                "recommended."
            ),
        ))

    # Sub-check 2: count vs statement period length.
    start_raw = extracted_data.get("statement_start_date")
    end_raw = extracted_data.get("statement_end_date")
    if start_raw and end_raw:
        try:
            start = (
                date.fromisoformat(str(start_raw))
                if not isinstance(start_raw, date)
                else start_raw
            )
            end = (
                date.fromisoformat(str(end_raw))
                if not isinstance(end_raw, date)
                else end_raw
            )
            period_days = (end - start).days
            if period_days >= 14:
                min_expected = max(2, period_days // _MIN_TXNS_PERIOD_DIVISOR)
                if count < min_expected:
                    findings.append(ReconciliationFinding(
                        check="transaction_count_plausibility",
                        severity="Medium",
                        title="Suspiciously low transaction count for statement period",
                        description=(
                            f"Only {count} transaction(s) were extracted for a "
                            f"{period_days}-day statement period (expected at least "
                            f"{min_expected}). This may indicate incomplete extraction. "
                            "Manual review recommended."
                        ),
                    ))
        except (ValueError, TypeError, AttributeError):
            pass

    return findings


def check_duplicate_transactions(extracted_data: dict) -> list:
    """
    Detect transactions that share the same date, amounts, balance, and
    description — a strong signal of accidental duplication.

    The deduplication key intentionally includes the running balance and page
    number (when available) so that legitimate repeated transactions (e.g. five
    TUI refunds of the same amount on the same date, or repeated same-amount
    transfers to savings) are NOT collapsed into a single record.  Using only
    date + description + amount would incorrectly suppress these valid rows.
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
            str(txn.get("balance")),
            (txn.get("description") or "").strip().lower(),
            txn.get("source_page_number"),
        )
        if key in seen:
            findings.append(ReconciliationFinding(
                check="duplicate_transaction_suspicion",
                severity="Medium",
                title=f"Possible duplicate transaction: {txn_id}",
                description=(
                    f"Transaction {txn_id} has the same date, amount, balance, and "
                    f"description as transaction {seen[key]}."
                ),
                transaction_id=txn_id,
            ))
        else:
            seen[key] = txn_id

    return findings


def check_money_totals(extracted_data: dict) -> list:
    """
    Compare the sum of extracted debits/credits against the stated money_in
    and money_out totals from the statement header.

    This check catches the case where a subset of transactions was extracted
    (e.g. only 74 of 195 rows) but the running-balance chain is accidentally
    self-consistent within that subset — the money totals will still diverge
    from the header figures even if no individual balance-continuity error fires.

    A total mismatch exceeding 1p is reported as Critical because it proves
    the extracted set is incomplete or incorrect.
    """
    findings = []
    stated_money_in = _to_decimal(extracted_data.get("money_in"))
    stated_money_out = _to_decimal(extracted_data.get("money_out"))

    if stated_money_in is None and stated_money_out is None:
        return findings

    transactions = extracted_data.get("transactions", [])
    tolerance = Decimal("0.01")

    extracted_in = sum(
        (c for c in (_to_decimal(t.get("credit")) for t in transactions) if c is not None),
        Decimal("0"),
    )
    extracted_out = sum(
        (d for d in (_to_decimal(t.get("debit")) for t in transactions) if d is not None),
        Decimal("0"),
    )

    if stated_money_in is not None and abs(extracted_in - stated_money_in) > tolerance:
        findings.append(ReconciliationFinding(
            check="money_totals_mismatch",
            severity="Critical",
            title="Extracted money-in total does not match statement header",
            description=(
                f"Statement header shows money in = {stated_money_in:.2f} but the sum "
                f"of extracted credit amounts is {extracted_in:.2f} "
                f"(difference: {abs(extracted_in - stated_money_in):.2f}). "
                "This strongly indicates incomplete extraction."
            ),
        ))

    if stated_money_out is not None and abs(extracted_out - stated_money_out) > tolerance:
        findings.append(ReconciliationFinding(
            check="money_totals_mismatch",
            severity="Critical",
            title="Extracted money-out total does not match statement header",
            description=(
                f"Statement header shows money out = {stated_money_out:.2f} but the sum "
                f"of extracted debit amounts is {extracted_out:.2f} "
                f"(difference: {abs(extracted_out - stated_money_out):.2f}). "
                "This strongly indicates incomplete extraction."
            ),
        ))

    return findings


def check_incomplete_extraction(extracted_data: dict) -> list:
    """
    Detect severely under-extracted statements by comparing the extracted
    transaction count against:

      1. The expected count supplied by the caller (``expected_transaction_count``
         key in *extracted_data*), when present.
      2. An estimate derived from money_in + money_out divided by a typical
         average transaction value (£50), when statement totals are available.

    A count below 50 % of the expected figure is reported as High — the
    extraction must be considered incomplete until proven otherwise.  A count
    below 25 % is Critical.
    """
    findings = []
    count = len(extracted_data.get("transactions", []))

    # Sub-check 1: explicit expected count supplied by caller.
    expected = extracted_data.get("expected_transaction_count")
    if isinstance(expected, int) and expected > 0:
        ratio = count / expected
        if ratio < 0.25:
            findings.append(ReconciliationFinding(
                check="incomplete_extraction",
                severity="Critical",
                title="Critically incomplete extraction — fewer than 25 % of expected transactions",
                description=(
                    f"Only {count} transaction(s) were extracted but {expected} are expected "
                    f"({ratio * 100:.0f} %). The extraction must be considered incomplete. "
                    "Manual review and re-extraction are required."
                ),
            ))
        elif ratio < 0.50:
            findings.append(ReconciliationFinding(
                check="incomplete_extraction",
                severity="High",
                title="Incomplete extraction suspected — fewer than 50 % of expected transactions",
                description=(
                    f"Only {count} transaction(s) were extracted but {expected} are expected "
                    f"({ratio * 100:.0f} %). This may indicate parser truncation, "
                    "continuation-page loss, or silent deduplication. "
                    "Manual review recommended."
                ),
            ))
        return findings

    # Sub-check 2: estimate from money totals when no explicit count is given.
    stated_in = _to_decimal(extracted_data.get("money_in"))
    stated_out = _to_decimal(extracted_data.get("money_out"))
    if stated_in is not None or stated_out is not None:
        total_flow = (stated_in or Decimal("0")) + (stated_out or Decimal("0"))
        # Estimate: assume an average transaction value of £50 (conservative).
        _AVG_TXN_VALUE = Decimal("50")
        if total_flow > 0:
            estimated = int(total_flow / _AVG_TXN_VALUE)
            if estimated > 0 and count < estimated // 2:  # fewer than ~50% of estimate
                findings.append(ReconciliationFinding(
                    check="incomplete_extraction",
                    severity="High",
                    title="Incomplete extraction suspected — extracted count far below estimate",
                    description=(
                        f"Only {count} transaction(s) were extracted. Based on stated "
                        f"money flow of {total_flow:.2f} an estimated {estimated} transactions "
                        "would be expected. This may indicate incomplete extraction."
                    ),
                ))

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

    # 5. Plausibility checks (sparse extraction detection)
    findings.extend(check_transaction_count_plausibility(extracted_data))

    # 6. Money-totals cross-check (catches incomplete extraction even when
    #    the running-balance chain is accidentally self-consistent).
    findings.extend(check_money_totals(extracted_data))

    # 7. Explicit incomplete-extraction check.
    findings.extend(check_incomplete_extraction(extracted_data))

    return ReconciliationResult(passed=len(findings) == 0, findings=findings)
