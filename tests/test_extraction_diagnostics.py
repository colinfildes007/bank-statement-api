"""Tests for extraction diagnostics, reconciliation hardening, and deduplication.

Covers the defect raised in BS-0394A9:
- Barclays statement with 195 transactions extracted as 74 (no exceptions raised).

Key scenarios tested:
1. Repeated descriptions on same date are NOT collapsed to one transaction.
2. Repeated transfer rows with same description but different amounts are kept.
3. Continuation-page parsing preserves all rows across page breaks.
4. money_in / money_out mismatch raises Critical reconciliation exceptions.
5. Incomplete extraction (count < 50% of expected) raises High exceptions.
6. Critically incomplete extraction (count < 25% of expected) raises Critical exceptions.
7. Returned direct debit / credit handling.
8. Multiple refunds from the same merchant on the same day.
9. Duplicate check uses balance so legitimate repeated same-amount rows are NOT flagged.
10. check_duplicate_transactions only fires when date + amounts + balance + description match.
"""

from datetime import date
from decimal import Decimal

import pytest

from app.reconciliation import (
    ReconciliationResult,
    check_duplicate_transactions,
    check_incomplete_extraction,
    check_money_totals,
    check_transaction_count_plausibility,
    run_reconciliation,
)
from app.pdf_fallback import _parse_barclays_metadata


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _txn(date_str, desc, credit=None, debit=None, balance=None, page=None):
    return {
        "transaction_id": f"t_{date_str}_{desc[:10]}",
        "date": date_str,
        "description": desc,
        "credit": credit,
        "debit": debit,
        "balance": balance,
        "source_page_number": page,
    }


# ---------------------------------------------------------------------------
# 1. Repeated descriptions on same date — must NOT be collapsed
# ---------------------------------------------------------------------------

class TestRepeatedDescriptionsSameDate:
    def test_five_tui_refunds_not_collapsed(self):
        """Five TUI refunds of the same amount on the same date must remain 5 rows."""
        txns = [
            _txn("2020-04-24", "TUI refund", credit=150.00, balance=200.00 + i * 150, page=5)
            for i in range(5)
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        # Each refund has a different balance → no duplicates should be flagged.
        assert findings == [], (
            f"Expected no duplicate findings for TUI refunds with different balances, got: {findings}"
        )

    def test_same_desc_same_balance_flagged_as_duplicate(self):
        """Two transactions with identical date/amount/balance/description are flagged."""
        txns = [
            _txn("2020-04-24", "DD COUNCIL TAX", debit=100.00, balance=500.00, page=5),
            _txn("2020-04-24", "DD COUNCIL TAX", debit=100.00, balance=500.00, page=5),
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        assert len(findings) == 1
        assert findings[0].check == "duplicate_transaction_suspicion"


# ---------------------------------------------------------------------------
# 2. Repeated transfer rows — different amounts, same description
# ---------------------------------------------------------------------------

class TestRepeatedTransfersDifferentAmounts:
    def test_transfers_same_desc_different_amounts_not_deduped(self):
        """Same-date transfers to savings with different amounts must all be kept."""
        txns = [
            _txn("2020-04-27", "TFR to savings 93180263", debit=amount, balance=1000 - sum_so_far, page=6)
            for amount, sum_so_far in [
                (200.00, 200), (150.00, 350), (100.00, 450), (75.00, 525),
            ]
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        assert findings == [], (
            f"Transfers with different amounts should NOT be flagged as duplicates, got: {findings}"
        )

    def test_transfers_same_amount_same_balance_are_duplicate(self):
        """Genuinely duplicated transfer rows (same everything) are flagged."""
        txns = [
            _txn("2020-04-27", "TFR to savings 93180263", debit=200.00, balance=800.00, page=6),
            _txn("2020-04-27", "TFR to savings 93180263", debit=200.00, balance=800.00, page=6),
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        assert len(findings) == 1


# ---------------------------------------------------------------------------
# 3. Continuation-page parsing
# ---------------------------------------------------------------------------

class TestContinuationPageParsing:
    def test_parse_page_text_with_continuation(self):
        """Rows at the top of a continuation page (no date prefix) are captured."""
        # Simulate two pages: page 1 ends mid-transaction, page 2 starts with balance only.
        page1 = (
            "Balance Brought Forward 200.00\n"
            "19 Feb DD GAS BILL 50.00 150.00\n"
            "20 Feb FPS SALARY 1000.00"  # amount only, balance overflows to page 2
        )
        page2 = (
            "1150.00\n"  # balance from previous page
            "21 Feb SO RENT 600.00 550.00\n"
        )
        from app.documentai import NormalisedAccount
        from app.pdf_fallback import _parse_transactions

        acct = NormalisedAccount(
            statement_start_date=date(2020, 2, 19),
            statement_end_date=date(2020, 5, 18),
            opening_balance=Decimal("200.00"),
        )
        txns = _parse_transactions([page1, page2], acct)
        # Expect: DD GAS BILL, FPS SALARY (cross-page), SO RENT = 3 transactions
        assert len(txns) >= 3, (
            f"Expected at least 3 transactions across continuation page, got {len(txns)}: "
            f"{[(t.description_raw, t.amount) for t in txns]}"
        )

    def test_dateless_lines_with_amounts_attributed_to_last_date(self):
        """Lines with amounts but no date are treated as continuation rows."""
        page1 = (
            "Balance Brought Forward 100.00\n"
            "15 Mar BP ELECTRICITY 30.00 70.00\n"
            "CARD PAYMENT TESCO 20.00 50.00\n"  # no date — same date as previous
        )
        from app.documentai import NormalisedAccount
        from app.pdf_fallback import _parse_transactions

        acct = NormalisedAccount(
            statement_start_date=date(2020, 3, 15),
            statement_end_date=date(2020, 3, 31),
            opening_balance=Decimal("100.00"),
        )
        txns = _parse_transactions([page1], acct)
        assert len(txns) >= 2, (
            f"Dateless continuation line should yield a transaction, got {len(txns)}: "
            f"{[(t.description_raw, t.amount) for t in txns]}"
        )


# ---------------------------------------------------------------------------
# 4. money_in / money_out mismatch triggers Critical exception
# ---------------------------------------------------------------------------

class TestMoneyTotalsMismatch:
    def test_mismatch_money_in_critical(self):
        data = {
            "money_in": 9896.75,
            "money_out": 9882.16,
            "transactions": [
                _txn("2020-02-19", "BGC SALARY", credit=100.00, balance=125.12),
            ],
        }
        findings = check_money_totals(data)
        assert any(f.severity == "Critical" and "money-in" in f.title.lower() for f in findings), (
            f"Expected Critical money-in finding, got: {findings}"
        )

    def test_mismatch_money_out_critical(self):
        data = {
            "money_in": 100.00,
            "money_out": 9882.16,
            "transactions": [
                _txn("2020-02-19", "BGC SALARY", credit=100.00, balance=125.12),
            ],
        }
        findings = check_money_totals(data)
        assert any(f.severity == "Critical" and "money-out" in f.title.lower() for f in findings)

    def test_no_mismatch_passes(self):
        data = {
            "money_in": 1000.00,
            "money_out": 500.00,
            "transactions": [
                _txn("2020-02-19", "BGC SALARY", credit=1000.00, balance=1025.12),
                _txn("2020-02-20", "DD RENT", debit=500.00, balance=525.12),
            ],
        }
        findings = check_money_totals(data)
        assert findings == []

    def test_no_money_totals_skipped(self):
        """When the statement provides no money_in/money_out, the check is skipped."""
        data = {
            "transactions": [
                _txn("2020-02-19", "BGC SALARY", credit=1000.00, balance=1025.12),
            ],
        }
        findings = check_money_totals(data)
        assert findings == []


# ---------------------------------------------------------------------------
# 5 & 6. Incomplete extraction
# ---------------------------------------------------------------------------

class TestIncompleteExtraction:
    def test_critical_below_25_percent(self):
        """47 of 195 expected = 24% — below 25% threshold should be Critical."""
        txns = [_txn(f"2020-03-{i % 28 + 1:02d}", f"TXN {i}", debit=10.0) for i in range(47)]
        data = {"transactions": txns, "expected_transaction_count": 195}
        findings = check_incomplete_extraction(data)
        assert any(f.severity == "Critical" for f in findings), (
            f"47/195 (24%) should trigger Critical incomplete extraction, got: {findings}"
        )

    def test_high_below_50_percent(self):
        """90 of 195 expected = 46% — should be High."""
        txns = [_txn(f"2020-03-{i % 28 + 1:02d}", f"TXN {i}", debit=10.0) for i in range(90)]
        data = {"transactions": txns, "expected_transaction_count": 195}
        findings = check_incomplete_extraction(data)
        assert any(f.severity == "High" for f in findings), (
            f"90/195 (46%) should trigger High incomplete extraction, got: {findings}"
        )

    def test_no_finding_above_50_percent(self):
        """100 of 195 expected = 51% — no incomplete extraction finding."""
        txns = [_txn(f"2020-03-{i % 28 + 1:02d}", f"TXN {i}", debit=10.0) for i in range(100)]
        data = {"transactions": txns, "expected_transaction_count": 195}
        findings = check_incomplete_extraction(data)
        assert all(f.check != "incomplete_extraction" for f in findings)

    def test_estimated_from_money_totals(self):
        """Without explicit count, derive estimate from money_in + money_out."""
        # money flow = 9896.75 + 9882.16 = 19778.91 / 50 = ~395 estimated
        # only 74 inserted = well below 50% → High finding
        txns = [_txn(f"2020-03-{i % 28 + 1:02d}", f"TXN {i}", debit=10.0) for i in range(74)]
        data = {
            "transactions": txns,
            "money_in": 9896.75,
            "money_out": 9882.16,
        }
        findings = check_incomplete_extraction(data)
        assert any(f.check == "incomplete_extraction" for f in findings), (
            f"74 rows with high money flow should trigger incomplete extraction, got: {findings}"
        )


# ---------------------------------------------------------------------------
# 7. Returned direct debit credit handling
# ---------------------------------------------------------------------------

class TestReturnedDirectDebit:
    def test_returned_dd_credit_not_deduped_with_original_dd(self):
        """A returned DD (credit) and the original DD (debit) must be kept as 2 rows."""
        txns = [
            _txn("2020-03-23", "DD COUNCIL TAX", debit=100.00, balance=900.00, page=3),
            _txn("2020-03-23", "DD COUNCIL TAX", credit=100.00, balance=1000.00, page=3),
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        # Different credit/debit values → different keys → no duplicate finding.
        assert findings == [], (
            f"Original DD and returned DD have different credit/debit values and must NOT be deduped: {findings}"
        )


# ---------------------------------------------------------------------------
# 8. Multiple refunds from same merchant on same day
# ---------------------------------------------------------------------------

class TestMultipleRefundsSameDaySameMerchant:
    def test_five_refunds_different_balance_not_deduped(self):
        """Five refunds from same merchant on same day with increasing balances are preserved."""
        balances = [140.00, 170.00, 200.00, 230.00, 260.00]
        txns = [
            _txn("2020-04-24", "TUI REFUND", credit=30.00, balance=b, page=5)
            for b in balances
        ]
        data = {"transactions": txns}
        findings = check_duplicate_transactions(data)
        assert findings == [], (
            f"Refunds with different balances must not be flagged as duplicates: {findings}"
        )


# ---------------------------------------------------------------------------
# 9 & 10. run_reconciliation integration: BS-0394A9 scenario
# ---------------------------------------------------------------------------

class TestBs0394a9Scenario:
    def _build_data(self, n_txns):
        """Build a synthetic BS-0394A9-like extracted_data with n_txns rows."""
        txns = []
        running = Decimal("25.12")
        for i in range(n_txns):
            amt = Decimal("50.00")
            running += amt
            txns.append({
                "transaction_id": f"t{i}",
                "date": "2020-03-01",
                "description": f"BGC SALARY {i}",
                "credit": float(amt),
                "debit": None,
                "balance": float(running),
                "source_page_number": (i // 20) + 1,
            })
        return {
            "opening_balance": 25.12,
            "closing_balance": float(running),
            "money_in": 9896.75,
            "money_out": 9882.16,
            "expected_transaction_count": 195,
            "transactions": txns,
            "page_count": 10,
            "statement_start_date": "2020-02-19",
            "statement_end_date": "2020-05-18",
        }

    def test_74_rows_raises_exceptions(self):
        """With only 74 rows, run_reconciliation must raise at least one exception."""
        data = self._build_data(74)
        result = run_reconciliation(data)
        assert not result.passed, "74 rows when 195 expected — reconciliation should not pass"
        assert result.findings, "Must have at least one finding for the BS-0394A9 scenario"

    def test_74_rows_raises_money_totals_mismatch(self):
        """74 rows will not sum to the stated money_in/money_out → Critical finding."""
        data = self._build_data(74)
        result = run_reconciliation(data)
        checks = {f.check for f in result.findings}
        assert "money_totals_mismatch" in checks or "incomplete_extraction" in checks, (
            f"Expected money_totals_mismatch or incomplete_extraction, got: {checks}"
        )

    def test_195_rows_with_correct_totals_passes(self):
        """195 rows that sum to stated totals should pass all new checks."""
        # Build 195 rows that DO sum to stated money_in = 9896.75
        txns = []
        running = Decimal("25.12")
        n = 195
        # Divide money_in evenly across n rows
        per_txn = Decimal("9896.75") / n
        for i in range(n):
            running += per_txn
            txns.append({
                "transaction_id": f"t{i}",
                "date": "2020-03-01",
                "description": f"BGC SALARY {i}",
                "credit": float(per_txn),
                "debit": None,
                "balance": float(running),
                "source_page_number": (i // 20) + 1,
            })
        data = {
            "opening_balance": 25.12,
            "closing_balance": float(running),
            "money_in": float(Decimal("9896.75")),
            "money_out": 0.0,
            "expected_transaction_count": 195,
            "transactions": txns,
            "page_count": 10,
            "statement_start_date": "2020-02-19",
            "statement_end_date": "2020-05-18",
        }
        result = run_reconciliation(data)
        money_checks = [f for f in result.findings if f.check in ("money_totals_mismatch", "incomplete_extraction")]
        assert money_checks == [], (
            f"195 rows with correct totals should not trigger money/count checks: {money_checks}"
        )


# ---------------------------------------------------------------------------
# Barclays metadata parsing — money_in / money_out
# ---------------------------------------------------------------------------

class TestBarclaysMetadataParsing:
    def test_parse_total_money_in(self):
        text = (
            "Sort code: 20-55-59  Account number: 13604152\n"
            "19 Feb 20 to 18 May 20\n"
            "Balance Brought Forward 25.12\n"
            "Total money in 9896.75\n"
            "Total money out 9882.16\n"
            "Balance Carried Forward 39.71\n"
        )
        account = _parse_barclays_metadata(text)
        assert account.money_in == Decimal("9896.75"), f"Got money_in={account.money_in}"
        assert account.money_out == Decimal("9882.16"), f"Got money_out={account.money_out}"

    def test_parse_payments_in_out(self):
        text = (
            "Sort code: 20-55-59\n"
            "Payments in 5000.00\n"
            "Payments out 4800.00\n"
        )
        account = _parse_barclays_metadata(text)
        assert account.money_in == Decimal("5000.00")
        assert account.money_out == Decimal("4800.00")

    def test_missing_money_totals_returns_none(self):
        text = "Sort code: 20-55-59\nBalance Brought Forward 100.00\n"
        account = _parse_barclays_metadata(text)
        assert account.money_in is None
        assert account.money_out is None
