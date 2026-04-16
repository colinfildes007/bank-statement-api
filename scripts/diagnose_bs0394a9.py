#!/usr/bin/env python3
"""Diagnostic script for case BS-0394A9.

Prints a full extraction diagnostic for the Barclays current account statement:
  Sort code: 20-55-59
  Account number: 13604152
  Period: 19 Feb 2020 to 18 May 2020

Expected ground-truth values:
  Transaction count : 195
  Money in          : £9,896.75
  Money out         : £9,882.16
  Opening balance   : £25.12
  Closing balance   : £39.71

Usage:
    DATABASE_URL=postgresql://... python scripts/diagnose_bs0394a9.py [case_id] [document_id]

If case_id / document_id are omitted the script discovers all documents whose
account record matches the known sort code / account number.
"""

import os
import sys
from collections import Counter
from decimal import Decimal

# ---------------------------------------------------------------------------
# Ground-truth constants for BS-0394A9
# ---------------------------------------------------------------------------
EXPECTED_TRANSACTION_COUNT = 195
EXPECTED_MONEY_IN = Decimal("9896.75")
EXPECTED_MONEY_OUT = Decimal("9882.16")
EXPECTED_OPENING_BALANCE = Decimal("25.12")
EXPECTED_CLOSING_BALANCE = Decimal("39.71")
SORT_CODE = "20-55-59"
ACCOUNT_NUMBER = "13604152"


def _find_documents(db):
    """Discover document IDs matching the known sort code / account number."""
    from app.models import Account
    accounts = (
        db.query(Account)
        .filter(Account.sort_code == SORT_CODE)
        .filter(Account.account_number_masked == ACCOUNT_NUMBER)
        .all()
    )
    return accounts


def _print_separator(title=""):
    w = 70
    if title:
        print(f"\n{'=' * 3} {title} {'=' * (w - len(title) - 5)}")
    else:
        print("=" * w)


def diagnose(db, document_id: str):
    from app.models import Account, CaseException, ExtractionAudit, Transaction
    from sqlalchemy import func

    _print_separator(f"BS-0394A9 DIAGNOSTIC — document {document_id}")

    # Account info
    account = db.query(Account).filter(Account.document_id == document_id).first()
    print("\n[Account]")
    if account:
        print(f"  Bank:               {account.bank_name}")
        print(f"  Sort code:          {account.sort_code}")
        print(f"  Account number:     {account.account_number_masked}")
        print(f"  Period:             {account.statement_start_date} → {account.statement_end_date}")
        print(f"  Opening balance:    £{account.opening_balance}")
        print(f"  Closing balance:    £{account.closing_balance}")
        print(f"  Money in (stated):  £{account.money_in}")
        print(f"  Money out (stated): £{account.money_out}")
    else:
        print("  (no account record found)")

    # Transaction counts
    txns = db.query(Transaction).filter(Transaction.document_id == document_id).all()
    actual_count = len(txns)
    missing_count = EXPECTED_TRANSACTION_COUNT - actual_count

    total_credit = sum(
        Decimal(str(t.credit)) for t in txns if t.credit is not None
    )
    total_debit = sum(
        Decimal(str(t.debit)) for t in txns if t.debit is not None
    )
    last_balance = txns[-1].balance if txns else None
    derived_closing = (
        EXPECTED_OPENING_BALANCE + total_credit - total_debit
    )

    print("\n[Transaction Counts]")
    print(f"  Expected count:     {EXPECTED_TRANSACTION_COUNT}")
    print(f"  Actual inserted:    {actual_count}")
    print(f"  Missing count:      {missing_count}")
    status = "✓ MATCH" if actual_count == EXPECTED_TRANSACTION_COUNT else "✗ MISMATCH"
    print(f"  Status:             {status}")

    print("\n[Money Totals]")
    print(f"  Expected money in:  £{EXPECTED_MONEY_IN}")
    print(f"  Actual credit sum:  £{total_credit:.2f}")
    in_diff = abs(total_credit - EXPECTED_MONEY_IN)
    print(f"  Difference:         £{in_diff:.2f}  {'✓' if in_diff < Decimal('0.01') else '✗ MISMATCH'}")

    print(f"  Expected money out: £{EXPECTED_MONEY_OUT}")
    print(f"  Actual debit sum:   £{total_debit:.2f}")
    out_diff = abs(total_debit - EXPECTED_MONEY_OUT)
    print(f"  Difference:         £{out_diff:.2f}  {'✓' if out_diff < Decimal('0.01') else '✗ MISMATCH'}")

    print("\n[Closing Balance]")
    print(f"  Expected:           £{EXPECTED_CLOSING_BALANCE}")
    print(f"  Derived from txns:  £{derived_closing:.2f}")
    print(f"  Last txn balance:   £{last_balance}")

    # Repeated descriptions
    desc_counts = Counter(
        (t.transaction_date, t.description_raw) for t in txns
    )
    repeated = [(k, v) for k, v in desc_counts.items() if v > 1]
    repeated.sort(key=lambda x: -x[1])

    print(f"\n[Repeated (date, description) Pairs] — {len(repeated)} pairs with count > 1")
    for (d, desc), cnt in repeated[:20]:
        print(f"  {d}  {cnt}×  {(desc or '')[:60]}")
    if len(repeated) > 20:
        print(f"  … and {len(repeated) - 20} more")

    # Extraction audit
    audit = (
        db.query(ExtractionAudit)
        .filter(ExtractionAudit.document_id == document_id)
        .order_by(ExtractionAudit.created_at.desc())
        .first()
    )
    print("\n[Extraction Audit]")
    if audit:
        print(f"  run_id:             {audit.extraction_run_id}")
        print(f"  processor:          {audit.processor_name} v{audit.processor_version}")
        print(f"  docai_row_count:    {audit.docai_row_count}")
        print(f"  fallback_row_count: {audit.fallback_row_count}")
        print(f"  raw_row_count:      {audit.raw_row_count}")
        print(f"  normalised_count:   {audit.normalised_row_count}")
        print(f"  inserted_count:     {audit.inserted_row_count}")
        print(f"  dropped_count:      {audit.dropped_row_count}")
        print(f"  duplicate_count:    {audit.duplicate_row_count}")
        print(f"  recon_outcome:      {audit.reconciliation_outcome}")
    else:
        print("  (no audit record — run extraction to generate one)")

    # Exceptions raised
    exceptions = (
        db.query(CaseException)
        .filter(CaseException.document_id == document_id)
        .order_by(CaseException.created_at.desc())
        .all()
    )
    print(f"\n[Exceptions Raised] — {len(exceptions)} total")
    for exc in exceptions:
        print(f"  [{exc.severity:8s}] {exc.exception_type:20s} {exc.title}")

    _print_separator()


def main():
    from dotenv import load_dotenv
    load_dotenv()

    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from app.database import SessionLocal
    if SessionLocal is None:
        print("ERROR: DATABASE_URL is not set. Cannot connect to database.")
        sys.exit(1)

    db = SessionLocal()
    try:
        case_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
        doc_id_arg = sys.argv[2] if len(sys.argv) > 2 else None

        if doc_id_arg:
            diagnose(db, doc_id_arg)
        else:
            accounts = _find_documents(db)
            if not accounts:
                print(
                    f"No accounts found with sort_code={SORT_CODE!r} "
                    f"account_number={ACCOUNT_NUMBER!r}."
                )
                if case_id_arg:
                    # Fall back: list documents for the given case
                    from app.models import Document
                    docs = db.query(Document).filter(
                        Document.case_id == case_id_arg
                    ).all()
                    for doc in docs:
                        diagnose(db, doc.document_id)
                return

            for acct in accounts:
                diagnose(db, acct.document_id)
    finally:
        db.close()


if __name__ == "__main__":
    main()
