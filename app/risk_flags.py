"""
Deterministic risk-flag detection over a set of transactions.

Each detector returns a list of flag dicts with keys:
    transaction_id  – str | None  (None for document-level flags)
    flag_type       – str
    severity        – "High" | "Medium" | "Low"
    title           – str (short human-readable label)
    detail          – str (explanation / evidence)
"""

import re
from collections import defaultdict
from decimal import Decimal
from statistics import mean, stdev
from typing import Optional

# ── Compiled keyword patterns ─────────────────────────────────────────────────

_GAMBLING_PATTERN = re.compile(
    r"bet365|betfair|william\s*hill|ladbrokes|\bcoral\b|paddy\s*power|sky\s*bet|"
    r"unibet|888\s*sport|betway|betfred|\bbwin\b|"
    r"national\s*lottery|\blottery\b|scratch\s*card|\bcasino\b|jackpot|"
    r"\bpoker\b|\bbingo\b|\broulette\b|betdaq|betvictor|betsson|888casino|"
    r"mr\s*green|casumo|leovegas|draftkings|fanduel|pointsbet|\bgambling\b|"
    r"flutter\s*entertain|grosvenor\s*casino|mecca\s*bingo|coral\s*betting",
    re.IGNORECASE,
)

_PAYDAY_LOAN_PATTERN = re.compile(
    r"\bwonga\b|quickquid|payday\s*loan|cashfloat|sunny\s*loan|\bpeachy\b|"
    r"\bmyjar\b|lending\s*stream|ferratum|mr\s*lender|118\s*money|amigo\s*loan|"
    r"everyday\s*loans|\boakam\b|moneyboat|wizzcash|piggybank\s*loan|"
    r"high[- ]cost\s*credit|short[- ]term\s*loan|logbook\s*loan|"
    r"satsuma\s*loan|pounds\s*to\s*pocket|vivus\s*loan|creditspring",
    re.IGNORECASE,
)

_RETURNED_PAYMENT_PATTERN = re.compile(
    r"returned\s*item|return\s*chq|unpaid\s*dd|unpaid\s*so|unpaid\s*cheque|"
    r"reverse\s*payment|\breversal\b|bounced\s*cheque|failed\s*payment|"
    r"\br/d\b|rd\s*payment|bacs\s*return|\bchargeback\b|"
    r"\bdishonour\b|failed\s*dd|failed\s*direct\s*debit|returned\s*direct\s*debit|"
    r"refer\s*to\s*drawer|\brtd\b",
    re.IGNORECASE,
)

_CASH_PATTERN = re.compile(
    r"\batm\b|cashpoint|cash\s*machine|cash\s*withdrawal|cash\s*advance|"
    r"link\s*atm|visa\s*cash|mastercard\s*cash|atm\s*withdrawal|"
    r"cash\s*out\b|withdraw\s*cash",
    re.IGNORECASE,
)

# ── Thresholds ────────────────────────────────────────────────────────────────

# Coefficient of variation above this is classed as irregular income.
_INCOME_CV_THRESHOLD = 0.40

# Minimum credits to analyse income regularity.
_MIN_INCOME_CREDITS = 2

# Cash transactions must represent at least this fraction of total debits.
_CASH_DEBIT_RATIO_THRESHOLD = 0.20

# Minimum number of cash transactions before raising a cash-heavy flag.
_MIN_CASH_TRANSACTIONS = 3


# ── Internal helpers ──────────────────────────────────────────────────────────

def _text_fields(txn) -> str:
    """Return a single string combining all searchable text fields of a transaction."""
    parts = [
        txn.description_raw or "",
        txn.description_normalised or "",
        txn.merchant_name or "",
        txn.counterparty_name or "",
        txn.counterparty or "",
        txn.reference or "",
    ]
    return " ".join(parts)


# ── Per-transaction detectors ─────────────────────────────────────────────────

def _flag_gambling(transactions) -> list[dict]:
    flags = []
    for txn in transactions:
        if _GAMBLING_PATTERN.search(_text_fields(txn)):
            flags.append({
                "transaction_id": txn.transaction_id,
                "flag_type": "gambling",
                "severity": "High",
                "title": "Gambling transaction detected",
                "detail": (
                    f"Transaction '{txn.description_raw or txn.description_normalised}' "
                    f"on {txn.transaction_date} matched a known gambling pattern."
                ),
            })
    return flags


def _flag_payday_loans(transactions) -> list[dict]:
    flags = []
    for txn in transactions:
        if _PAYDAY_LOAN_PATTERN.search(_text_fields(txn)):
            flags.append({
                "transaction_id": txn.transaction_id,
                "flag_type": "payday_loan",
                "severity": "High",
                "title": "Payday loan indicator detected",
                "detail": (
                    f"Transaction '{txn.description_raw or txn.description_normalised}' "
                    f"on {txn.transaction_date} matched a known payday-loan pattern."
                ),
            })
    return flags


def _flag_returned_payments(transactions) -> list[dict]:
    flags = []
    for txn in transactions:
        if _RETURNED_PAYMENT_PATTERN.search(_text_fields(txn)):
            flags.append({
                "transaction_id": txn.transaction_id,
                "flag_type": "returned_payment",
                "severity": "Medium",
                "title": "Returned or failed payment detected",
                "detail": (
                    f"Transaction '{txn.description_raw or txn.description_normalised}' "
                    f"on {txn.transaction_date} indicates a returned or failed payment."
                ),
            })
    return flags


def _flag_overdraft_usage(transactions) -> list[dict]:
    flags = []
    for txn in transactions:
        if txn.balance is not None and txn.balance < 0:
            flags.append({
                "transaction_id": txn.transaction_id,
                "flag_type": "overdraft_usage",
                "severity": "Medium",
                "title": "Account in overdraft",
                "detail": (
                    f"Balance was {txn.balance} on {txn.transaction_date} "
                    f"after transaction '{txn.description_raw or txn.description_normalised}'."
                ),
            })
    return flags


# ── Document-level detectors ──────────────────────────────────────────────────

def _flag_irregular_income(transactions) -> list[dict]:
    """
    Flag when credit amounts are highly variable, suggesting irregular income.
    Uses the coefficient of variation (std / mean) across all credit amounts.
    """
    credits = [
        float(txn.amount or txn.credit or 0)
        for txn in transactions
        if (txn.direction or "").lower() == "credit"
        and (txn.amount or txn.credit or 0) > 0
    ]

    if len(credits) < _MIN_INCOME_CREDITS:
        return []

    avg = mean(credits)
    if avg == 0:
        return []

    cv = stdev(credits) / avg if len(credits) > 1 else 0.0

    if cv <= _INCOME_CV_THRESHOLD:
        return []

    return [{
        "transaction_id": None,
        "flag_type": "irregular_income",
        "severity": "Medium",
        "title": "Irregular income pattern detected",
        "detail": (
            f"Credit amounts vary significantly across {len(credits)} credit transactions "
            f"(coefficient of variation: {cv:.2f}, threshold: {_INCOME_CV_THRESHOLD}). "
            f"Average credit: £{avg:.2f}."
        ),
    }]


def _flag_cash_heavy_behaviour(transactions) -> list[dict]:
    """
    Flag when a disproportionate share of debits are ATM/cash withdrawals.
    """
    debit_txns = [
        txn for txn in transactions
        if (txn.direction or "").lower() == "debit"
        or (txn.debit is not None and txn.debit > 0)
    ]
    cash_txns = [
        txn for txn in debit_txns
        if _CASH_PATTERN.search(_text_fields(txn))
    ]

    if len(cash_txns) < _MIN_CASH_TRANSACTIONS:
        return []

    if not debit_txns:
        return []

    ratio = len(cash_txns) / len(debit_txns)
    if ratio <= _CASH_DEBIT_RATIO_THRESHOLD:
        return []

    total_cash = sum(
        float(txn.amount or txn.debit or 0) for txn in cash_txns
    )

    return [{
        "transaction_id": None,
        "flag_type": "cash_heavy_behaviour",
        "severity": "Low",
        "title": "High proportion of cash withdrawals",
        "detail": (
            f"{len(cash_txns)} of {len(debit_txns)} debit transactions "
            f"({ratio:.0%}) are cash withdrawals, totalling £{total_cash:.2f}."
        ),
    }]


# ── Public API ────────────────────────────────────────────────────────────────

def compute_risk_flags(transactions: list) -> list[dict]:
    """
    Run all deterministic risk-flag detectors over the provided transactions.

    Returns a list of flag dicts ready to be persisted as RiskFlag records.
    Each dict contains: transaction_id, flag_type, severity, title, detail.
    """
    flags: list[dict] = []
    flags.extend(_flag_gambling(transactions))
    flags.extend(_flag_payday_loans(transactions))
    flags.extend(_flag_returned_payments(transactions))
    flags.extend(_flag_overdraft_usage(transactions))
    flags.extend(_flag_irregular_income(transactions))
    flags.extend(_flag_cash_heavy_behaviour(transactions))
    return flags
