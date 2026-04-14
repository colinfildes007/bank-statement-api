"""Fallback PDF text parser for bank statements.

Used when Google Document AI returns no transactions from a PDF document.
Supports Barclays-format statements and includes a heuristic generic
parser for other UK high-street bank layouts.

Confidence scores on text-parsed transactions are set to 0.75 so they
are flagged for human review but do not block downstream processing.
"""

import io
import logging
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple

import pypdf

from app.documentai import ExtractionResult, NormalisedAccount, NormalisedTransaction

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

# "01 Jan 2024" / "1 January 2024" (abbreviated or full month name)
_DATE_LONG = re.compile(
    r"\b(\d{1,2})\s+"
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)

# "01/01/2024" or "01-01-2024" (day/month/year)
_DATE_DMY = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")

# Monetary amount: 1,234.56 or 0.00
_AMOUNT = re.compile(r"\b(\d{1,3}(?:,\d{3})*\.\d{2})\b")

_MONTH_SHORT = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_KNOWN_BANKS = [
    "barclays", "hsbc", "lloyds", "natwest", "santander", "halifax",
    "nationwide", "monzo", "starling", "revolut", "metro bank",
    "first direct", "bank of scotland", "rbs", "royal bank of scotland",
    "co-operative bank", "virgin money", "tsb",
]

# Confidence level assigned to all text-parsed transactions so downstream
# reconciliation flags them for manual review.
_TEXT_PARSE_CONFIDENCE = Decimal("0.75")

# Maximum number of lines to collect after a date-headed line when building
# a single transaction block (description + amounts may span several lines).
_MAX_LOOKAHEAD_LINES = 6

# Tolerance (in currency units) when checking whether a balance delta exactly
# matches a transaction amount.  2p covers minor PDF rounding artefacts.
_BALANCE_TOLERANCE = Decimal("0.02")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _parse_decimal(text: str) -> Optional[Decimal]:
    cleaned = re.sub(r"[£$€,\s]", "", (text or "").strip())
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _parse_date_long(m: re.Match) -> Optional[date]:
    month = _MONTH_SHORT.get(m.group(2)[:3].lower())
    if month is None:
        return None
    try:
        return date(int(m.group(3)), month, int(m.group(1)))
    except ValueError:
        return None


def _parse_date_dmy(m: re.Match) -> Optional[date]:
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _extract_page_texts(file_bytes: bytes) -> List[str]:
    reader = pypdf.PdfReader(io.BytesIO(file_bytes))
    return [(page.extract_text() or "") for page in reader.pages]


def _amounts_in(text: str) -> List[Decimal]:
    return [
        v for v in (_parse_decimal(m.group(1)) for m in _AMOUNT.finditer(text))
        if v is not None
    ]


def _strip_amounts(text: str) -> str:
    return re.sub(r"\s+", " ", _AMOUNT.sub("", text)).strip()


# ---------------------------------------------------------------------------
# Barclays-specific header / metadata parsing
# ---------------------------------------------------------------------------

def _parse_barclays_metadata(full_text: str) -> NormalisedAccount:
    account = NormalisedAccount(bank_name="Barclays")

    # Sort code: "Sort code: 20-00-00" or bare "20-00-00"
    m = re.search(r"\bSort\s+code[:\s]+(\d{2}[-\u2013]\d{2}[-\u2013]\d{2})", full_text, re.I)
    if not m:
        m = re.search(r"\b(\d{2}-\d{2}-\d{2})\b", full_text)
    if m:
        account.sort_code = m.group(1)

    # Account number: "Account number: 12345678" or "A/C No: 12345678"
    m = re.search(r"\bAccount\s+number[:\s]+(\d{7,8})\b", full_text, re.I)
    if not m:
        m = re.search(r"\bA/C\s+No\.?\s*:?\s*(\d{7,8})\b", full_text, re.I)
    if m:
        account.account_number_masked = m.group(1)

    # Account holder: title + name (restrict to same line with [ \t]+ to avoid
    # crossing into adjacent header fields on the next line).
    # Supports up to 5 additional name parts after the first to accommodate
    # multi-part names (e.g. "Dr Jean-Pierre Van Der Berg").
    m = re.search(
        r"(?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?[ \t]+([A-Z][A-Za-z\-']+(?:[ \t]+[A-Z][A-Za-z\-']+){0,5})",
        full_text,
    )
    if m:
        account.account_holder_name = m.group(0).strip()

    # Statement period: "1 January 2024 to 31 January 2024"
    m = re.search(
        r"(\d{1,2})\s+"
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})"
        r"\s+to\s+"
        r"(\d{1,2})\s+"
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})",
        full_text, re.I,
    )
    if m:
        sm = _MONTH_SHORT.get(m.group(2)[:3].lower())
        em = _MONTH_SHORT.get(m.group(5)[:3].lower())
        try:
            if sm:
                account.statement_start_date = date(int(m.group(3)), sm, int(m.group(1)))
            if em:
                account.statement_end_date = date(int(m.group(6)), em, int(m.group(4)))
        except ValueError:
            pass

    # Explicit opening / closing balance labels
    m = re.search(r"[Oo]pening\s+[Bb]alance\s+[£]?([\d,]+\.\d{2})", full_text)
    if m:
        account.opening_balance = _parse_decimal(m.group(1))

    m = re.search(r"[Cc]losing\s+[Bb]alance\s+[£]?([\d,]+\.\d{2})", full_text)
    if m:
        account.closing_balance = _parse_decimal(m.group(1))

    return account


# ---------------------------------------------------------------------------
# Generic metadata parsing (best-effort for non-Barclays PDFs)
# ---------------------------------------------------------------------------

def _parse_generic_metadata(full_text: str) -> NormalisedAccount:
    account = NormalisedAccount()
    for bank in _KNOWN_BANKS:
        if bank in full_text.lower():
            account.bank_name = bank.title()
            break

    m = re.search(r"[Oo]pening\s+[Bb]alance\s+[£]?([\d,]+\.\d{2})", full_text)
    if m:
        account.opening_balance = _parse_decimal(m.group(1))

    m = re.search(r"[Cc]losing\s+[Bb]alance\s+[£]?([\d,]+\.\d{2})", full_text)
    if m:
        account.closing_balance = _parse_decimal(m.group(1))

    return account


# ---------------------------------------------------------------------------
# Transaction row parser (shared logic)
# ---------------------------------------------------------------------------

def _parse_transactions(pages: List[str], account: NormalisedAccount) -> List[NormalisedTransaction]:
    """Parse transaction rows from extracted PDF page text.

    Strategy
    --------
    1. Scan lines for those that *start* with a recognisable date token.
    2. For each date-headed block (the date line plus following lines up to
       the next date), collect all monetary amounts.
    3. Interpret: last amount = running balance; penultimate = transaction
       amount (money out / money in).
    4. Infer direction (debit / credit) from whether the running balance
       went up (+credit) or down (+debit) relative to the previous row.
    """
    transactions: List[NormalisedTransaction] = []
    prev_balance: Optional[Decimal] = account.opening_balance

    for page_num, page_text in enumerate(pages, start=1):
        lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]

            dm_long = _DATE_LONG.match(line)
            dm_dmy = _DATE_DMY.match(line)

            # Try the named-month pattern first; fall back to DD/MM/YYYY if the
            # named-month match exists but yields an invalid calendar date.
            txn_date = None
            dm = None
            if dm_long:
                txn_date = _parse_date_long(dm_long)
                dm = dm_long
            if txn_date is None and dm_dmy:
                txn_date = _parse_date_dmy(dm_dmy)
                dm = dm_dmy

            if txn_date is None or dm is None:
                i += 1
                continue

            # Rest of the date line (description + possible amounts)
            rest = line[dm.end():].strip()

            # Collect lookahead lines until the next date-headed line
            lookahead: List[str] = []
            j = i + 1
            while j < min(i + _MAX_LOOKAHEAD_LINES, len(lines)):
                nxt = lines[j]
                if _DATE_LONG.match(nxt) or _DATE_DMY.match(nxt):
                    break
                lookahead.append(nxt)
                j += 1

            combined = " ".join(filter(None, [rest] + lookahead))
            amounts = _amounts_in(combined)
            desc = _strip_amounts(combined) or None

            # "Opening balance" row — treat as account-level metadata, not a transaction
            if re.search(r"\bopening\s+balance\b", combined, re.I):
                if amounts and account.opening_balance is None:
                    account.opening_balance = amounts[-1]
                prev_balance = account.opening_balance or (amounts[-1] if amounts else None)
                i = j if j > i + 1 else i + 1
                continue

            # Need at least 2 amounts to build a transaction (txn_amount + balance)
            if len(amounts) < 2:
                if len(amounts) == 1 and prev_balance is None:
                    logger.debug(
                        "pdf_fallback: single amount %.2f on %s treated as initial balance",
                        amounts[0], txn_date,
                    )
                    prev_balance = amounts[0]
                i = j if j > i + 1 else i + 1
                continue

            balance = amounts[-1]
            txn_amount = amounts[-2]

            # Infer direction from balance movement
            direction: Optional[str] = None
            if prev_balance is not None:
                delta = balance - prev_balance
                if abs(delta - txn_amount) < _BALANCE_TOLERANCE:
                    direction = "credit"
                elif abs(delta + txn_amount) < _BALANCE_TOLERANCE:
                    direction = "debit"
                else:
                    # Ambiguous — pick the sign of the delta as best guess
                    direction = "credit" if delta >= 0 else "debit"

            txn = NormalisedTransaction(
                transaction_date=txn_date,
                description_raw=desc,
                description_normalised=desc,
                direction=direction,
                amount=txn_amount,
                balance=balance,
                extractor_confidence=_TEXT_PARSE_CONFIDENCE,
                source_page_number=page_num,
            )
            transactions.append(txn)
            prev_balance = balance
            i = j if j > i + 1 else i + 1

    # Infer closing balance from the last transaction's running balance
    if transactions and account.closing_balance is None:
        account.closing_balance = transactions[-1].balance

    return transactions


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def extract_from_pdf_text(file_bytes: bytes) -> ExtractionResult:
    """Extract account and transaction data from PDF bytes using text parsing.

    This is a best-effort fallback used when Google Document AI returns no
    entities.  All extracted transactions receive a confidence score of
    ``_TEXT_PARSE_CONFIDENCE`` (0.75) so they are flagged for human review.

    Returns an :class:`~app.documentai.ExtractionResult` that may be empty
    if no text can be extracted from the PDF (e.g. scanned image PDFs).
    """
    try:
        pages = _extract_page_texts(file_bytes)
    except Exception as exc:
        logger.warning("pdf_fallback: could not read PDF: %s", exc)
        return ExtractionResult()

    full_text = "\n".join(pages)
    if not full_text.strip():
        logger.warning("pdf_fallback: PDF yielded no extractable text (may be image-only)")
        return ExtractionResult()

    lower = full_text.lower()
    bank = next((b for b in _KNOWN_BANKS if b in lower), None)
    logger.info("pdf_fallback: detected bank=%r, text_length=%d", bank, len(full_text))

    if bank == "barclays":
        account = _parse_barclays_metadata(full_text)
    else:
        account = _parse_generic_metadata(full_text)

    transactions = _parse_transactions(pages, account)

    logger.info(
        "pdf_fallback: extracted %d transaction(s) for bank=%r",
        len(transactions), bank,
    )
    return ExtractionResult(account=account, transactions=transactions)
