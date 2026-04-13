"""Google Document AI client and response normaliser for bank statement extraction."""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
GOOGLE_LOCATION = os.getenv("GOOGLE_LOCATION", "us")
GOOGLE_DOCAI_PROCESSOR_ID = os.getenv("GOOGLE_DOCAI_PROCESSOR_ID")
# Optional: pin to a specific trained processor version (e.g. a deployed custom extractor).
# When unset, the processor's default (stable) version is used.
GOOGLE_DOCAI_PROCESSOR_VERSION = os.getenv("GOOGLE_DOCAI_PROCESSOR_VERSION")

# Transactions with extractor confidence below this threshold trigger an exception.
CONFIDENCE_THRESHOLD = Decimal(str(os.getenv("DOCAI_CONFIDENCE_THRESHOLD", "0.8")))


@dataclass
class NormalisedAccount:
    bank_name: Optional[str] = None
    account_holder_name: Optional[str] = None
    sort_code: Optional[str] = None
    account_number_masked: Optional[str] = None
    statement_start_date: Optional[date] = None
    statement_end_date: Optional[date] = None
    opening_balance: Optional[Decimal] = None
    closing_balance: Optional[Decimal] = None


@dataclass
class NormalisedTransaction:
    transaction_date: Optional[date] = None
    posting_date: Optional[date] = None
    description_raw: Optional[str] = None
    description_normalised: Optional[str] = None
    direction: Optional[str] = None
    amount: Optional[Decimal] = None
    balance: Optional[Decimal] = None
    merchant_name: Optional[str] = None
    counterparty_name: Optional[str] = None
    extractor_confidence: Decimal = field(default_factory=lambda: Decimal("1.0"))
    source_page_number: Optional[int] = None
    source_row_reference: Optional[str] = None


@dataclass
class ExtractionResult:
    account: NormalisedAccount = field(default_factory=NormalisedAccount)
    transactions: list = field(default_factory=list)


def _get_processor_client():
    """Return an authenticated Document AI DocumentProcessorServiceClient."""
    from google.cloud import documentai

    google_key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_JSON")
    if google_key_json:
        import google.oauth2.service_account as sa_module

        key_data = json.loads(google_key_json)
        credentials = sa_module.Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return documentai.DocumentProcessorServiceClient(credentials=credentials)

    return documentai.DocumentProcessorServiceClient()


def _parse_date(value: str) -> Optional[date]:
    """Try date formats in order of specificity: ISO 8601 first, then unambiguous
    locale formats, then potentially ambiguous ones (%m/%d/%Y last)."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            from datetime import datetime as dt

            return dt.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_amount(value: str) -> Optional[Decimal]:
    """Strip currency symbols/commas and return a Decimal, or None if not parseable."""
    if not value:
        return None
    cleaned = re.sub(r"[£$€,\s]", "", value.strip())
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _entity_text(entity) -> str:
    return (entity.mention_text or "").strip()


def _entity_confidence(entity) -> Decimal:
    return Decimal(str(getattr(entity, "confidence", 1.0)))


def _normalise_direction(raw: str) -> Optional[str]:
    """Map common debit/credit labels to a canonical direction string."""
    if not raw:
        return None
    lowered = raw.lower().strip()
    if lowered in ("cr", "credit", "in", "deposit"):
        return "credit"
    if lowered in ("dr", "debit", "out", "withdrawal"):
        return "debit"
    return lowered


def _normalise_description(raw: str) -> str:
    """Basic normalisation: collapse whitespace and remove leading/trailing noise."""
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw).strip()


def process_document(file_bytes: bytes, mime_type: str) -> ExtractionResult:
    """
    Send *file_bytes* to Google Document AI and return a normalised
    :class:`ExtractionResult` containing account-level data and a list
    of :class:`NormalisedTransaction` objects.

    Required environment variables
    --------------------------------
    GOOGLE_PROJECT_ID             GCP project that owns the processor
    GOOGLE_LOCATION               Processor region (default: "us")
    GOOGLE_DOCAI_PROCESSOR_ID     Processor ID (without the full resource path)

    Optional
    --------
    GOOGLE_DOCAI_PROCESSOR_VERSION    Specific trained processor version to call.
                                      When unset, the processor's default stable
                                      version is used.  Required when using a
                                      custom extractor with a trained version.
    GOOGLE_SERVICE_ACCOUNT_KEY_JSON   Inline JSON service-account key
    GOOGLE_APPLICATION_CREDENTIALS    Path to a service-account key file
    DOCAI_CONFIDENCE_THRESHOLD        Float in [0, 1]; default 0.8
    """
    if not GOOGLE_PROJECT_ID or not GOOGLE_DOCAI_PROCESSOR_ID:
        raise RuntimeError(
            "GOOGLE_PROJECT_ID and GOOGLE_DOCAI_PROCESSOR_ID must be set"
        )

    if not file_bytes:
        raise ValueError("file_bytes must not be empty")

    from google.cloud import documentai

    client = _get_processor_client()

    if GOOGLE_DOCAI_PROCESSOR_VERSION:
        processor_name = client.processor_version_path(
            GOOGLE_PROJECT_ID,
            GOOGLE_LOCATION,
            GOOGLE_DOCAI_PROCESSOR_ID,
            GOOGLE_DOCAI_PROCESSOR_VERSION,
        )
        logger.info(
            "Sending document to Document AI processor %s version %s (size=%d bytes, mime=%s)",
            GOOGLE_DOCAI_PROCESSOR_ID,
            GOOGLE_DOCAI_PROCESSOR_VERSION,
            len(file_bytes),
            mime_type,
        )
    else:
        processor_name = client.processor_path(
            GOOGLE_PROJECT_ID, GOOGLE_LOCATION, GOOGLE_DOCAI_PROCESSOR_ID
        )
        logger.info(
            "Sending document to Document AI processor %s (size=%d bytes, mime=%s)",
            GOOGLE_DOCAI_PROCESSOR_ID,
            len(file_bytes),
            mime_type,
        )

    raw_document = documentai.RawDocument(content=file_bytes, mime_type=mime_type)
    request = documentai.ProcessRequest(name=processor_name, raw_document=raw_document)

    response = client.process_document(request=request)
    document = response.document

    entity_count = len(document.entities) if document.entities else 0
    doc_text_len = len(document.text) if document.text else 0
    logger.info(
        "Document AI response for processor %s: text_length=%d, entity_count=%d",
        GOOGLE_DOCAI_PROCESSOR_ID,
        doc_text_len,
        entity_count,
    )
    if logger.isEnabledFor(logging.DEBUG) and entity_count:
        entity_types = sorted({(e.type_ or "").lower() for e in document.entities})
        logger.debug("Document AI entity types found: %s", entity_types)
    if entity_count == 0:
        logger.warning(
            "Document AI returned 0 entities for processor %s. "
            "Verify the processor ID, location, and processor version are correct, "
            "and that the processor is enabled and has a deployed version.",
            GOOGLE_DOCAI_PROCESSOR_ID,
        )

    return _normalise_response(document)


def _normalise_response(document) -> ExtractionResult:
    """
    Map Document AI entity types onto the canonical schema.

    Document AI bank-statement processors typically emit entities such as:
      - account_holder_name / holder_name
      - bank_name
      - sort_code / routing_number
      - account_number / account_number_masked
      - statement_start_date / period_start
      - statement_end_date / period_end
      - opening_balance / start_balance
      - closing_balance / end_balance
      - transaction (parent entity with children: date, description,
        debit_amount / credit_amount / amount, balance, type)
    """
    account = NormalisedAccount()
    transactions: list[NormalisedTransaction] = []

    # Map of known entity type names to account fields
    account_field_map = {
        "account_holder_name": "account_holder_name",
        "holder_name": "account_holder_name",
        "bank_name": "bank_name",
        "sort_code": "sort_code",
        "routing_number": "sort_code",
        "account_number": "account_number_masked",
        "account_number_masked": "account_number_masked",
        "statement_start_date": "statement_start_date",
        "period_start": "statement_start_date",
        "statement_end_date": "statement_end_date",
        "period_end": "statement_end_date",
        "opening_balance": "opening_balance",
        "start_balance": "opening_balance",
        "closing_balance": "closing_balance",
        "end_balance": "closing_balance",
    }

    for entity in document.entities:
        etype = (entity.type_ or "").lower().strip()

        if etype in account_field_map:
            field_name = account_field_map[etype]
            raw = _entity_text(entity)
            if field_name.endswith("_date"):
                setattr(account, field_name, _parse_date(raw))
            elif field_name.endswith("_balance"):
                setattr(account, field_name, _parse_amount(raw))
            else:
                setattr(account, field_name, raw)

        elif etype == "transaction":
            txn = _parse_transaction_entity(entity)
            transactions.append(txn)

    return ExtractionResult(account=account, transactions=transactions)


def _parse_transaction_entity(entity) -> NormalisedTransaction:
    """Extract a single transaction from a Document AI parent entity."""
    txn = NormalisedTransaction()
    txn.extractor_confidence = _entity_confidence(entity)
    txn.source_page_number = _first_page_number(entity)

    child_map = {
        "date": "transaction_date",
        "transaction_date": "transaction_date",
        "posting_date": "posting_date",
        "value_date": "posting_date",
        "description": "description_raw",
        "narrative": "description_raw",
        "details": "description_raw",
        "debit_amount": "__debit",
        "credit_amount": "__credit",
        "amount": "__amount",
        "balance": "balance",
        "running_balance": "balance",
        "type": "__direction",
        "direction": "__direction",
        "merchant_name": "merchant_name",
        "counterparty_name": "counterparty_name",
        "counterparty": "counterparty_name",
    }

    debit_amount: Optional[float] = None
    credit_amount: Optional[float] = None
    raw_amount: Optional[float] = None
    raw_direction: Optional[str] = None

    for child in entity.properties:
        ctype = (child.type_ or "").lower().strip()
        raw = _entity_text(child)
        target = child_map.get(ctype)

        if target is None:
            continue
        elif target == "__debit":
            debit_amount = _parse_amount(raw)
        elif target == "__credit":
            credit_amount = _parse_amount(raw)
        elif target == "__amount":
            raw_amount = _parse_amount(raw)
        elif target == "__direction":
            raw_direction = raw
        elif target in ("transaction_date", "posting_date"):
            setattr(txn, target, _parse_date(raw))
        elif target == "balance":
            txn.balance = _parse_amount(raw)
        elif target == "description_raw":
            txn.description_raw = raw
            txn.description_normalised = _normalise_description(raw)
        else:
            setattr(txn, target, raw)

        # Use the child confidence if lower than the parent entity confidence
        child_conf = _entity_confidence(child)
        if child_conf < txn.extractor_confidence:
            txn.extractor_confidence = child_conf

    # Resolve direction and amount
    if debit_amount is not None:
        txn.amount = debit_amount
        txn.direction = "debit"
    elif credit_amount is not None:
        txn.amount = credit_amount
        txn.direction = "credit"
    elif raw_amount is not None:
        txn.amount = raw_amount
        txn.direction = _normalise_direction(raw_direction or "")

    if txn.direction is None and raw_direction:
        txn.direction = _normalise_direction(raw_direction)

    return txn


def _first_page_number(entity) -> Optional[int]:
    """Return the 1-based page number of the first text segment anchor, if any."""
    try:
        page_refs = entity.page_anchor.page_refs
        if page_refs:
            return int(page_refs[0].page) + 1
    except (AttributeError, IndexError, ValueError):
        pass
    return None
