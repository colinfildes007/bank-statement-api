import io
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import pypdf
from botocore.exceptions import BotoCoreError, ClientError

from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Account, CaseException, Document, ProcessingJob, Transaction, ValidationResult

logger = logging.getLogger(__name__)

ALLOWED_MIME_TYPES = {
    "application/pdf",
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "image/jpeg",
    "image/png",
    "image/tiff",
}

ALLOWED_EXTENSIONS = {".pdf", ".csv", ".xls", ".xlsx", ".jpg", ".jpeg", ".png", ".tif", ".tiff"}

KNOWN_BANKS = [
    "barclays", "hsbc", "lloyds", "natwest", "santander", "halifax",
    "nationwide", "monzo", "starling", "revolut", "metro bank", "first direct",
    "bank of scotland", "rbs", "royal bank of scotland", "co-operative bank",
    "coop bank", "virgin money", "tsb", "ulster bank", "danske bank",
    "allied irish", "bank of ireland",
]

DATE_PATTERN = re.compile(
    r"\b(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}|\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2}|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}|"
    r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4})\b",
    re.IGNORECASE,
)


def _mark_job_started(db, job_id: str):
    job = db.query(ProcessingJob).filter(ProcessingJob.job_id == job_id).first()
    if job:
        job.status = "Running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()
    return job


def _mark_job_completed(db, job_id: str, result: dict):
    job = db.query(ProcessingJob).filter(ProcessingJob.job_id == job_id).first()
    if job:
        job.status = "Completed"
        job.completed_at = datetime.now(timezone.utc)
        job.result_json = json.dumps(result)
        db.commit()


def _mark_job_failed(db, job_id: str, error_code: str, error_message: str):
    job = db.query(ProcessingJob).filter(ProcessingJob.job_id == job_id).first()
    if job:
        job.status = "Failed"
        job.completed_at = datetime.now(timezone.utc)
        job.error_code = error_code
        job.error_message = error_message
        db.commit()


def _save_validation_result(db, document_id: str, job_id: str, check_name: str,
                             passed: bool, severity: str, result_code: str,
                             message: str, details: dict = None):
    vr = ValidationResult(
        validation_result_id=f"vr_{uuid4().hex[:12]}",
        document_id=document_id,
        job_id=job_id,
        check_name=check_name,
        severity=severity,
        passed=passed,
        result_code=result_code,
        message=message,
        details_json=json.dumps(details) if details else None,
    )
    db.add(vr)
    db.commit()
    return vr


def _save_exception(db, document: Document, job_id: str, exception_type: str,
                    severity: str, title: str, description: str):
    exc = CaseException(
        exception_id=f"exc_{uuid4().hex[:12]}",
        case_id=document.case_id,
        document_id=document.document_id,
        job_id=job_id,
        exception_type=exception_type,
        severity=severity,
        title=title,
        description=description,
    )
    db.add(exc)
    db.commit()


def _fetch_file_from_s3(storage_key: str) -> bytes | None:
    """Fetch raw file bytes from S3. Returns None on any error."""
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket or not storage_key:
        return None
    try:
        client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        response = client.get_object(Bucket=bucket, Key=storage_key)
        return response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        logger.warning("S3 fetch failed for key %s: %s", storage_key, exc)
        return None


def _run_checks(db, document: Document, job_id: str) -> list[dict]:
    """Run all validation checks and persist results. Returns list of check outcomes."""
    outcomes = []
    file_bytes: bytes | None = None

    # --- 1. file_exists ---
    exists = bool(document.storage_key or document.file_size)
    _save_validation_result(
        db, document.document_id, job_id,
        check_name="file_exists",
        passed=exists,
        severity="Critical",
        result_code="FILE_EXISTS" if exists else "FILE_MISSING",
        message="Document record has storage reference" if exists else "No storage key or file size recorded",
        details={"storage_key": document.storage_key, "file_size": document.file_size},
    )
    if not exists:
        _save_exception(
            db, document, job_id,
            exception_type="validation_failure",
            severity="Critical",
            title="Document file reference missing",
            description="The document record has neither a storage key nor a recorded file size.",
        )
    outcomes.append({"check": "file_exists", "passed": exists})

    # --- 2. file_type_allowed ---
    ext = os.path.splitext(document.original_filename or "")[-1].lower()
    mime_ok = document.mime_type in ALLOWED_MIME_TYPES if document.mime_type else False
    ext_ok = ext in ALLOWED_EXTENSIONS
    type_allowed = mime_ok or ext_ok
    _save_validation_result(
        db, document.document_id, job_id,
        check_name="file_type_allowed",
        passed=type_allowed,
        severity="High",
        result_code="TYPE_ALLOWED" if type_allowed else "TYPE_NOT_ALLOWED",
        message=f"MIME type '{document.mime_type}', extension '{ext}'" + ("" if type_allowed else " — not in allowed list"),
        details={"mime_type": document.mime_type, "extension": ext},
    )
    if not type_allowed:
        _save_exception(
            db, document, job_id,
            exception_type="validation_failure",
            severity="High",
            title="File type not allowed",
            description=f"MIME type '{document.mime_type}' and extension '{ext}' are not in the allowed list.",
        )
    outcomes.append({"check": "file_type_allowed", "passed": type_allowed})

    # --- 3. file_not_empty ---
    not_empty = bool(document.file_size and document.file_size > 0)
    _save_validation_result(
        db, document.document_id, job_id,
        check_name="file_not_empty",
        passed=not_empty,
        severity="High",
        result_code="FILE_NOT_EMPTY" if not_empty else "FILE_EMPTY",
        message=f"File size: {document.file_size} bytes" if document.file_size is not None else "File size unknown",
        details={"file_size": document.file_size},
    )
    if not not_empty:
        _save_exception(
            db, document, job_id,
            exception_type="validation_failure",
            severity="High",
            title="File is empty or size unknown",
            description=f"Recorded file size is {document.file_size!r}.",
        )
    outcomes.append({"check": "file_not_empty", "passed": not_empty})

    # --- 4. file_readable (fetch from S3) ---
    if document.storage_key:
        file_bytes = _fetch_file_from_s3(document.storage_key)
        readable = file_bytes is not None
        _save_validation_result(
            db, document.document_id, job_id,
            check_name="file_readable",
            passed=readable,
            severity="Critical",
            result_code="FILE_READABLE" if readable else "FILE_UNREADABLE",
            message="File successfully retrieved from storage" if readable else "Could not retrieve file from storage",
            details={"storage_key": document.storage_key, "bytes_read": len(file_bytes) if file_bytes else 0},
        )
        if not readable:
            _save_exception(
                db, document, job_id,
                exception_type="validation_failure",
                severity="Critical",
                title="File could not be read from storage",
                description=f"Attempt to read '{document.storage_key}' from S3 failed.",
            )
        outcomes.append({"check": "file_readable", "passed": readable})
    else:
        _save_validation_result(
            db, document.document_id, job_id,
            check_name="file_readable",
            passed=False,
            severity="Critical",
            result_code="NO_STORAGE_KEY",
            message="No storage key — cannot attempt read",
        )
        outcomes.append({"check": "file_readable", "passed": False})

    # --- 5. page_count_detected (PDFs only) ---
    is_pdf = (document.mime_type == "application/pdf") or ext == ".pdf"
    if is_pdf and file_bytes:
        try:
            reader = pypdf.PdfReader(io.BytesIO(file_bytes))
            page_count = len(reader.pages)
            detected = page_count > 0
            _save_validation_result(
                db, document.document_id, job_id,
                check_name="page_count_detected",
                passed=detected,
                severity="Medium",
                result_code="PAGE_COUNT_DETECTED" if detected else "NO_PAGES",
                message=f"{page_count} page(s) detected" if detected else "PDF appears to have no pages",
                details={"page_count": page_count},
            )
            if not detected:
                _save_exception(
                    db, document, job_id,
                    exception_type="validation_failure",
                    severity="Medium",
                    title="PDF has no pages",
                    description="The PDF was opened successfully but reports zero pages.",
                )
            outcomes.append({"check": "page_count_detected", "passed": detected, "page_count": page_count})
        except Exception as pdf_err:
            _save_validation_result(
                db, document.document_id, job_id,
                check_name="page_count_detected",
                passed=False,
                severity="Medium",
                result_code="PDF_PARSE_ERROR",
                message=f"Could not parse PDF: {pdf_err}",
            )
            _save_exception(
                db, document, job_id,
                exception_type="validation_failure",
                severity="Medium",
                title="PDF could not be parsed",
                description=str(pdf_err),
            )
            outcomes.append({"check": "page_count_detected", "passed": False})
    else:
        _save_validation_result(
            db, document.document_id, job_id,
            check_name="page_count_detected",
            passed=True,
            severity="Low",
            result_code="NOT_APPLICABLE",
            message="Page count check skipped for non-PDF files",
        )
        outcomes.append({"check": "page_count_detected", "passed": True, "skipped": True})

    # --- 6. statement_date_range_present ---
    extracted_text = ""
    if is_pdf and file_bytes:
        try:
            reader = pypdf.PdfReader(io.BytesIO(file_bytes))
            extracted_text = " ".join(
                (page.extract_text() or "") for page in reader.pages[:5]
            )
        except Exception:
            pass

    dates_found = DATE_PATTERN.findall(extracted_text)
    date_range_present = len(dates_found) >= 2
    _save_validation_result(
        db, document.document_id, job_id,
        check_name="statement_date_range_present",
        passed=date_range_present if extracted_text else True,
        severity="Low",
        result_code="DATE_RANGE_PRESENT" if date_range_present else ("DATE_RANGE_NOT_FOUND" if extracted_text else "TEXT_NOT_EXTRACTED"),
        message=(
            f"Found {len(dates_found)} date(s) in document text" if extracted_text
            else "Date range check skipped — text could not be extracted"
        ),
        details={"dates_found": dates_found[:10]},
    )
    outcomes.append({"check": "statement_date_range_present", "passed": date_range_present if extracted_text else True})

    # --- 7. bank_type_identified ---
    lower_text = extracted_text.lower() + " " + (document.original_filename or "").lower()
    matched_banks = [b for b in KNOWN_BANKS if b in lower_text]
    bank_identified = len(matched_banks) > 0
    _save_validation_result(
        db, document.document_id, job_id,
        check_name="bank_type_identified",
        passed=bank_identified if extracted_text else True,
        severity="Low",
        result_code="BANK_IDENTIFIED" if bank_identified else ("BANK_NOT_IDENTIFIED" if extracted_text else "TEXT_NOT_EXTRACTED"),
        message=(
            f"Bank(s) identified: {', '.join(matched_banks)}" if bank_identified
            else ("No known bank name found in document" if extracted_text else "Bank check skipped — text could not be extracted")
        ),
        details={"matched_banks": matched_banks},
    )
    outcomes.append({"check": "bank_type_identified", "passed": bank_identified if extracted_text else True})

    return outcomes


@celery_app.task(bind=True, name="validate_document_task")
def validate_document_task(self, document_id: str, job_id: str):
    """Async task to validate a document with real file checks."""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()

        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        _mark_job_started(db, job_id)

        outcomes = _run_checks(db, document, job_id)

        all_critical_passed = all(
            o["passed"] for o in outcomes
            if o.get("check") in ("file_exists", "file_readable")
        )

        if all_critical_passed:
            document.status = "Validated"
        else:
            document.status = "ValidationFailed"
        db.commit()

        result = {
            "document_id": document_id,
            "status": document.status,
            "checks_run": len(outcomes),
            "checks_passed": sum(1 for o in outcomes if o["passed"]),
            "checks_failed": sum(1 for o in outcomes if not o["passed"]),
            "outcomes": outcomes,
        }
        if all_critical_passed:
            _mark_job_completed(db, job_id, result)
        else:
            _mark_job_failed(db, job_id, "VALIDATION_FAILED", "One or more critical validation checks failed")
        return result

    except Exception as exc:
        _mark_job_failed(db, job_id, "VALIDATION_ERROR", str(exc))
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="extract_document_task")
def extract_document_task(self, document_id: str, job_id: str):
    """
    Extraction worker that:
      1. Fetches the stored file from S3
      2. Sends it to Google Document AI
      3. Normalises the response
      4. Saves Account and Transaction records
      5. Creates exceptions for low-confidence rows
      6. Updates the document and job status
    """
    from app.documentai import CONFIDENCE_THRESHOLD, process_document
    from app.storage import download_file_from_s3

    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()
        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        if not document.storage_key:
            _mark_job_failed(db, job_id, "NO_STORAGE_KEY", "Document has no storage key")
            raise ValueError(f"Document {document_id} has no storage key")

        _mark_job_started(db, job_id)

        # 1. Download file
        logger.info("Downloading document %s from storage", document_id)
        file_bytes = download_file_from_s3(document.storage_key)
        mime_type = document.mime_type or "application/pdf"

        # 2. Send to Document AI
        logger.info("Sending document %s to Document AI", document_id)
        extraction = process_document(file_bytes, mime_type)

        # 3. Persist Account
        account_id = f"acc_{uuid4().hex[:8]}"
        acct_data = extraction.account
        account = Account(
            account_id=account_id,
            case_id=document.case_id,
            document_id=document_id,
            bank_name=acct_data.bank_name,
            account_holder_name=acct_data.account_holder_name,
            sort_code=acct_data.sort_code,
            account_number_masked=acct_data.account_number_masked,
            statement_start_date=acct_data.statement_start_date,
            statement_end_date=acct_data.statement_end_date,
            opening_balance=acct_data.opening_balance,
            closing_balance=acct_data.closing_balance,
        )
        db.add(account)
        db.flush()

        # 4. Persist Transactions + create exceptions for uncertain rows
        saved_count = 0
        exception_count = 0
        for idx, txn_data in enumerate(extraction.transactions):
            transaction_id = f"txn_{uuid4().hex[:8]}"
            txn = Transaction(
                transaction_id=transaction_id,
                document_id=document_id,
                account_id=account_id,
                transaction_date=txn_data.transaction_date,
                posting_date=txn_data.posting_date,
                description_raw=txn_data.description_raw,
                description_normalised=txn_data.description_normalised,
                direction=txn_data.direction,
                amount=txn_data.amount,
                balance=txn_data.balance,
                merchant_name=txn_data.merchant_name,
                counterparty_name=txn_data.counterparty_name,
                extractor_confidence=txn_data.extractor_confidence,
                source_page_number=txn_data.source_page_number,
                source_row_reference=str(idx + 1),
            )
            db.add(txn)
            db.flush()
            saved_count += 1

            if txn_data.extractor_confidence < CONFIDENCE_THRESHOLD:
                exception_id = f"exc_{uuid4().hex[:8]}"
                exc_record = CaseException(
                    exception_id=exception_id,
                    case_id=document.case_id,
                    document_id=document_id,
                    transaction_id=transaction_id,
                    job_id=job_id,
                    exception_type="low_confidence_extraction",
                    severity="Medium",
                    status="Open",
                    title="Low-confidence transaction extraction",
                    description=(
                        f"Transaction row {idx + 1} was extracted with confidence "
                        f"{float(txn_data.extractor_confidence):.2f}, below the threshold "
                        f"{float(CONFIDENCE_THRESHOLD):.2f}. Manual review recommended."
                    ),
                )
                db.add(exc_record)
                exception_count += 1

        # 5. Update document status
        document.status = "Extracted"
        db.commit()

        result = {
            "document_id": document_id,
            "account_id": account_id,
            "transactions_saved": saved_count,
            "exceptions_created": exception_count,
            "status": "Extracted",
            "message": "Document extraction completed",
        }
        _mark_job_completed(db, job_id, result)
        logger.info(
            "Extraction complete for %s: %d transactions, %d exceptions",
            document_id,
            saved_count,
            exception_count,
        )
        return result

    except Exception as exc:
        db.rollback()
        _mark_job_failed(db, job_id, "EXTRACTION_ERROR", str(exc))
        logger.exception("Extraction failed for document %s", document_id)
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="categorise_document_task")
def categorise_document_task(self, document_id: str, job_id: str):
    """Async task to categorise a document"""
    import time

    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()

        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        _mark_job_started(db, job_id)

        # Simulate categorisation work
        time.sleep(2)

        document.status = "Categorised"
        db.commit()

        result = {"document_id": document_id, "status": "Categorised", "message": "Document categorisation completed"}
        _mark_job_completed(db, job_id, result)
        return result
    except Exception as exc:
        _mark_job_failed(db, job_id, "CATEGORISATION_ERROR", str(exc))
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="generate_report_task")
def generate_report_task(self, case_id: str, report_type: str, job_id: str):
    """Async task to generate a report for a case"""
    import time

    db = SessionLocal()
    try:
        _mark_job_started(db, job_id)

        # Simulate report generation work
        time.sleep(3)

        result = {"case_id": case_id, "report_type": report_type, "status": "Generated", "message": "Report generation completed"}
        _mark_job_completed(db, job_id, result)
        return result
    except Exception as exc:
        _mark_job_failed(db, job_id, "REPORT_ERROR", str(exc))
        raise
    finally:
        db.close()
