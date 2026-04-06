import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Account, CaseException, Document, ProcessingJob, Transaction

logger = logging.getLogger(__name__)


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


@celery_app.task(bind=True, name="validate_document_task")
def validate_document_task(self, document_id: str, job_id: str):
    """Async task to validate a document"""
    import time

    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()

        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        _mark_job_started(db, job_id)

        # Simulate validation work
        time.sleep(2)

        document.status = "Validated"
        db.commit()

        result = {"document_id": document_id, "status": "Validated", "message": "Document validation completed"}
        _mark_job_completed(db, job_id, result)
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
