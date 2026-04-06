import json
import time
from datetime import datetime, timezone
from uuid import uuid4

from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import CaseException, Document, ProcessingJob
from app.reconciliation import run_reconciliation


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


def _mark_job_completed_with_warnings(db, job_id: str, result: dict):
    job = db.query(ProcessingJob).filter(ProcessingJob.job_id == job_id).first()
    if job:
        job.status = "CompletedWithWarnings"
        job.completed_at = datetime.now(timezone.utc)
        job.result_json = json.dumps(result)
        db.commit()


def _persist_reconciliation_exceptions(db, reconciliation_result, case_id: str, document_id: str, job_id: str):
    """Create CaseException rows for every finding in *reconciliation_result*."""
    severity_map = {
        "Critical": "Critical",
        "High": "High",
        "Medium": "Medium",
        "Low": "Low",
    }
    for finding in reconciliation_result.findings:
        exception_id = f"exc_{uuid4().hex[:8]}"
        exc = CaseException(
            exception_id=exception_id,
            case_id=case_id,
            document_id=document_id,
            transaction_id=finding.transaction_id,
            job_id=job_id,
            exception_type=finding.check,
            severity=severity_map.get(finding.severity, "Medium"),
            status="Open",
            title=finding.title,
            description=finding.description,
        )
        db.add(exc)
    db.commit()


@celery_app.task(bind=True, name="validate_document_task")
def validate_document_task(self, document_id: str, job_id: str):
    """Async task to validate a document"""
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
    """Async task to extract data from a document and reconcile the result."""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()

        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        _mark_job_started(db, job_id)

        # Simulate extraction work
        time.sleep(3)

        # Placeholder extracted data — replace with real parser output when available
        extracted_data = {
            "document_id": document_id,
            "opening_balance": None,
            "closing_balance": None,
            "transactions": [],
        }

        # --- Reconciliation ---
        reconciliation_result = run_reconciliation(extracted_data)

        # Persist an exception record for every finding so nothing is silent
        _persist_reconciliation_exceptions(
            db,
            reconciliation_result,
            case_id=document.case_id,
            document_id=document_id,
            job_id=job_id,
        )

        outcome = reconciliation_result.outcome  # "passed" | "warning" | "failed"

        finding_summaries = [
            {
                "check": f.check,
                "severity": f.severity,
                "title": f.title,
                "transaction_id": f.transaction_id,
            }
            for f in reconciliation_result.findings
        ]

        result = {
            "document_id": document_id,
            "extracted_data": extracted_data,
            "reconciliation": {
                "outcome": outcome,
                "finding_count": len(reconciliation_result.findings),
                "findings": finding_summaries,
            },
        }

        if outcome == "failed":
            document.status = "ExtractionFailed"
            db.commit()
            _mark_job_failed(
                db,
                job_id,
                "RECONCILIATION_FAILED",
                f"Reconciliation failed with {len(reconciliation_result.findings)} finding(s).",
            )
            result["status"] = "ExtractionFailed"
            result["message"] = "Extraction reconciliation failed — see exceptions for details."
        elif outcome == "warning":
            document.status = "ExtractionWarning"
            db.commit()
            _mark_job_completed_with_warnings(db, job_id, result)
            result["status"] = "ExtractionWarning"
            result["message"] = "Extraction completed with reconciliation warnings — see exceptions for details."
        else:
            document.status = "Extracted"
            db.commit()
            _mark_job_completed(db, job_id, result)
            result["status"] = "Extracted"
            result["message"] = "Extraction and reconciliation completed successfully."

        return result
    except Exception as exc:
        _mark_job_failed(db, job_id, "EXTRACTION_ERROR", str(exc))
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="categorise_document_task")
def categorise_document_task(self, document_id: str, job_id: str):
    """Async task to categorise a document"""
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
