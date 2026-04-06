import json
import time
from datetime import datetime, timezone
from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Document, ProcessingJob


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
    """Async task to extract data from a document"""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()

        if not document:
            _mark_job_failed(db, job_id, "NOT_FOUND", "Document not found")
            raise ValueError(f"Document {document_id} not found")

        _mark_job_started(db, job_id)

        # Simulate extraction work
        time.sleep(3)

        document.status = "Extracted"
        db.commit()

        result = {"document_id": document_id, "status": "Extracted", "message": "Document extraction completed"}
        _mark_job_completed(db, job_id, result)
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
