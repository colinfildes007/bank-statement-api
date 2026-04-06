import logging
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import Base, engine, get_db
from app.models import Case, Document, ProcessingJob, CaseException
from app.schemas import CaseCreate, DocumentRegister, ProcessingJobResponse, ReportRequest, ExceptionResponse, ExceptionActionRequest
from app.storage import compute_sha256, delete_file_from_s3, upload_file_to_s3
from app.storage import MAX_UPLOAD_SIZE
from app.tasks import validate_document_task, extract_document_task, categorise_document_task, generate_report_task
from app.celery_app import celery_app

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bank Statement API",
    description="Starter API for Base44 bank statement processing orchestration",
    version="0.3.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _generate_job_id() -> str:
    return f"job_{uuid4().hex[:8]}"


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "bank-statement-api",
        "message": "FastAPI service is running"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }


@app.post("/cases", dependencies=[Depends(verify_api_key)])
def create_case(payload: CaseCreate, db: Session = Depends(get_db)):
    case_id = f"case_{uuid4().hex[:8]}"
    case_reference = f"BS-{uuid4().hex[:6].upper()}"

    case = Case(
        case_id=case_id,
        case_reference=case_reference,
        customer_name=payload.customer_name,
        organisation_name=payload.organisation_name,
        jurisdiction=payload.jurisdiction,
        case_type=payload.case_type,
        status="Draft"
    )

    db.add(case)
    db.commit()
    db.refresh(case)

    return {
        "case_id": case.case_id,
        "case_reference": case.case_reference,
        "customer_name": case.customer_name,
        "organisation_name": case.organisation_name,
        "jurisdiction": case.jurisdiction,
        "case_type": case.case_type,
        "status": case.status
    }


@app.get("/cases/{case_id}", dependencies=[Depends(verify_api_key)])
def get_case(case_id: str, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()

    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    return {
        "case_id": case.case_id,
        "case_reference": case.case_reference,
        "customer_name": case.customer_name,
        "organisation_name": case.organisation_name,
        "jurisdiction": case.jurisdiction,
        "case_type": case.case_type,
        "status": case.status
    }


def _document_response(document: Document) -> dict:
    return {
        "document_id": document.document_id,
        "case_id": document.case_id,
        "original_filename": document.original_filename,
        "source_type": document.source_type,
        "file_size": document.file_size,
        "mime_type": document.mime_type,
        "storage_key": document.storage_key,
        "file_hash": document.file_hash,
        "status": document.status,
    }


@app.post("/cases/{case_id}/documents/register", dependencies=[Depends(verify_api_key)])
def register_document(case_id: str, payload: DocumentRegister, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()

    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    document_id = f"doc_{uuid4().hex[:8]}"

    document = Document(
        document_id=document_id,
        case_id=case_id,
        original_filename=payload.original_filename,
        source_type=payload.source_type,
        file_size=payload.file_size,
        mime_type=payload.mime_type,
        status="Uploaded"
    )

    db.add(document)
    db.commit()
    db.refresh(document)

    return _document_response(document)


@app.post("/cases/{case_id}/documents/upload", dependencies=[Depends(verify_api_key)])
async def upload_document(
    case_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    case = db.query(Case).filter(Case.case_id == case_id).first()

    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    file_bytes = await file.read()

    if len(file_bytes) > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds the maximum allowed size of {MAX_UPLOAD_SIZE} bytes",
        )

    file_hash = compute_sha256(file_bytes)
    file_size = len(file_bytes)
    mime_type = file.content_type or "application/octet-stream"
    original_filename = file.filename or "unknown"

    document_id = f"doc_{uuid4().hex[:8]}"
    storage_key = f"{case_id}/{document_id}/{original_filename}"

    upload_file_to_s3(file_bytes, storage_key, mime_type)

    document = Document(
        document_id=document_id,
        case_id=case_id,
        original_filename=original_filename,
        source_type="upload",
        file_size=file_size,
        mime_type=mime_type,
        storage_key=storage_key,
        file_hash=file_hash,
        status="Uploaded",
    )

    db.add(document)
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("Failed to save document metadata for %s: %s", document_id, exc)
        delete_file_from_s3(storage_key)
        raise HTTPException(status_code=500, detail="Failed to save document metadata")

    db.refresh(document)

    return _document_response(document)


@app.get("/documents/{document_id}/status", dependencies=[Depends(verify_api_key)])
def get_document_status(document_id: str, db: Session = Depends(get_db)):
    document = db.query(Document).filter(Document.document_id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return {
        "document_id": document.document_id,
        "case_id": document.case_id,
        "status": document.status,
        "original_filename": document.original_filename,
        "source_type": document.source_type
    }


@app.post("/documents/{document_id}/validate", dependencies=[Depends(verify_api_key)])
def validate_document(document_id: str, db: Session = Depends(get_db)):
    document = db.query(Document).filter(Document.document_id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=document.case_id,
        document_id=document_id,
        job_type="validate",
        status="Pending",
    )
    db.add(job)
    db.commit()

    validate_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "validate",
        "status": "Pending",
        "message": "Validation job queued"
    }


@app.post("/documents/{document_id}/extract", dependencies=[Depends(verify_api_key)])
def extract_document(document_id: str, db: Session = Depends(get_db)):
    document = db.query(Document).filter(Document.document_id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=document.case_id,
        document_id=document_id,
        job_type="extract",
        status="Pending",
    )
    db.add(job)
    db.commit()

    extract_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "extract",
        "status": "Pending",
        "message": "Extraction job queued"
    }


@app.post("/documents/{document_id}/categorise", dependencies=[Depends(verify_api_key)])
def categorise_document(document_id: str, db: Session = Depends(get_db)):
    document = db.query(Document).filter(Document.document_id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=document.case_id,
        document_id=document_id,
        job_type="categorise",
        status="Pending",
    )
    db.add(job)
    db.commit()

    categorise_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "categorise",
        "status": "Pending",
        "message": "Categorisation job queued"
    }


@app.get("/tasks/{task_id}", dependencies=[Depends(verify_api_key)])
def get_task_status(task_id: str):
    """Get the status of a Celery task"""
    task = celery_app.AsyncResult(task_id)

    return {
        "task_id": task_id,
        "status": task.status,
        "result": task.result if task.status == "SUCCESS" else None
    }


@app.get("/jobs/{job_id}", dependencies=[Depends(verify_api_key)], response_model=ProcessingJobResponse)
def get_job(job_id: str, db: Session = Depends(get_db)):
    """Get the status and result of a processing job"""
    job = db.query(ProcessingJob).filter(ProcessingJob.job_id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return job


@app.post("/cases/{case_id}/reports/generate", dependencies=[Depends(verify_api_key)])
def generate_report(case_id: str, payload: ReportRequest, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()

    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=case_id,
        document_id=None,
        job_type="generate_report",
        status="Pending",
    )
    db.add(job)
    db.commit()

    generate_report_task.delay(case_id, payload.report_type, job_id)

    return {
        "job_id": job_id,
        "case_id": case_id,
        "job_type": "generate_report",
        "report_type": payload.report_type,
        "status": "Pending",
        "message": "Report generation job queued"
    }


@app.get("/cases/{case_id}/exceptions", dependencies=[Depends(verify_api_key)], response_model=list[ExceptionResponse])
def get_case_exceptions(case_id: str, db: Session = Depends(get_db)):
    """List all exceptions for a case"""
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    return db.query(CaseException).filter(CaseException.case_id == case_id).all()


@app.get("/documents/{document_id}/exceptions", dependencies=[Depends(verify_api_key)], response_model=list[ExceptionResponse])
def get_document_exceptions(document_id: str, db: Session = Depends(get_db)):
    """List all exceptions for a document"""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return db.query(CaseException).filter(CaseException.document_id == document_id).all()


@app.post("/exceptions/{exception_id}/resolve", dependencies=[Depends(verify_api_key)], response_model=ExceptionResponse)
def resolve_exception(exception_id: str, payload: ExceptionActionRequest, db: Session = Depends(get_db)):
    """Resolve an exception"""
    exc = db.query(CaseException).filter(CaseException.exception_id == exception_id).first()
    if not exc:
        raise HTTPException(status_code=404, detail="Exception not found")

    exc.status = "Resolved"
    exc.resolved_at = datetime.now(timezone.utc)
    if payload.resolution_notes is not None:
        exc.resolution_notes = payload.resolution_notes

    db.commit()
    db.refresh(exc)
    return exc


@app.post("/exceptions/{exception_id}/dismiss", dependencies=[Depends(verify_api_key)], response_model=ExceptionResponse)
def dismiss_exception(exception_id: str, payload: ExceptionActionRequest, db: Session = Depends(get_db)):
    """Dismiss an exception"""
    exc = db.query(CaseException).filter(CaseException.exception_id == exception_id).first()
    if not exc:
        raise HTTPException(status_code=404, detail="Exception not found")

    exc.status = "Dismissed"
    exc.resolved_at = datetime.now(timezone.utc)
    if payload.resolution_notes is not None:
        exc.resolution_notes = payload.resolution_notes

    db.commit()
    db.refresh(exc)
    return exc
