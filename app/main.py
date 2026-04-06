from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import Base, engine, get_db
from app.models import Case, Document, ProcessingJob
from app.schemas import CaseCreate, DocumentRegister, ProcessingJobResponse, ReportRequest
from app.tasks import validate_document_task, extract_document_task, categorise_document_task, generate_report_task
from app.celery_app import celery_app

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

    return {
        "document_id": document.document_id,
        "case_id": document.case_id,
        "original_filename": document.original_filename,
        "source_type": document.source_type,
        "file_size": document.file_size,
        "mime_type": document.mime_type,
        "status": document.status
    }


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
