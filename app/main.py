import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import Base, SessionLocal, engine, get_db
from app.models import (
    Account, AiReport, Case, CaseException, CounterpartyRule, Document,
    KeywordRule, ManualOverride, MerchantAlias, MerchantRule, ProcessingJob,
    RegexRule, RiskFlag, Transaction, ValidationResult,
    CATEGORIES, CATEGORY_CODES,
)
from app.schemas import (
    AccountResponse, AiReportResponse, CaseCreate,
    CounterpartyRuleCreate, CounterpartyRuleResponse,
    DocumentRegister, ExceptionResponse, ExceptionActionRequest,
    KeywordRuleCreate, KeywordRuleResponse,
    ManualOverrideCreate, ManualOverrideResponse,
    MerchantAliasCreate, MerchantAliasResponse,
    MerchantRuleCreate, MerchantRuleResponse,
    ProcessingJobResponse, RegexRuleCreate, RegexRuleResponse,
    ReportRequest, RiskFlagResponse, SuggestRuleRequest, SuggestRuleResponse,
    TransactionResponse, ValidationResultResponse,
)
from app.storage import compute_sha256, delete_file_from_s3, upload_file_to_s3, is_r2_configured
from app.storage import MAX_UPLOAD_SIZE
from app.tasks import validate_document_task, extract_document_task, categorise_document_task, compute_risk_flags_task, generate_report_task
from app.celery_app import celery_app
from app.documentai import get_processor_info

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bank Statement API",
    description="Starter API for Base44 bank statement processing orchestration",
    version="0.4.0"
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
    if engine is not None:
        Base.metadata.create_all(bind=engine)
        # Apply any schema migrations for columns added after initial table creation
        with engine.connect() as conn:
            conn.execute(
                text("ALTER TABLE documents ADD COLUMN IF NOT EXISTS storage_key VARCHAR(500)")
            )
            conn.execute(
                text("ALTER TABLE documents ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64)")
            )
            conn.execute(
                text("ALTER TABLE documents ADD COLUMN IF NOT EXISTS bucket_name VARCHAR(255)")
            )
            conn.execute(
                text("ALTER TABLE documents ADD COLUMN IF NOT EXISTS upload_timestamp TIMESTAMPTZ")
            )
            conn.execute(
                text("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS requested_by VARCHAR(100)")
            )
            conn.execute(
                text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category_primary VARCHAR(100)")
            )
            conn.execute(
                text("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category_secondary VARCHAR(100)")
            )
            conn.commit()

        _seed_default_rules()


# Default categorisation keyword rules for common UK bank transaction prefixes.
# These are seeded once at startup only when the keyword_rules table is empty,
# so they will not overwrite rules created by users via the API.
_DEFAULT_KEYWORD_RULES = [
    # Income
    {"keyword": "BGC",     "category": "income",           "match_type": "startswith", "priority": 10},
    {"keyword": "BACS",    "category": "income",           "match_type": "startswith", "priority": 11},
    {"keyword": "SALARY",  "category": "income",           "match_type": "contains",   "priority": 12},
    {"keyword": "WAGES",   "category": "income",           "match_type": "contains",   "priority": 13},
    {"keyword": "PAYROLL", "category": "income",           "match_type": "contains",   "priority": 14},
    {"keyword": "CDT",     "category": "income",           "match_type": "startswith", "priority": 15},
    # Household bills
    {"keyword": "DD",      "category": "household_bills",  "match_type": "startswith", "priority": 20},
    {"keyword": "SO",      "category": "household_bills",  "match_type": "startswith", "priority": 21},
    {"keyword": "BP",      "category": "household_bills",  "match_type": "startswith", "priority": 22},
    # Everyday spending
    {"keyword": "VIS",     "category": "everyday_spending","match_type": "startswith", "priority": 30},
    {"keyword": "VISA",    "category": "everyday_spending","match_type": "startswith", "priority": 31},
    {"keyword": "POS",     "category": "everyday_spending","match_type": "startswith", "priority": 32},
    {"keyword": "DEB",     "category": "everyday_spending","match_type": "startswith", "priority": 33},
    # Transport
    {"keyword": "TFL",       "category": "transport",      "match_type": "contains",   "priority": 40},
    {"keyword": "TRAINLINE", "category": "transport",      "match_type": "contains",   "priority": 41},
    {"keyword": "UBER",      "category": "transport",      "match_type": "contains",   "priority": 42},
    # Financial / banking
    {"keyword": "ATM",      "category": "financial_banking","match_type": "startswith", "priority": 50},
    {"keyword": "CHQ",      "category": "financial_banking","match_type": "startswith", "priority": 51},
    {"keyword": "CHEQUE",   "category": "financial_banking","match_type": "startswith", "priority": 52},
    {"keyword": "TFR",      "category": "financial_banking","match_type": "startswith", "priority": 53},
    {"keyword": "TRANSFER", "category": "financial_banking","match_type": "startswith", "priority": 54},
    {"keyword": "FPS",      "category": "financial_banking","match_type": "startswith", "priority": 55},
    {"keyword": "INT",      "category": "financial_banking","match_type": "startswith", "priority": 56},
    {"keyword": "REF",      "category": "financial_banking","match_type": "startswith", "priority": 57},
    {"keyword": "REFUND",   "category": "financial_banking","match_type": "startswith", "priority": 58},
]


def _seed_default_rules():
    """Seed default UK bank keyword rules if the table is empty.

    Safe to call on every startup — it checks for existing rows first and
    does nothing when rules have already been created (either by a previous
    seed or by users via the API).
    """
    if SessionLocal is None:
        logger.warning("_seed_default_rules: SessionLocal is None — database not configured, skipping rule seeding")
        return
    db = SessionLocal()
    try:
        if db.query(KeywordRule).first():
            return  # Rules already exist — do not overwrite
        for r in _DEFAULT_KEYWORD_RULES:
            db.add(KeywordRule(
                rule_id=f"rule_{uuid4().hex[:8]}",
                keyword=r["keyword"],
                category=r["category"],
                match_type=r["match_type"],
                case_sensitive=False,
                priority=r["priority"],
                enabled=True,
            ))
        db.commit()
        logger.info("Seeded %d default keyword categorisation rules", len(_DEFAULT_KEYWORD_RULES))
    except Exception:
        db.rollback()
        logger.exception("Failed to seed default keyword rules")
    finally:
        db.close()


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
        "status": "healthy",
        "r2_configured": is_r2_configured(),
    }


@app.get("/health/docai")
def health_docai():
    """Check connectivity to the configured Google Document AI processor."""
    info = get_processor_info()
    status = "ok" if (info["configured"] and info["error"] is None) else "error"
    return {"status": status, **info}


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
        "bucket_name": document.bucket_name,
        "file_hash": document.file_hash,
        "upload_timestamp": document.upload_timestamp,
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
    bucket_name = os.getenv("R2_BUCKET_NAME")

    upload_file_to_s3(file_bytes, storage_key, mime_type)

    document = Document(
        document_id=document_id,
        case_id=case_id,
        original_filename=original_filename,
        source_type="upload",
        file_size=file_size,
        mime_type=mime_type,
        storage_key=storage_key,
        bucket_name=bucket_name,
        file_hash=file_hash,
        upload_timestamp=datetime.now(timezone.utc),
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


@app.get("/documents/{document_id}", dependencies=[Depends(verify_api_key)])
def get_document(document_id: str, db: Session = Depends(get_db)):
    """Get a document record, enriched with account-level metadata when available.

    Includes ``statement_start_date``, ``statement_end_date``, and
    ``account_holder_name`` sourced from the linked Account so that callers
    do not need a separate request to ``/documents/{id}/accounts``.
    """
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    account = db.query(Account).filter(Account.document_id == document_id).first()

    response = _document_response(document)
    response.update({
        "statement_start_date": account.statement_start_date if account else None,
        "statement_end_date": account.statement_end_date if account else None,
        "account_holder_name": account.account_holder_name if account else None,
        "account_id": account.account_id if account else None,
    })
    return response


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
        status="queued",
    )
    db.add(job)
    db.commit()

    validate_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "validate",
        "status": "queued",
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
        status="queued",
    )
    db.add(job)
    db.commit()

    extract_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "extract",
        "status": "queued",
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
        status="queued",
    )
    db.add(job)
    db.commit()

    categorise_document_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "categorise",
        "status": "queued",
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


@app.post("/cases/{case_id}/reports/generate", dependencies=[Depends(verify_api_key)], response_model=ProcessingJobResponse, status_code=202)
def generate_report(case_id: str, payload: ReportRequest, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()

    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    if payload.report_type not in ("affordability",):
        raise HTTPException(status_code=422, detail=f"Unsupported report_type '{payload.report_type}'. Supported: affordability")

    report_id = f"rpt_{uuid4().hex[:12]}"
    ai_report = AiReport(
        report_id=report_id,
        case_id=case_id,
        report_type=payload.report_type,
        status="Pending",
    )
    db.add(ai_report)

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=case_id,
        document_id=None,
        job_type="generate_report",
        status="queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    generate_report_task.delay(case_id, payload.report_type, job_id, report_id)

    return job


@app.get("/cases/{case_id}/reports", dependencies=[Depends(verify_api_key)], response_model=list[AiReportResponse])
def list_case_reports(case_id: str, db: Session = Depends(get_db)):
    """List all AI reports generated for a case."""
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")
    return db.query(AiReport).filter(AiReport.case_id == case_id).order_by(AiReport.requested_at.desc()).all()


@app.get("/reports/{report_id}", dependencies=[Depends(verify_api_key)], response_model=AiReportResponse)
def get_report(report_id: str, db: Session = Depends(get_db)):
    """Get details of a specific AI report."""
    report = db.query(AiReport).filter(AiReport.report_id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report


@app.get("/cases/{case_id}/exceptions", dependencies=[Depends(verify_api_key)], response_model=list[ExceptionResponse])
def get_case_exceptions(case_id: str, db: Session = Depends(get_db)):
    """List all exceptions for a case"""
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    return db.query(CaseException).filter(CaseException.case_id == case_id).all()


@app.get("/documents/{document_id}/validation-results", dependencies=[Depends(verify_api_key)], response_model=list[ValidationResultResponse])
def get_document_validation_results(document_id: str, db: Session = Depends(get_db)):
    """List all validation check results for a document"""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return db.query(ValidationResult).filter(ValidationResult.document_id == document_id).all()


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

    exc.status = "resolved"
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

    exc.status = "dismissed"
    exc.resolved_at = datetime.now(timezone.utc)
    if payload.resolution_notes is not None:
        exc.resolution_notes = payload.resolution_notes

    db.commit()
    db.refresh(exc)
    return exc


@app.get("/documents/{document_id}/accounts", dependencies=[Depends(verify_api_key)], response_model=list[AccountResponse])
def get_document_accounts(document_id: str, db: Session = Depends(get_db)):
    """List extracted accounts for a document"""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return db.query(Account).filter(Account.document_id == document_id).all()


@app.get("/accounts/{account_id}", dependencies=[Depends(verify_api_key)], response_model=AccountResponse)
def get_account(account_id: str, db: Session = Depends(get_db)):
    """Get a single extracted account by ID"""
    account = db.query(Account).filter(Account.account_id == account_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return account


@app.get("/accounts/{account_id}/transactions", dependencies=[Depends(verify_api_key)], response_model=list[TransactionResponse])
def get_account_transactions(account_id: str, db: Session = Depends(get_db)):
    """List all transactions for an account"""
    account = db.query(Account).filter(Account.account_id == account_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return db.query(Transaction).filter(Transaction.account_id == account_id).all()


@app.get("/transactions/{transaction_id}", dependencies=[Depends(verify_api_key)], response_model=TransactionResponse)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """Get a single transaction by ID"""
    txn = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return txn

# ── Category taxonomy ─────────────────────────────────────────────────────────

@app.get("/categories", dependencies=[Depends(verify_api_key)])
def list_categories():
    """Return the full category taxonomy."""
    return [{"code": code, "name": name} for code, name in CATEGORIES.items()]


# ── Merchant rules ────────────────────────────────────────────────────────────

@app.get("/rules/merchants", dependencies=[Depends(verify_api_key)], response_model=list[MerchantRuleResponse])
def list_merchant_rules(db: Session = Depends(get_db)):
    """List all merchant categorisation rules."""
    return db.query(MerchantRule).order_by(MerchantRule.priority.asc()).all()


@app.post("/rules/merchants", dependencies=[Depends(verify_api_key)], response_model=MerchantRuleResponse, status_code=201)
def create_merchant_rule(payload: MerchantRuleCreate, db: Session = Depends(get_db)):
    """Create a merchant categorisation rule."""
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(status_code=422, detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}")
    if payload.match_type not in ("exact", "contains", "startswith"):
        raise HTTPException(status_code=422, detail="match_type must be one of: exact, contains, startswith")

    rule = MerchantRule(
        rule_id=f"mr_{uuid4().hex[:8]}",
        merchant_name=payload.merchant_name,
        category=payload.category,
        match_type=payload.match_type,
        case_sensitive=payload.case_sensitive,
        priority=payload.priority,
        enabled=payload.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@app.delete("/rules/merchants/{rule_id}", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_merchant_rule(rule_id: str, db: Session = Depends(get_db)):
    """Delete a merchant categorisation rule."""
    rule = db.query(MerchantRule).filter(MerchantRule.rule_id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Merchant rule not found")
    db.delete(rule)
    db.commit()


# ── Keyword rules ─────────────────────────────────────────────────────────────

@app.get("/rules/keywords", dependencies=[Depends(verify_api_key)], response_model=list[KeywordRuleResponse])
def list_keyword_rules(db: Session = Depends(get_db)):
    """List all keyword categorisation rules."""
    return db.query(KeywordRule).order_by(KeywordRule.priority.asc()).all()


@app.post("/rules/keywords", dependencies=[Depends(verify_api_key)], response_model=KeywordRuleResponse, status_code=201)
def create_keyword_rule(payload: KeywordRuleCreate, db: Session = Depends(get_db)):
    """Create a keyword categorisation rule."""
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(status_code=422, detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}")
    if payload.match_type not in ("exact", "contains", "startswith"):
        raise HTTPException(status_code=422, detail="match_type must be one of: exact, contains, startswith")

    rule = KeywordRule(
        rule_id=f"kr_{uuid4().hex[:8]}",
        keyword=payload.keyword,
        category=payload.category,
        match_type=payload.match_type,
        case_sensitive=payload.case_sensitive,
        priority=payload.priority,
        enabled=payload.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@app.delete("/rules/keywords/{rule_id}", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_keyword_rule(rule_id: str, db: Session = Depends(get_db)):
    """Delete a keyword categorisation rule."""
    rule = db.query(KeywordRule).filter(KeywordRule.rule_id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Keyword rule not found")
    db.delete(rule)
    db.commit()


# ── Regex rules ───────────────────────────────────────────────────────────────

@app.get("/rules/regex", dependencies=[Depends(verify_api_key)], response_model=list[RegexRuleResponse])
def list_regex_rules(db: Session = Depends(get_db)):
    """List all regex categorisation rules."""
    return db.query(RegexRule).order_by(RegexRule.priority.asc()).all()


@app.post("/rules/regex", dependencies=[Depends(verify_api_key)], response_model=RegexRuleResponse, status_code=201)
def create_regex_rule(payload: RegexRuleCreate, db: Session = Depends(get_db)):
    """Create a regex categorisation rule."""
    import re as _re
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(status_code=422, detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}")
    try:
        _re.compile(payload.pattern)
    except _re.error as e:
        raise HTTPException(status_code=422, detail=f"Invalid regex pattern: {e}")

    rule = RegexRule(
        rule_id=f"rr_{uuid4().hex[:8]}",
        pattern=payload.pattern,
        category=payload.category,
        flags=payload.flags,
        priority=payload.priority,
        enabled=payload.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@app.delete("/rules/regex/{rule_id}", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_regex_rule(rule_id: str, db: Session = Depends(get_db)):
    """Delete a regex categorisation rule."""
    rule = db.query(RegexRule).filter(RegexRule.rule_id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Regex rule not found")
    db.delete(rule)
    db.commit()


# ── Counterparty rules ────────────────────────────────────────────────────────

@app.get("/rules/counterparties", dependencies=[Depends(verify_api_key)], response_model=list[CounterpartyRuleResponse])
def list_counterparty_rules(db: Session = Depends(get_db)):
    """List all counterparty categorisation rules."""
    return db.query(CounterpartyRule).order_by(CounterpartyRule.priority.asc()).all()


@app.post("/rules/counterparties", dependencies=[Depends(verify_api_key)], response_model=CounterpartyRuleResponse, status_code=201)
def create_counterparty_rule(payload: CounterpartyRuleCreate, db: Session = Depends(get_db)):
    """Create a counterparty categorisation rule."""
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(status_code=422, detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}")
    if payload.match_type not in ("exact", "contains", "startswith"):
        raise HTTPException(status_code=422, detail="match_type must be one of: exact, contains, startswith")

    rule = CounterpartyRule(
        rule_id=f"cr_{uuid4().hex[:8]}",
        counterparty=payload.counterparty,
        category=payload.category,
        match_type=payload.match_type,
        case_sensitive=payload.case_sensitive,
        priority=payload.priority,
        enabled=payload.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@app.delete("/rules/counterparties/{rule_id}", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_counterparty_rule(rule_id: str, db: Session = Depends(get_db)):
    """Delete a counterparty categorisation rule."""
    rule = db.query(CounterpartyRule).filter(CounterpartyRule.rule_id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Counterparty rule not found")
    db.delete(rule)
    db.commit()


# ── Transactions ──────────────────────────────────────────────────────────────

@app.get("/documents/{document_id}/transactions", dependencies=[Depends(verify_api_key)], response_model=list[TransactionResponse])
def list_document_transactions(document_id: str, db: Session = Depends(get_db)):
    """List all transactions extracted from a document."""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    return db.query(Transaction).filter(Transaction.document_id == document_id).all()


# ── Manual overrides ──────────────────────────────────────────────────────────

@app.post("/transactions/{transaction_id}/override", dependencies=[Depends(verify_api_key)], response_model=ManualOverrideResponse, status_code=201)
def set_manual_override(transaction_id: str, payload: ManualOverrideCreate, db: Session = Depends(get_db)):
    """Set or replace the manual category override for a transaction."""
    txn = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(status_code=422, detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}")

    existing = db.query(ManualOverride).filter(ManualOverride.transaction_id == transaction_id).first()
    if existing:
        existing.category = payload.category
        existing.notes = payload.notes
        existing.created_by = payload.created_by
        db.commit()
        db.refresh(existing)
        override = existing
    else:
        override = ManualOverride(
            override_id=f"mo_{uuid4().hex[:8]}",
            transaction_id=transaction_id,
            category=payload.category,
            notes=payload.notes,
            created_by=payload.created_by,
        )
        db.add(override)
        db.commit()
        db.refresh(override)

    txn.category = payload.category
    txn.category_source = "manual"
    txn.rule_id = override.override_id
    txn.needs_review = False
    db.commit()
    return override


@app.delete("/transactions/{transaction_id}/override", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_manual_override(transaction_id: str, db: Session = Depends(get_db)):
    """Remove the manual category override for a transaction."""
    override = db.query(ManualOverride).filter(ManualOverride.transaction_id == transaction_id).first()
    if not override:
        raise HTTPException(status_code=404, detail="No manual override found for this transaction")
    db.delete(override)
    db.commit()


# ── Rule suggestion ───────────────────────────────────────────────────────────

RULE_TYPE_DEFAULTS = {
    "merchant": 100,
    "keyword": 200,
    "regex": 300,
    "counterparty": 150,
}


@app.post("/transactions/{transaction_id}/suggest-rule", dependencies=[Depends(verify_api_key)], response_model=SuggestRuleResponse, status_code=201)
def suggest_rule(transaction_id: str, payload: SuggestRuleRequest, db: Session = Depends(get_db)):
    """Create a categorisation rule suggested from reviewing a specific transaction.

    rule_type must be one of: merchant, keyword, regex, counterparty.
    This allows analysts to codify a reusable rule directly from a manual review
    without a separate admin workflow.
    """
    txn = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    if payload.rule_type not in RULE_TYPE_DEFAULTS:
        raise HTTPException(
            status_code=422,
            detail=f"rule_type must be one of: {', '.join(sorted(RULE_TYPE_DEFAULTS))}",
        )
    if payload.category not in CATEGORY_CODES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid category '{payload.category}'. Valid codes: {sorted(CATEGORY_CODES)}",
        )

    priority = payload.priority if payload.priority is not None else RULE_TYPE_DEFAULTS[payload.rule_type]

    if payload.rule_type == "merchant":
        rule = MerchantRule(
            rule_id=f"mr_{uuid4().hex[:8]}",
            merchant_name=payload.pattern,
            category=payload.category,
            match_type="contains",
            case_sensitive=False,
            priority=priority,
            enabled=True,
        )
        db.add(rule)
        db.commit()
        db.refresh(rule)
        return SuggestRuleResponse(
            rule_type="merchant", rule_id=rule.rule_id, pattern=rule.merchant_name,
            category=rule.category, priority=rule.priority, created_at=rule.created_at,
        )

    if payload.rule_type == "keyword":
        rule = KeywordRule(
            rule_id=f"kr_{uuid4().hex[:8]}",
            keyword=payload.pattern,
            category=payload.category,
            match_type="contains",
            case_sensitive=False,
            priority=priority,
            enabled=True,
        )
        db.add(rule)
        db.commit()
        db.refresh(rule)
        return SuggestRuleResponse(
            rule_type="keyword", rule_id=rule.rule_id, pattern=rule.keyword,
            category=rule.category, priority=rule.priority, created_at=rule.created_at,
        )

    if payload.rule_type == "counterparty":
        rule = CounterpartyRule(
            rule_id=f"cr_{uuid4().hex[:8]}",
            counterparty=payload.pattern,
            category=payload.category,
            match_type="contains",
            case_sensitive=False,
            priority=priority,
            enabled=True,
        )
        db.add(rule)
        db.commit()
        db.refresh(rule)
        return SuggestRuleResponse(
            rule_type="counterparty", rule_id=rule.rule_id, pattern=rule.counterparty,
            category=rule.category, priority=rule.priority, created_at=rule.created_at,
        )

    # payload.rule_type == "regex"
    import re as _re
    try:
        _re.compile(payload.pattern)
    except _re.error as e:
        raise HTTPException(status_code=422, detail=f"Invalid regex pattern: {e}")
    rule = RegexRule(
        rule_id=f"rr_{uuid4().hex[:8]}",
        pattern=payload.pattern,
        category=payload.category,
        priority=priority,
        enabled=True,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return SuggestRuleResponse(
        rule_type="regex", rule_id=rule.rule_id, pattern=rule.pattern,
        category=rule.category, priority=rule.priority, created_at=rule.created_at,
    )


# ── Merchant aliases ──────────────────────────────────────────────────────────

@app.get("/rules/merchant-aliases", dependencies=[Depends(verify_api_key)], response_model=list[MerchantAliasResponse])
def list_merchant_aliases(db: Session = Depends(get_db)):
    """List all merchant alias mappings."""
    return db.query(MerchantAlias).order_by(MerchantAlias.alias_name.asc()).all()


@app.post("/rules/merchant-aliases", dependencies=[Depends(verify_api_key)], response_model=MerchantAliasResponse, status_code=201)
def create_merchant_alias(payload: MerchantAliasCreate, db: Session = Depends(get_db)):
    """Create a merchant alias that maps a raw description string to a canonical merchant name."""
    alias = MerchantAlias(
        alias_id=f"ma_{uuid4().hex[:8]}",
        alias_name=payload.alias_name,
        canonical_name=payload.canonical_name,
        case_sensitive=payload.case_sensitive,
    )
    db.add(alias)
    db.commit()
    db.refresh(alias)
    return alias


@app.delete("/rules/merchant-aliases/{alias_id}", dependencies=[Depends(verify_api_key)], status_code=204)
def delete_merchant_alias(alias_id: str, db: Session = Depends(get_db)):
    """Delete a merchant alias mapping."""
    alias = db.query(MerchantAlias).filter(MerchantAlias.alias_id == alias_id).first()
    if not alias:
        raise HTTPException(status_code=404, detail="Merchant alias not found")
    db.delete(alias)
    db.commit()


# ── Risk flags ────────────────────────────────────────────────────────────────

@app.post("/documents/{document_id}/risk-flags", dependencies=[Depends(verify_api_key)])
def trigger_risk_flags(document_id: str, db: Session = Depends(get_db)):
    """Queue a job to compute risk flags for all transactions in a document."""
    document = db.query(Document).filter(Document.document_id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    job_id = _generate_job_id()
    job = ProcessingJob(
        job_id=job_id,
        case_id=document.case_id,
        document_id=document_id,
        job_type="compute_risk_flags",
        status="queued",
    )
    db.add(job)
    db.commit()

    compute_risk_flags_task.delay(document_id, job_id)

    return {
        "job_id": job_id,
        "document_id": document_id,
        "job_type": "compute_risk_flags",
        "status": "queued",
        "message": "Risk flag computation job queued",
    }


@app.get("/documents/{document_id}/risk-flags", dependencies=[Depends(verify_api_key)], response_model=list[RiskFlagResponse])
def get_document_risk_flags(document_id: str, db: Session = Depends(get_db)):
    """List all risk flags computed for a document."""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return db.query(RiskFlag).filter(RiskFlag.document_id == document_id).all()


@app.get("/cases/{case_id}/risk-flags", dependencies=[Depends(verify_api_key)], response_model=list[RiskFlagResponse])
def get_case_risk_flags(case_id: str, db: Session = Depends(get_db)):
    """List all risk flags across all documents for a case."""
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    return db.query(RiskFlag).filter(RiskFlag.case_id == case_id).all()
