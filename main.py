from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from uuid import uuid4

app = FastAPI(
    title="Bank Statement API",
    description="Starter API for Base44 bank statement processing orchestration",
    version="0.1.0"
)

# Temporary CORS settings for testing with Base44
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


class CaseCreate(BaseModel):
    customer_name: str
    organisation_name: Optional[str] = None
    jurisdiction: str = "UK"
    case_type: str = "bank_statement_review"


@app.post("/cases")
def create_case(payload: CaseCreate):
    case_id = f"case_{uuid4().hex[:8]}"
    case_reference = f"BS-{uuid4().hex[:6].upper()}"

    return {
        "case_id": case_id,
        "case_reference": case_reference,
        "customer_name": payload.customer_name,
        "organisation_name": payload.organisation_name,
        "jurisdiction": payload.jurisdiction,
        "case_type": payload.case_type,
        "status": "Draft"
    }


@app.get("/cases/{case_id}")
def get_case(case_id: str):
    return {
        "case_id": case_id,
        "case_reference": "BS-DEMO01",
        "customer_name": "Demo Customer",
        "organisation_name": "Demo Organisation",
        "jurisdiction": "UK",
        "case_type": "bank_statement_review",
        "status": "Draft"
    }


class DocumentRegister(BaseModel):
    original_filename: str
    source_type: str
    file_size: Optional[int] = None
    mime_type: Optional[str] = None


@app.post("/cases/{case_id}/documents/register")
def register_document(case_id: str, payload: DocumentRegister):
    document_id = f"doc_{uuid4().hex[:8]}"

    return {
        "document_id": document_id,
        "case_id": case_id,
        "original_filename": payload.original_filename,
        "source_type": payload.source_type,
        "file_size": payload.file_size,
        "mime_type": payload.mime_type,
        "status": "Uploaded"
    }


@app.post("/documents/{document_id}/validate")
def validate_document(document_id: str):
    return {
        "document_id": document_id,
        "status": "Validating",
        "message": "Validation job accepted"
    }


@app.post("/documents/{document_id}/extract")
def extract_document(document_id: str):
    return {
        "document_id": document_id,
        "status": "Extracting",
        "message": "Extraction job accepted"
    }


@app.post("/documents/{document_id}/categorise")
def categorise_document(document_id: str):
    return {
        "document_id": document_id,
        "status": "Categorising",
        "message": "Categorisation job accepted"
    }


class ReportRequest(BaseModel):
    report_type: str


@app.post("/cases/{case_id}/reports/generate")
def generate_report(case_id: str, payload: ReportRequest):
    report_id = f"rep_{uuid4().hex[:8]}"

    return {
        "report_id": report_id,
        "case_id": case_id,
        "report_type": payload.report_type,
        "status": "Requested"
    }


@app.get("/documents/{document_id}/status")
def get_document_status(document_id: str):
    return {
        "document_id": document_id,
        "status": "Uploaded"
    }


@app.get("/reports/{report_id}")
def get_report(report_id: str):
    return {
        "report_id": report_id,
        "status": "Requested"
    }
