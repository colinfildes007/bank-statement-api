from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class CaseCreate(BaseModel):
    customer_name: str
    organisation_name: Optional[str] = None
    jurisdiction: str = "UK"
    case_type: str = "bank_statement_review"


class CaseResponse(BaseModel):
    case_id: str
    case_reference: str
    customer_name: str
    organisation_name: Optional[str] = None
    jurisdiction: str
    case_type: str
    status: str

    class Config:
        from_attributes = True


class DocumentRegister(BaseModel):
    original_filename: str
    source_type: str
    file_size: Optional[int] = None
    mime_type: Optional[str] = None


class DocumentResponse(BaseModel):
    document_id: str
    case_id: str
    original_filename: str
    source_type: str
    file_size: Optional[int] = None
    mime_type: Optional[str] = None
    status: str

    class Config:
        from_attributes = True


class ReportRequest(BaseModel):
    report_type: str


class ProcessingJobResponse(BaseModel):
    job_id: str
    case_id: str
    document_id: Optional[str] = None
    job_type: str
    status: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    result_json: Optional[str] = None

    class Config:
        from_attributes = True
