from typing import Optional
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
    storage_key: Optional[str] = None
    file_hash: Optional[str] = None
    status: str

    class Config:
        from_attributes = True


class ReportRequest(BaseModel):
    report_type: str
