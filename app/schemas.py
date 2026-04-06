from typing import Optional
from datetime import date, datetime
from decimal import Decimal
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


class ExceptionResponse(BaseModel):
    exception_id: str
    case_id: str
    document_id: Optional[str] = None
    transaction_id: Optional[str] = None
    job_id: Optional[str] = None
    exception_type: str
    severity: str
    status: str
    title: str
    description: Optional[str] = None
    resolution_notes: Optional[str] = None
    created_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ExceptionActionRequest(BaseModel):
    resolution_notes: Optional[str] = None


class ValidationResultResponse(BaseModel):
    validation_result_id: str
    document_id: str
    job_id: str
    check_name: str
    severity: str
    passed: bool
    result_code: Optional[str] = None
    message: Optional[str] = None
    details_json: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class AccountResponse(BaseModel):
    account_id: str
    case_id: str
    document_id: str
    bank_name: Optional[str] = None
    account_holder_name: Optional[str] = None
    sort_code: Optional[str] = None
    account_number_masked: Optional[str] = None
    statement_start_date: Optional[date] = None
    statement_end_date: Optional[date] = None
    opening_balance: Optional[Decimal] = None
    closing_balance: Optional[Decimal] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class TransactionResponse(BaseModel):
    transaction_id: str
    document_id: str
    account_id: str
    transaction_date: Optional[date] = None
    posting_date: Optional[date] = None
    description_raw: Optional[str] = None
    description_normalised: Optional[str] = None
    direction: Optional[str] = None
    amount: Optional[Decimal] = None
    balance: Optional[Decimal] = None
    merchant_name: Optional[str] = None
    counterparty_name: Optional[str] = None
    extractor_confidence: Optional[Decimal] = None
    source_page_number: Optional[int] = None
    source_row_reference: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
