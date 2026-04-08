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


class AiReportResponse(BaseModel):
    report_id: str
    case_id: str
    report_type: str
    status: str
    requested_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    output_json: Optional[str] = None
    pdf_file_url: Optional[str] = None
    spreadsheet_file_url: Optional[str] = None

    class Config:
        from_attributes = True


class ProcessingJobResponse(BaseModel):
    job_id: str
    case_id: str
    document_id: Optional[str] = None
    job_type: str
    status: str
    requested_by: Optional[str] = None
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


# ── Transactions ──────────────────────────────────────────────────────────────

class TransactionResponse(BaseModel):
    transaction_id: str
    document_id: str
    case_id: Optional[str] = None
    account_id: Optional[str] = None
    transaction_date: Optional[date] = None
    posting_date: Optional[date] = None
    description_raw: Optional[str] = None
    description_normalised: Optional[str] = None
    raw_text: Optional[str] = None
    direction: Optional[str] = None
    amount: Optional[Decimal] = None
    credit: Optional[Decimal] = None
    debit: Optional[Decimal] = None
    balance: Optional[Decimal] = None
    merchant_name: Optional[str] = None
    counterparty_name: Optional[str] = None
    counterparty: Optional[str] = None
    reference: Optional[str] = None
    transaction_type: Optional[str] = None
    extractor_confidence: Optional[Decimal] = None
    source_page_number: Optional[int] = None
    source_row_reference: Optional[str] = None
    category: Optional[str] = None
    category_source: Optional[str] = None
    rule_id: Optional[str] = None
    needs_review: bool = False
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ── Categorisation rules ──────────────────────────────────────────────────────

class MerchantRuleCreate(BaseModel):
    merchant_name: str
    category: str
    match_type: str = "contains"
    case_sensitive: bool = False
    priority: int = 100
    enabled: bool = True


class MerchantRuleResponse(MerchantRuleCreate):
    rule_id: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class KeywordRuleCreate(BaseModel):
    keyword: str
    category: str
    match_type: str = "contains"
    case_sensitive: bool = False
    priority: int = 200
    enabled: bool = True


class KeywordRuleResponse(KeywordRuleCreate):
    rule_id: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class RegexRuleCreate(BaseModel):
    pattern: str
    category: str
    flags: Optional[str] = None
    priority: int = 300
    enabled: bool = True


class RegexRuleResponse(RegexRuleCreate):
    rule_id: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CounterpartyRuleCreate(BaseModel):
    counterparty: str
    category: str
    match_type: str = "contains"
    case_sensitive: bool = False
    priority: int = 150
    enabled: bool = True


class CounterpartyRuleResponse(CounterpartyRuleCreate):
    rule_id: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ── Risk flags ────────────────────────────────────────────────────────────────

class RiskFlagResponse(BaseModel):
    flag_id: str
    case_id: str
    document_id: str
    transaction_id: Optional[str] = None
    flag_type: str
    severity: str
    title: str
    detail: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ── Manual overrides ──────────────────────────────────────────────────────────

class ManualOverrideCreate(BaseModel):
    category: str
    notes: Optional[str] = None
    created_by: Optional[str] = None


class ManualOverrideResponse(ManualOverrideCreate):
    override_id: str
    transaction_id: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
