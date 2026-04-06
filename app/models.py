from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.sql import func
from app.database import Base


class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, index=True)
    case_id = Column(String(100), unique=True, index=True, nullable=False)
    case_reference = Column(String(100), unique=True, index=True, nullable=False)
    customer_name = Column(String(255), nullable=False)
    organisation_name = Column(String(255), nullable=True)
    jurisdiction = Column(String(50), nullable=False, default="UK")
    case_type = Column(String(100), nullable=False, default="bank_statement_review")
    status = Column(String(50), nullable=False, default="Draft")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(String(100), unique=True, index=True, nullable=False)
    case_id = Column(String(100), ForeignKey("cases.case_id"), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    source_type = Column(String(50), nullable=False)
    file_size = Column(Integer, nullable=True)
    mime_type = Column(String(255), nullable=True)
    storage_key = Column(String(500), nullable=True)
    file_hash = Column(String(64), nullable=True)
    status = Column(String(50), nullable=False, default="Uploaded")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(100), unique=True, index=True, nullable=False)
    case_id = Column(String(100), ForeignKey("cases.case_id"), nullable=False, index=True)
    document_id = Column(String(100), ForeignKey("documents.document_id"), nullable=True, index=True)
    job_type = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default="Pending")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_code = Column(String(50), nullable=True)
    error_message = Column(Text, nullable=True)
    result_json = Column(Text, nullable=True)


class CaseException(Base):
    __tablename__ = "exceptions"

    id = Column(Integer, primary_key=True, index=True)
    exception_id = Column(String(100), unique=True, index=True, nullable=False)
    case_id = Column(String(100), ForeignKey("cases.case_id"), nullable=False, index=True)
    document_id = Column(String(100), ForeignKey("documents.document_id"), nullable=True, index=True)
    transaction_id = Column(String(100), nullable=True, index=True)
    job_id = Column(String(100), ForeignKey("processing_jobs.job_id"), nullable=True, index=True)
    exception_type = Column(String(100), nullable=False)
    severity = Column(String(50), nullable=False, default="Medium")
    status = Column(String(50), nullable=False, default="Open")
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    resolution_notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    resolved_at = Column(DateTime(timezone=True), nullable=True)


class ValidationResult(Base):
    __tablename__ = "validation_results"

    id = Column(Integer, primary_key=True, index=True)
    validation_result_id = Column(String(100), unique=True, index=True, nullable=False)
    document_id = Column(String(100), ForeignKey("documents.document_id"), nullable=False, index=True)
    job_id = Column(String(100), ForeignKey("processing_jobs.job_id"), nullable=False, index=True)
    check_name = Column(String(100), nullable=False)
    severity = Column(String(50), nullable=False, default="Medium")
    passed = Column(Boolean, nullable=False)
    result_code = Column(String(100), nullable=True)
    message = Column(Text, nullable=True)
    details_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(String(100), unique=True, index=True, nullable=False)
    case_id = Column(String(100), ForeignKey("cases.case_id"), nullable=False, index=True)
    document_id = Column(String(100), ForeignKey("documents.document_id"), nullable=False, index=True)
    bank_name = Column(String(255), nullable=True)
    account_holder_name = Column(String(255), nullable=True)
    sort_code = Column(String(20), nullable=True)
    account_number_masked = Column(String(50), nullable=True)
    statement_start_date = Column(Date, nullable=True)
    statement_end_date = Column(Date, nullable=True)
    opening_balance = Column(Numeric(precision=18, scale=2), nullable=True)
    closing_balance = Column(Numeric(precision=18, scale=2), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String(100), unique=True, index=True, nullable=False)
    document_id = Column(String(100), ForeignKey("documents.document_id"), nullable=False, index=True)
    account_id = Column(String(100), ForeignKey("accounts.account_id"), nullable=False, index=True)
    transaction_date = Column(Date, nullable=True)
    posting_date = Column(Date, nullable=True)
    description_raw = Column(Text, nullable=True)
    description_normalised = Column(Text, nullable=True)
    direction = Column(String(10), nullable=True)
    amount = Column(Numeric(precision=18, scale=2), nullable=True)
    balance = Column(Numeric(precision=18, scale=2), nullable=True)
    merchant_name = Column(String(255), nullable=True)
    counterparty_name = Column(String(255), nullable=True)
    extractor_confidence = Column(Numeric(precision=5, scale=4), nullable=True)
    source_page_number = Column(Integer, nullable=True)
    source_row_reference = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
