import hashlib
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import Base, SessionLocal, engine, get_db
from app.models import (
    Account, AiReport, Case, CaseException, CounterpartyRule, Document,
    ExtractionAudit, KeywordRule, ManualOverride, MerchantAlias, MerchantRule, ProcessingJob,
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


# Ordered severity levels used to filter exceptions by minimum severity.
_SEVERITY_RANK: dict[str, int] = {"Low": 0, "Medium": 1, "High": 2, "Critical": 3}


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
            conn.execute(
                text("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS money_in NUMERIC(18,2)")
            )
            conn.execute(
                text("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS money_out NUMERIC(18,2)")
            )
            conn.commit()

        # Widen any varchar(255) transaction columns that can hold long values to TEXT.
        # This is required when the database was created before these columns were changed
        # to Text in the ORM model — create_all never alters existing columns.
        try:
            from sqlalchemy.exc import DatabaseError
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE transactions "
                    "ALTER COLUMN description_raw TYPE TEXT, "
                    "ALTER COLUMN description_normalised TYPE TEXT, "
                    "ALTER COLUMN counterparty_name TYPE TEXT, "
                    "ALTER COLUMN counterparty TYPE TEXT, "
                    "ALTER COLUMN merchant_name TYPE TEXT"
                ))
                conn.commit()
        except DatabaseError as _migrate_err:
            logger.warning(
                "Text-column migration on transactions table failed (may already be TEXT): %s",
                _migrate_err,
            )

        _seed_default_rules()


def _stable_rule_id(prefix: str, *parts: str) -> str:
    """Generate a deterministic rule ID for a default seeded rule.

    Uses the first 8 hex characters of a SHA-256 hash over the joined parts so
    that the same rule always gets the same ID.  This enables idempotent
    per-rule seeding: new default rules added in code updates are added to
    existing deployments without overwriting rules created by users (which have
    random IDs like ``kr_abc12345``).
    """
    content = "|".join(parts)
    return f"{prefix}_{hashlib.sha256(content.encode()).hexdigest()[:8]}"


# ---------------------------------------------------------------------------
# Default categorisation rules seeded at startup.
# ---------------------------------------------------------------------------
# Each entry is a dict consumed by _seed_default_rules().
# Priority values within each rule type determine which rule fires first when
# multiple rules match (lower number = higher priority).
# ---------------------------------------------------------------------------

# Income keyword rules
_DEFAULT_KEYWORD_RULES = [
    # Bank payment type prefixes
    {"keyword": "BGC",              "category": "income",            "match_type": "startswith", "priority": 10},
    {"keyword": "BACS",             "category": "income",            "match_type": "startswith", "priority": 11},
    {"keyword": "CDT",              "category": "income",            "match_type": "startswith", "priority": 12},
    # Employment income
    {"keyword": "SALARY",           "category": "income",            "match_type": "contains",   "priority": 15},
    {"keyword": "WAGES",            "category": "income",            "match_type": "contains",   "priority": 16},
    {"keyword": "PAYROLL",          "category": "income",            "match_type": "contains",   "priority": 17},
    # Benefits & pensions
    {"keyword": "PENSION",          "category": "income",            "match_type": "contains",   "priority": 20},
    {"keyword": "UNIVERSAL CREDIT", "category": "income",            "match_type": "contains",   "priority": 21},
    {"keyword": "CHILD BENEFIT",    "category": "income",            "match_type": "contains",   "priority": 22},
    {"keyword": "TAX CREDIT",       "category": "income",            "match_type": "contains",   "priority": 23},
    {"keyword": "WORKING TAX",      "category": "income",            "match_type": "contains",   "priority": 24},
    {"keyword": "HMRC PAYE",        "category": "income",            "match_type": "contains",   "priority": 25},
    {"keyword": "DIVIDEND",         "category": "income",            "match_type": "contains",   "priority": 26},
    {"keyword": "INTEREST RECEIVED","category": "income",            "match_type": "contains",   "priority": 27},
    # Household bills — payment type prefixes
    {"keyword": "DD",               "category": "household_bills",   "match_type": "startswith", "priority": 30},
    {"keyword": "SO",               "category": "household_bills",   "match_type": "startswith", "priority": 31},
    {"keyword": "BP",               "category": "household_bills",   "match_type": "startswith", "priority": 32},
    # Rent / mortgage
    {"keyword": "MORTGAGE",         "category": "household_bills",   "match_type": "contains",   "priority": 35},
    {"keyword": "RENT",             "category": "household_bills",   "match_type": "contains",   "priority": 36},
    {"keyword": "COUNCIL TAX",      "category": "household_bills",   "match_type": "contains",   "priority": 37},
    # Utilities
    {"keyword": "WATER RATES",      "category": "household_bills",   "match_type": "contains",   "priority": 40},
    {"keyword": "GAS AND ELECTRIC", "category": "household_bills",   "match_type": "contains",   "priority": 41},
    {"keyword": "ENERGY",           "category": "household_bills",   "match_type": "contains",   "priority": 42},
    {"keyword": "ELECTRIC",         "category": "household_bills",   "match_type": "contains",   "priority": 43},
    {"keyword": "BRITISH GAS",      "category": "household_bills",   "match_type": "contains",   "priority": 44},
    {"keyword": "SCOTTISH POWER",   "category": "household_bills",   "match_type": "contains",   "priority": 45},
    {"keyword": "EON",              "category": "household_bills",   "match_type": "contains",   "priority": 46},
    {"keyword": "NPOWER",           "category": "household_bills",   "match_type": "contains",   "priority": 47},
    {"keyword": "THAMES WATER",     "category": "household_bills",   "match_type": "contains",   "priority": 48},
    {"keyword": "ANGLIAN WATER",    "category": "household_bills",   "match_type": "contains",   "priority": 49},
    {"keyword": "SEVERN TRENT",     "category": "household_bills",   "match_type": "contains",   "priority": 50},
    # Telecoms & media
    {"keyword": "BROADBAND",        "category": "household_bills",   "match_type": "contains",   "priority": 53},
    {"keyword": "TALKTALK",         "category": "household_bills",   "match_type": "contains",   "priority": 54},
    {"keyword": "TV LICENCE",       "category": "household_bills",   "match_type": "contains",   "priority": 55},
    {"keyword": "TV LICENSE",       "category": "household_bills",   "match_type": "contains",   "priority": 56},
    # Insurance
    {"keyword": "INSURANCE",        "category": "household_bills",   "match_type": "contains",   "priority": 58},
    # Everyday spending — card payment prefixes
    {"keyword": "VIS",              "category": "everyday_spending", "match_type": "startswith", "priority": 60},
    {"keyword": "VISA",             "category": "everyday_spending", "match_type": "startswith", "priority": 61},
    {"keyword": "POS",              "category": "everyday_spending", "match_type": "startswith", "priority": 62},
    {"keyword": "DEB",              "category": "everyday_spending", "match_type": "startswith", "priority": 63},
    # Supermarkets / groceries
    {"keyword": "TESCO",            "category": "everyday_spending", "match_type": "contains",   "priority": 65},
    {"keyword": "SAINSBURY",        "category": "everyday_spending", "match_type": "contains",   "priority": 66},
    {"keyword": "ASDA",             "category": "everyday_spending", "match_type": "contains",   "priority": 67},
    {"keyword": "MORRISONS",        "category": "everyday_spending", "match_type": "contains",   "priority": 68},
    {"keyword": "WAITROSE",         "category": "everyday_spending", "match_type": "contains",   "priority": 69},
    {"keyword": "LIDL",             "category": "everyday_spending", "match_type": "contains",   "priority": 70},
    {"keyword": "ALDI",             "category": "everyday_spending", "match_type": "contains",   "priority": 71},
    {"keyword": "MARKS SPENCER",    "category": "everyday_spending", "match_type": "contains",   "priority": 72},
    {"keyword": "ICELAND",          "category": "everyday_spending", "match_type": "contains",   "priority": 73},
    {"keyword": "FARMFOODS",        "category": "everyday_spending", "match_type": "contains",   "priority": 74},
    {"keyword": "SPAR",             "category": "everyday_spending", "match_type": "contains",   "priority": 75},
    # Fast food & coffee
    {"keyword": "MCDONALDS",        "category": "everyday_spending", "match_type": "contains",   "priority": 76},
    {"keyword": "KFC",              "category": "everyday_spending", "match_type": "contains",   "priority": 77},
    {"keyword": "SUBWAY",           "category": "everyday_spending", "match_type": "contains",   "priority": 78},
    {"keyword": "GREGGS",           "category": "everyday_spending", "match_type": "contains",   "priority": 79},
    {"keyword": "STARBUCKS",        "category": "everyday_spending", "match_type": "contains",   "priority": 80},
    {"keyword": "COSTA COFFEE",     "category": "everyday_spending", "match_type": "contains",   "priority": 81},
    {"keyword": "PRET A MANGER",    "category": "everyday_spending", "match_type": "contains",   "priority": 82},
    {"keyword": "NANDOS",           "category": "everyday_spending", "match_type": "contains",   "priority": 83},
    {"keyword": "PIZZA EXPRESS",    "category": "everyday_spending", "match_type": "contains",   "priority": 84},
    {"keyword": "PIZZA HUT",        "category": "everyday_spending", "match_type": "contains",   "priority": 85},
    {"keyword": "DOMINOS",          "category": "everyday_spending", "match_type": "contains",   "priority": 86},
    # Food delivery
    {"keyword": "DELIVEROO",        "category": "everyday_spending", "match_type": "contains",   "priority": 87},
    {"keyword": "JUST EAT",         "category": "everyday_spending", "match_type": "contains",   "priority": 88},
    {"keyword": "UBER EATS",        "category": "everyday_spending", "match_type": "contains",   "priority": 89},
    # Online shopping / retail
    {"keyword": "AMAZON",           "category": "everyday_spending", "match_type": "contains",   "priority": 90},
    {"keyword": "EBAY",             "category": "everyday_spending", "match_type": "contains",   "priority": 91},
    {"keyword": "ARGOS",            "category": "everyday_spending", "match_type": "contains",   "priority": 92},
    {"keyword": "IKEA",             "category": "everyday_spending", "match_type": "contains",   "priority": 93},
    {"keyword": "PRIMARK",          "category": "everyday_spending", "match_type": "contains",   "priority": 94},
    {"keyword": "ASOS",             "category": "everyday_spending", "match_type": "contains",   "priority": 95},
    {"keyword": "JD SPORTS",        "category": "everyday_spending", "match_type": "contains",   "priority": 96},
    {"keyword": "SPORTS DIRECT",    "category": "everyday_spending", "match_type": "contains",   "priority": 97},
    {"keyword": "CURRYS",           "category": "everyday_spending", "match_type": "contains",   "priority": 98},
    # Transport
    {"keyword": "TFL",              "category": "transport",         "match_type": "contains",   "priority": 100},
    {"keyword": "TRAINLINE",        "category": "transport",         "match_type": "contains",   "priority": 101},
    {"keyword": "NATIONAL RAIL",    "category": "transport",         "match_type": "contains",   "priority": 102},
    {"keyword": "AVANTI",           "category": "transport",         "match_type": "contains",   "priority": 103},
    {"keyword": "GREAT WESTERN",    "category": "transport",         "match_type": "contains",   "priority": 104},
    {"keyword": "EAST MIDLANDS",    "category": "transport",         "match_type": "contains",   "priority": 105},
    {"keyword": "HEATHROW EXPRESS", "category": "transport",         "match_type": "contains",   "priority": 106},
    {"keyword": "UBER",             "category": "transport",         "match_type": "contains",   "priority": 107},
    {"keyword": "ADDISON LEE",      "category": "transport",         "match_type": "contains",   "priority": 108},
    {"keyword": "RYANAIR",          "category": "transport",         "match_type": "contains",   "priority": 109},
    {"keyword": "EASYJET",          "category": "transport",         "match_type": "contains",   "priority": 110},
    {"keyword": "BRITISH AIRWAYS",  "category": "transport",         "match_type": "contains",   "priority": 111},
    {"keyword": "JET2",             "category": "transport",         "match_type": "contains",   "priority": 112},
    {"keyword": "VIRGIN ATLANTIC",  "category": "transport",         "match_type": "contains",   "priority": 113},
    {"keyword": "PARKING",          "category": "transport",         "match_type": "contains",   "priority": 114},
    {"keyword": "CAR PARK",         "category": "transport",         "match_type": "contains",   "priority": 115},
    {"keyword": "PETROL",           "category": "transport",         "match_type": "contains",   "priority": 116},
    {"keyword": "SHELL PETROL",     "category": "transport",         "match_type": "contains",   "priority": 117},
    {"keyword": "BP GARAGE",        "category": "transport",         "match_type": "contains",   "priority": 118},
    {"keyword": "ESSO",             "category": "transport",         "match_type": "contains",   "priority": 119},
    # Financial & banking
    {"keyword": "ATM",              "category": "financial_banking", "match_type": "startswith", "priority": 120},
    {"keyword": "CHQ",              "category": "financial_banking", "match_type": "startswith", "priority": 121},
    {"keyword": "CHEQUE",           "category": "financial_banking", "match_type": "startswith", "priority": 122},
    {"keyword": "TFR",              "category": "financial_banking", "match_type": "startswith", "priority": 123},
    {"keyword": "TRANSFER",         "category": "financial_banking", "match_type": "startswith", "priority": 124},
    {"keyword": "FPS",              "category": "financial_banking", "match_type": "startswith", "priority": 125},
    {"keyword": "INT",              "category": "financial_banking", "match_type": "startswith", "priority": 126},
    {"keyword": "REFUND",           "category": "financial_banking", "match_type": "startswith", "priority": 127},
    {"keyword": "PAYPAL",           "category": "financial_banking", "match_type": "contains",   "priority": 128},
    {"keyword": "WISE",             "category": "financial_banking", "match_type": "contains",   "priority": 129},
    {"keyword": "LOAN REPAYMENT",   "category": "financial_banking", "match_type": "contains",   "priority": 130},
    {"keyword": "CREDIT CARD",      "category": "financial_banking", "match_type": "contains",   "priority": 131},
    {"keyword": "BANK CHARGE",      "category": "financial_banking", "match_type": "contains",   "priority": 132},
    {"keyword": "OVERDRAFT FEE",    "category": "financial_banking", "match_type": "contains",   "priority": 133},
    # Health & personal care
    {"keyword": "BOOTS",            "category": "health_personal",   "match_type": "contains",   "priority": 140},
    {"keyword": "SUPERDRUG",        "category": "health_personal",   "match_type": "contains",   "priority": 141},
    {"keyword": "LLOYDS PHARMACY",  "category": "health_personal",   "match_type": "contains",   "priority": 142},
    {"keyword": "PHARMACY",         "category": "health_personal",   "match_type": "contains",   "priority": 143},
    {"keyword": "PRESCRIPTION",     "category": "health_personal",   "match_type": "contains",   "priority": 144},
    {"keyword": "NHS",              "category": "health_personal",   "match_type": "contains",   "priority": 145},
    {"keyword": "BUPA",             "category": "health_personal",   "match_type": "contains",   "priority": 146},
    {"keyword": "VITALITY",         "category": "health_personal",   "match_type": "contains",   "priority": 147},
    {"keyword": "DENTIST",          "category": "health_personal",   "match_type": "contains",   "priority": 148},
    {"keyword": "SPECSAVERS",       "category": "health_personal",   "match_type": "contains",   "priority": 149},
    {"keyword": "VISION EXPRESS",   "category": "health_personal",   "match_type": "contains",   "priority": 150},
    {"keyword": "PUREGYM",          "category": "health_personal",   "match_type": "contains",   "priority": 151},
    {"keyword": "THE GYM",          "category": "health_personal",   "match_type": "contains",   "priority": 152},
    {"keyword": "NUFFIELD HEALTH",  "category": "health_personal",   "match_type": "contains",   "priority": 153},
    {"keyword": "DAVID LLOYD",      "category": "health_personal",   "match_type": "contains",   "priority": 154},
    {"keyword": "VIRGIN ACTIVE",    "category": "health_personal",   "match_type": "contains",   "priority": 155},
    {"keyword": "ANYTIME FITNESS",  "category": "health_personal",   "match_type": "contains",   "priority": 156},
    # Leisure & lifestyle
    {"keyword": "NETFLIX",          "category": "leisure_lifestyle", "match_type": "contains",   "priority": 160},
    {"keyword": "SPOTIFY",          "category": "leisure_lifestyle", "match_type": "contains",   "priority": 161},
    {"keyword": "APPLE MUSIC",      "category": "leisure_lifestyle", "match_type": "contains",   "priority": 162},
    {"keyword": "DISNEY",           "category": "leisure_lifestyle", "match_type": "contains",   "priority": 163},
    {"keyword": "NOW TV",           "category": "leisure_lifestyle", "match_type": "contains",   "priority": 164},
    {"keyword": "AMAZON PRIME",     "category": "leisure_lifestyle", "match_type": "contains",   "priority": 165},
    {"keyword": "PARAMOUNT",        "category": "leisure_lifestyle", "match_type": "contains",   "priority": 166},
    {"keyword": "APPLE TV",         "category": "leisure_lifestyle", "match_type": "contains",   "priority": 167},
    {"keyword": "ODEON",            "category": "leisure_lifestyle", "match_type": "contains",   "priority": 168},
    {"keyword": "CINEWORLD",        "category": "leisure_lifestyle", "match_type": "contains",   "priority": 169},
    {"keyword": "VUE CINEMA",       "category": "leisure_lifestyle", "match_type": "contains",   "priority": 170},
    {"keyword": "TICKETMASTER",     "category": "leisure_lifestyle", "match_type": "contains",   "priority": 171},
    {"keyword": "EVENTBRITE",       "category": "leisure_lifestyle", "match_type": "contains",   "priority": 172},
    {"keyword": "AIRBNB",           "category": "leisure_lifestyle", "match_type": "contains",   "priority": 173},
    {"keyword": "BOOKING.COM",      "category": "leisure_lifestyle", "match_type": "contains",   "priority": 174},
    {"keyword": "PREMIER INN",      "category": "leisure_lifestyle", "match_type": "contains",   "priority": 175},
    {"keyword": "TRAVELODGE",       "category": "leisure_lifestyle", "match_type": "contains",   "priority": 176},
    {"keyword": "BET365",           "category": "leisure_lifestyle", "match_type": "contains",   "priority": 177},
    {"keyword": "BETFAIR",          "category": "leisure_lifestyle", "match_type": "contains",   "priority": 178},
    {"keyword": "PADDY POWER",      "category": "leisure_lifestyle", "match_type": "contains",   "priority": 179},
    {"keyword": "WILLIAM HILL",     "category": "leisure_lifestyle", "match_type": "contains",   "priority": 180},
    {"keyword": "LADBROKES",        "category": "leisure_lifestyle", "match_type": "contains",   "priority": 181},
]

# Default merchant rules — mapped against description_raw.
# Merchant rules are checked BEFORE keyword rules in apply_rules so these
# will fire for well-known merchants even when the description contains noise
# (e.g. "TESCO STORES 1234" still matches the "TESCO" merchant rule).
_DEFAULT_MERCHANT_RULES: list[dict] = [
    # Telecoms & media (household_bills)
    {"merchant_name": "VODAFONE",       "category": "household_bills",   "match_type": "contains", "priority": 10},
    {"merchant_name": "O2",             "category": "household_bills",   "match_type": "contains", "priority": 11},
    {"merchant_name": "EE LIMITED",     "category": "household_bills",   "match_type": "contains", "priority": 12},
    {"merchant_name": "THREE MOBILE",   "category": "household_bills",   "match_type": "contains", "priority": 13},
    {"merchant_name": "SKY DIGITAL",    "category": "household_bills",   "match_type": "contains", "priority": 14},
    {"merchant_name": "VIRGIN MEDIA",   "category": "household_bills",   "match_type": "contains", "priority": 15},
    {"merchant_name": "BT BROADBAND",   "category": "household_bills",   "match_type": "contains", "priority": 16},
    # Utilities (household_bills)
    {"merchant_name": "BRITISH GAS",    "category": "household_bills",   "match_type": "contains", "priority": 20},
    {"merchant_name": "EDF ENERGY",     "category": "household_bills",   "match_type": "contains", "priority": 21},
    {"merchant_name": "E.ON",           "category": "household_bills",   "match_type": "contains", "priority": 22},
    {"merchant_name": "SCOTTISH POWER", "category": "household_bills",   "match_type": "contains", "priority": 23},
    {"merchant_name": "NPOWER",         "category": "household_bills",   "match_type": "contains", "priority": 24},
    {"merchant_name": "THAMES WATER",   "category": "household_bills",   "match_type": "contains", "priority": 25},
    {"merchant_name": "ANGLIAN WATER",  "category": "household_bills",   "match_type": "contains", "priority": 26},
    {"merchant_name": "SEVERN TRENT",   "category": "household_bills",   "match_type": "contains", "priority": 27},
    {"merchant_name": "UNITED UTILITIES","category": "household_bills",  "match_type": "contains", "priority": 28},
    {"merchant_name": "YORKSHIRE WATER","category": "household_bills",   "match_type": "contains", "priority": 29},
    # Supermarkets (everyday_spending)
    {"merchant_name": "TESCO",          "category": "everyday_spending", "match_type": "contains", "priority": 40},
    {"merchant_name": "SAINSBURYS",     "category": "everyday_spending", "match_type": "contains", "priority": 41},
    {"merchant_name": "ASDA",           "category": "everyday_spending", "match_type": "contains", "priority": 42},
    {"merchant_name": "MORRISONS",      "category": "everyday_spending", "match_type": "contains", "priority": 43},
    {"merchant_name": "WAITROSE",       "category": "everyday_spending", "match_type": "contains", "priority": 44},
    {"merchant_name": "LIDL",           "category": "everyday_spending", "match_type": "contains", "priority": 45},
    {"merchant_name": "ALDI",           "category": "everyday_spending", "match_type": "contains", "priority": 46},
    {"merchant_name": "MARKS SPENCER",  "category": "everyday_spending", "match_type": "contains", "priority": 47},
    {"merchant_name": "CO-OPERATIVE",   "category": "everyday_spending", "match_type": "contains", "priority": 48},
    {"merchant_name": "ICELAND",        "category": "everyday_spending", "match_type": "contains", "priority": 49},
    # Restaurants & coffee (everyday_spending)
    {"merchant_name": "MCDONALDS",      "category": "everyday_spending", "match_type": "contains", "priority": 50},
    {"merchant_name": "KFC",            "category": "everyday_spending", "match_type": "contains", "priority": 51},
    {"merchant_name": "SUBWAY",         "category": "everyday_spending", "match_type": "contains", "priority": 52},
    {"merchant_name": "GREGGS",         "category": "everyday_spending", "match_type": "contains", "priority": 53},
    {"merchant_name": "STARBUCKS",      "category": "everyday_spending", "match_type": "contains", "priority": 54},
    {"merchant_name": "COSTA COFFEE",   "category": "everyday_spending", "match_type": "contains", "priority": 55},
    {"merchant_name": "PRET A MANGER",  "category": "everyday_spending", "match_type": "contains", "priority": 56},
    {"merchant_name": "NANDOS",         "category": "everyday_spending", "match_type": "contains", "priority": 57},
    {"merchant_name": "PIZZA EXPRESS",  "category": "everyday_spending", "match_type": "contains", "priority": 58},
    {"merchant_name": "PIZZA HUT",      "category": "everyday_spending", "match_type": "contains", "priority": 59},
    {"merchant_name": "WAGAMAMA",       "category": "everyday_spending", "match_type": "contains", "priority": 60},
    # Food delivery (everyday_spending)
    {"merchant_name": "DELIVEROO",      "category": "everyday_spending", "match_type": "contains", "priority": 61},
    {"merchant_name": "JUST EAT",       "category": "everyday_spending", "match_type": "contains", "priority": 62},
    {"merchant_name": "UBER EATS",      "category": "everyday_spending", "match_type": "contains", "priority": 63},
    # Online retail (everyday_spending)
    {"merchant_name": "AMAZON",         "category": "everyday_spending", "match_type": "contains", "priority": 65},
    {"merchant_name": "EBAY",           "category": "everyday_spending", "match_type": "contains", "priority": 66},
    {"merchant_name": "ARGOS",          "category": "everyday_spending", "match_type": "contains", "priority": 67},
    {"merchant_name": "IKEA",           "category": "everyday_spending", "match_type": "contains", "priority": 68},
    {"merchant_name": "PRIMARK",        "category": "everyday_spending", "match_type": "contains", "priority": 69},
    {"merchant_name": "ASOS",           "category": "everyday_spending", "match_type": "contains", "priority": 70},
    {"merchant_name": "JD SPORTS",      "category": "everyday_spending", "match_type": "contains", "priority": 71},
    {"merchant_name": "SPORTS DIRECT",  "category": "everyday_spending", "match_type": "contains", "priority": 72},
    # Transport
    {"merchant_name": "TFL",            "category": "transport",         "match_type": "contains", "priority": 80},
    {"merchant_name": "TRAINLINE",      "category": "transport",         "match_type": "contains", "priority": 81},
    {"merchant_name": "RYANAIR",        "category": "transport",         "match_type": "contains", "priority": 82},
    {"merchant_name": "EASYJET",        "category": "transport",         "match_type": "contains", "priority": 83},
    {"merchant_name": "BRITISH AIRWAYS","category": "transport",         "match_type": "contains", "priority": 84},
    {"merchant_name": "JET2",           "category": "transport",         "match_type": "contains", "priority": 85},
    {"merchant_name": "UBER",           "category": "transport",         "match_type": "contains", "priority": 86},
    # Health & personal
    {"merchant_name": "BOOTS",          "category": "health_personal",   "match_type": "contains", "priority": 90},
    {"merchant_name": "SUPERDRUG",      "category": "health_personal",   "match_type": "contains", "priority": 91},
    {"merchant_name": "LLOYDS PHARMACY","category": "health_personal",   "match_type": "contains", "priority": 92},
    {"merchant_name": "PUREGYM",        "category": "health_personal",   "match_type": "contains", "priority": 93},
    {"merchant_name": "THE GYM GROUP",  "category": "health_personal",   "match_type": "contains", "priority": 94},
    {"merchant_name": "NUFFIELD HEALTH","category": "health_personal",   "match_type": "contains", "priority": 95},
    {"merchant_name": "DAVID LLOYD",    "category": "health_personal",   "match_type": "contains", "priority": 96},
    {"merchant_name": "VIRGIN ACTIVE",  "category": "health_personal",   "match_type": "contains", "priority": 97},
    {"merchant_name": "SPECSAVERS",     "category": "health_personal",   "match_type": "contains", "priority": 98},
    # Leisure & lifestyle
    {"merchant_name": "NETFLIX",        "category": "leisure_lifestyle", "match_type": "contains", "priority": 100},
    {"merchant_name": "SPOTIFY",        "category": "leisure_lifestyle", "match_type": "contains", "priority": 101},
    {"merchant_name": "DISNEY",         "category": "leisure_lifestyle", "match_type": "contains", "priority": 102},
    {"merchant_name": "NOW TV",         "category": "leisure_lifestyle", "match_type": "contains", "priority": 103},
    {"merchant_name": "AMAZON PRIME",   "category": "leisure_lifestyle", "match_type": "contains", "priority": 104},
    {"merchant_name": "APPLE MUSIC",    "category": "leisure_lifestyle", "match_type": "contains", "priority": 105},
    {"merchant_name": "APPLE TV",       "category": "leisure_lifestyle", "match_type": "contains", "priority": 106},
    {"merchant_name": "PARAMOUNT",      "category": "leisure_lifestyle", "match_type": "contains", "priority": 107},
    {"merchant_name": "ODEON",          "category": "leisure_lifestyle", "match_type": "contains", "priority": 108},
    {"merchant_name": "CINEWORLD",      "category": "leisure_lifestyle", "match_type": "contains", "priority": 109},
    {"merchant_name": "TICKETMASTER",   "category": "leisure_lifestyle", "match_type": "contains", "priority": 110},
    {"merchant_name": "AIRBNB",         "category": "leisure_lifestyle", "match_type": "contains", "priority": 111},
    {"merchant_name": "PREMIER INN",    "category": "leisure_lifestyle", "match_type": "contains", "priority": 112},
    {"merchant_name": "TRAVELODGE",     "category": "leisure_lifestyle", "match_type": "contains", "priority": 113},
    {"merchant_name": "BET365",         "category": "leisure_lifestyle", "match_type": "contains", "priority": 114},
    {"merchant_name": "BETFAIR",        "category": "leisure_lifestyle", "match_type": "contains", "priority": 115},
    {"merchant_name": "PADDY POWER",    "category": "leisure_lifestyle", "match_type": "contains", "priority": 116},
    {"merchant_name": "WILLIAM HILL",   "category": "leisure_lifestyle", "match_type": "contains", "priority": 117},
    {"merchant_name": "LADBROKES",      "category": "leisure_lifestyle", "match_type": "contains", "priority": 118},
]

# Default counterparty rules — matched against counterparty_name / counterparty.
_DEFAULT_COUNTERPARTY_RULES: list[dict] = [
    # Government & benefits
    {"counterparty": "DWP",             "category": "income",            "match_type": "contains", "priority": 10},
    {"counterparty": "HMRC",            "category": "income",            "match_type": "contains", "priority": 11},
    {"counterparty": "UNIVERSAL CREDIT","category": "income",            "match_type": "contains", "priority": 12},
    {"counterparty": "CHILD BENEFIT",   "category": "income",            "match_type": "contains", "priority": 13},
    {"counterparty": "PENSION SERVICE", "category": "income",            "match_type": "contains", "priority": 14},
    # Council (household_bills)
    {"counterparty": "COUNCIL",         "category": "household_bills",   "match_type": "contains", "priority": 20},
    # Utilities (household_bills)
    {"counterparty": "BRITISH GAS",     "category": "household_bills",   "match_type": "contains", "priority": 25},
    {"counterparty": "THAMES WATER",    "category": "household_bills",   "match_type": "contains", "priority": 26},
    {"counterparty": "SCOTTISH POWER",  "category": "household_bills",   "match_type": "contains", "priority": 27},
    # Telecoms (household_bills)
    {"counterparty": "VODAFONE",        "category": "household_bills",   "match_type": "contains", "priority": 30},
    {"counterparty": "VIRGIN MEDIA",    "category": "household_bills",   "match_type": "contains", "priority": 31},
    {"counterparty": "SKY",             "category": "household_bills",   "match_type": "contains", "priority": 32},
    # Financial
    {"counterparty": "PAYPAL",          "category": "financial_banking", "match_type": "contains", "priority": 40},
    {"counterparty": "WISE",            "category": "financial_banking", "match_type": "contains", "priority": 41},
    {"counterparty": "REVOLUT",         "category": "financial_banking", "match_type": "contains", "priority": 42},
]


def _seed_default_rules():
    """Idempotently seed default UK bank categorisation rules.

    Unlike the previous approach (which only seeded when the table was
    completely empty), this function uses stable deterministic rule IDs so
    that:
    - New deployments receive all default rules.
    - Existing deployments receive any *new* default rules added in code
      updates without overwriting rules created by users (whose IDs have
      random suffixes like ``kr_abc12345``).
    - Already-seeded default rules are skipped silently.
    - Semantically identical rules created manually by users (same keyword +
      category combination) are also skipped to avoid duplicates.
    """
    if SessionLocal is None:
        logger.warning("_seed_default_rules: SessionLocal is None — database not configured, skipping rule seeding")
        return
    db = SessionLocal()
    try:
        added = 0

        # ── Keyword rules ────────────────────────────────────────────────────
        for r in _DEFAULT_KEYWORD_RULES:
            rule_id = _stable_rule_id("dkr", r["keyword"], r["category"])
            if db.query(KeywordRule).filter(KeywordRule.rule_id == rule_id).first():
                continue  # Already seeded
            # Also skip if a semantically identical rule exists (different ID)
            if db.query(KeywordRule).filter(
                KeywordRule.keyword == r["keyword"],
                KeywordRule.category == r["category"],
            ).first():
                continue
            db.add(KeywordRule(
                rule_id=rule_id,
                keyword=r["keyword"],
                category=r["category"],
                match_type=r.get("match_type", "contains"),
                case_sensitive=False,
                priority=r["priority"],
                enabled=True,
            ))
            added += 1

        # ── Merchant rules ───────────────────────────────────────────────────
        for r in _DEFAULT_MERCHANT_RULES:
            rule_id = _stable_rule_id("dmr", r["merchant_name"], r["category"])
            if db.query(MerchantRule).filter(MerchantRule.rule_id == rule_id).first():
                continue
            if db.query(MerchantRule).filter(
                MerchantRule.merchant_name == r["merchant_name"],
                MerchantRule.category == r["category"],
            ).first():
                continue
            db.add(MerchantRule(
                rule_id=rule_id,
                merchant_name=r["merchant_name"],
                category=r["category"],
                match_type=r.get("match_type", "contains"),
                case_sensitive=False,
                priority=r["priority"],
                enabled=True,
            ))
            added += 1

        # ── Counterparty rules ───────────────────────────────────────────────
        for r in _DEFAULT_COUNTERPARTY_RULES:
            rule_id = _stable_rule_id("dcr", r["counterparty"], r["category"])
            if db.query(CounterpartyRule).filter(CounterpartyRule.rule_id == rule_id).first():
                continue
            if db.query(CounterpartyRule).filter(
                CounterpartyRule.counterparty == r["counterparty"],
                CounterpartyRule.category == r["category"],
            ).first():
                continue
            db.add(CounterpartyRule(
                rule_id=rule_id,
                counterparty=r["counterparty"],
                category=r["category"],
                match_type=r.get("match_type", "contains"),
                case_sensitive=False,
                priority=r["priority"],
                enabled=True,
            ))
            added += 1

        if added:
            db.commit()
            logger.info("Seeded %d default categorisation rules", added)
        else:
            logger.debug("Default categorisation rules already up to date — nothing to seed")
    except Exception:
        db.rollback()
        logger.exception("Failed to seed default categorisation rules")
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
def get_case_exceptions(
    case_id: str,
    min_severity: str = Query(
        default="Medium",
        description=(
            "Minimum severity level to include. One of: Low, Medium, High, Critical. "
            "Defaults to Medium so that low-signal informational exceptions are hidden. "
            "Pass min_severity=Low to retrieve all exceptions."
        ),
    ),
    db: Session = Depends(get_db),
):
    """List exceptions for a case, filtered to *min_severity* and above."""
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    threshold = _SEVERITY_RANK.get(min_severity, 1)
    all_exceptions = db.query(CaseException).filter(CaseException.case_id == case_id).all()
    return [e for e in all_exceptions if _SEVERITY_RANK.get(e.severity, 0) >= threshold]


@app.get("/documents/{document_id}/validation-results", dependencies=[Depends(verify_api_key)], response_model=list[ValidationResultResponse])
def get_document_validation_results(document_id: str, db: Session = Depends(get_db)):
    """List all validation check results for a document"""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return db.query(ValidationResult).filter(ValidationResult.document_id == document_id).all()


@app.get("/documents/{document_id}/exceptions", dependencies=[Depends(verify_api_key)], response_model=list[ExceptionResponse])
def get_document_exceptions(
    document_id: str,
    min_severity: str = Query(
        default="Medium",
        description=(
            "Minimum severity level to include. One of: Low, Medium, High, Critical. "
            "Defaults to Medium so that low-signal informational exceptions are hidden. "
            "Pass min_severity=Low to retrieve all exceptions."
        ),
    ),
    db: Session = Depends(get_db),
):
    """List exceptions for a document, filtered to *min_severity* and above."""
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    threshold = _SEVERITY_RANK.get(min_severity, 1)
    all_exceptions = db.query(CaseException).filter(CaseException.document_id == document_id).all()
    return [e for e in all_exceptions if _SEVERITY_RANK.get(e.severity, 0) >= threshold]
    all_exceptions = db.query(CaseException).filter(CaseException.document_id == document_id).all()
    return [e for e in all_exceptions if severity_rank.get(e.severity, 0) >= threshold]


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


@app.get("/documents/{document_id}/extraction-diagnostics", dependencies=[Depends(verify_api_key)])
def get_extraction_diagnostics(document_id: str, db: Session = Depends(get_db)):
    """Return a full diagnostic report for a document's most recent extraction run.

    Includes:
    - raw/normalised/inserted row counts from the ExtractionAudit record
    - dropped rows and reasons (when recorded)
    - duplicate rows and reasons (when recorded)
    - reconciliation outcome and findings from CaseException records
    - money_in / money_out from the Account record
    - exceptions raised
    """
    document = db.query(Document).filter(Document.document_id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    # Most recent audit for this document
    audit = (
        db.query(ExtractionAudit)
        .filter(ExtractionAudit.document_id == document_id)
        .order_by(ExtractionAudit.created_at.desc())
        .first()
    )

    # Account data (first account for this document)
    account = db.query(Account).filter(Account.document_id == document_id).first()

    # Extraction exceptions
    exceptions = (
        db.query(CaseException)
        .filter(CaseException.document_id == document_id)
        .order_by(CaseException.created_at.desc())
        .all()
    )

    # Transaction summary
    from sqlalchemy import func as sqlfunc
    total_txns = db.query(sqlfunc.count(Transaction.id)).filter(
        Transaction.document_id == document_id
    ).scalar() or 0
    total_credit = db.query(sqlfunc.sum(Transaction.credit)).filter(
        Transaction.document_id == document_id
    ).scalar()
    total_debit = db.query(sqlfunc.sum(Transaction.debit)).filter(
        Transaction.document_id == document_id
    ).scalar()

    import json as _json
    normalisation_summary = None
    drop_reasons = None
    if audit:
        try:
            normalisation_summary = _json.loads(audit.normalisation_summary_json) if audit.normalisation_summary_json else None
        except Exception:
            pass
        try:
            drop_reasons = _json.loads(audit.drop_reasons_json) if audit.drop_reasons_json else None
        except Exception:
            pass

    return {
        "document_id": document_id,
        "case_id": document.case_id,
        "document_status": document.status,
        "extraction_audit": {
            "extraction_run_id": audit.extraction_run_id if audit else None,
            "processor_name": audit.processor_name if audit else None,
            "processor_version": audit.processor_version if audit else None,
            "raw_response_present": audit.raw_response_present if audit else None,
            "docai_row_count": audit.docai_row_count if audit else None,
            "fallback_row_count": audit.fallback_row_count if audit else None,
            "raw_row_count": audit.raw_row_count if audit else None,
            "normalised_row_count": audit.normalised_row_count if audit else None,
            "inserted_row_count": audit.inserted_row_count if audit else None,
            "dropped_row_count": audit.dropped_row_count if audit else None,
            "duplicate_row_count": audit.duplicate_row_count if audit else None,
            "drop_reasons": drop_reasons,
            "reconciliation_outcome": audit.reconciliation_outcome if audit else None,
            "normalisation_summary": normalisation_summary,
            "created_at": audit.created_at.isoformat() if audit and audit.created_at else None,
        } if audit else None,
        "account_summary": {
            "account_id": account.account_id if account else None,
            "bank_name": account.bank_name if account else None,
            "sort_code": account.sort_code if account else None,
            "account_number_masked": account.account_number_masked if account else None,
            "statement_start_date": account.statement_start_date.isoformat() if account and account.statement_start_date else None,
            "statement_end_date": account.statement_end_date.isoformat() if account and account.statement_end_date else None,
            "opening_balance": float(account.opening_balance) if account and account.opening_balance is not None else None,
            "closing_balance": float(account.closing_balance) if account and account.closing_balance is not None else None,
            "money_in_stated": float(account.money_in) if account and account.money_in is not None else None,
            "money_out_stated": float(account.money_out) if account and account.money_out is not None else None,
        } if account else None,
        "transaction_summary": {
            "total_inserted": total_txns,
            "total_credit": float(total_credit) if total_credit is not None else None,
            "total_debit": float(total_debit) if total_debit is not None else None,
        },
        "exceptions": [
            {
                "exception_id": e.exception_id,
                "exception_type": e.exception_type,
                "severity": e.severity,
                "status": e.status,
                "title": e.title,
                "description": e.description,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in exceptions
        ],
        "exception_count": len(exceptions),
    }
