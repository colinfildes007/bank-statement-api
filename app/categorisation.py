import re
from typing import Optional

from sqlalchemy.orm import Session

from app.models import (
    CounterpartyRule,
    KeywordRule,
    ManualOverride,
    MerchantAlias,
    MerchantRule,
    RegexRule,
    Transaction,
)

# Categories that indicate a transaction could not be resolved
UNRESOLVED_CATEGORIES = {"uncategorised", "unknown", "needs_review"}

# Fallback category derived from the transaction_type field (set during extraction
# from payment-prefix codes such as DD, BGC, SO).  Applied after all rule-based
# matching fails so that transactions without an explicit rule still receive a
# meaningful primary category.  Values must match CATEGORY_CODES in models.py.
_TRANSACTION_TYPE_CATEGORY: dict[str, str] = {
    "bank_giro_credit": "income",
    "bacs": "income",
    "income": "income",
    "credit": "income",
    "direct_debit": "household_bills",
    "standing_order": "household_bills",
    "bill_payment": "household_bills",
    "card_payment": "everyday_spending",
    "atm": "financial_banking",
    "faster_payment": "financial_banking",
    "transfer": "financial_banking",
    "cheque": "financial_banking",
    "interest": "financial_banking",
    "refund": "income",
}

# Subcategory lookup keyed by lowercase keyword / merchant name.
# Used to populate Transaction.category_secondary with a more granular label
# than the top-level category code.
_SUBCATEGORY_MAP: dict[str, str] = {
    # Income
    "salary": "employment_income",
    "wages": "employment_income",
    "payroll": "employment_income",
    "pension": "pension_income",
    "dividend": "investment_income",
    "bacs": "bacs_credit",
    "bgc": "bank_giro_credit",
    "universal credit": "benefits",
    "child benefit": "benefits",
    "tax credit": "benefits",
    "hmrc paye": "employment_income",
    "pip": "benefits",
    "working tax credit": "benefits",
    "interest received": "interest_received",
    "bank interest": "interest_received",
    # Household bills
    "mortgage": "mortgage",
    "rent": "rent",
    "council tax": "council_tax",
    "water": "utilities",
    "gas": "utilities",
    "electricity": "utilities",
    "electric": "utilities",
    "energy": "utilities",
    "broadband": "telecoms",
    "internet": "telecoms",
    "bt": "telecoms",
    "sky": "telecoms",
    "virgin media": "telecoms",
    "vodafone": "telecoms",
    "o2": "telecoms",
    "ee": "telecoms",
    "three": "telecoms",
    "talktalk": "telecoms",
    "tv licence": "tv_licence",
    "tv license": "tv_licence",
    "insurance": "insurance",
    "home insurance": "insurance",
    "car insurance": "insurance",
    "buildings insurance": "insurance",
    "contents insurance": "insurance",
    "life insurance": "insurance",
    "british gas": "utilities",
    "edf": "utilities",
    "e.on": "utilities",
    "scottish power": "utilities",
    "eon": "utilities",
    "npower": "utilities",
    "thames water": "utilities",
    "anglian water": "utilities",
    "severn trent": "utilities",
    "yorkshire water": "utilities",
    "united utilities": "utilities",
    # Transport
    "tfl": "public_transport",
    "national rail": "public_transport",
    "trainline": "public_transport",
    "avanti": "rail_travel",
    "great western railway": "rail_travel",
    "east midlands railway": "rail_travel",
    "south western railway": "rail_travel",
    "northern rail": "rail_travel",
    "crosscountry": "rail_travel",
    "heathrow express": "rail_travel",
    "petrol": "fuel",
    "fuel": "fuel",
    "shell": "fuel",
    "bp": "fuel",
    "esso": "fuel",
    "texaco": "fuel",
    "total energies": "fuel",
    "uber": "taxi",
    "addison lee": "taxi",
    "black cab": "taxi",
    "parking": "parking",
    "ncp": "parking",
    "ryanair": "air_travel",
    "easyjet": "air_travel",
    "british airways": "air_travel",
    "jet2": "air_travel",
    "tui": "travel",
    "virgin atlantic": "air_travel",
    # Everyday spending
    "tesco": "groceries",
    "sainsbury": "groceries",
    "asda": "groceries",
    "morrisons": "groceries",
    "waitrose": "groceries",
    "lidl": "groceries",
    "aldi": "groceries",
    "marks & spencer": "groceries",
    "marks and spencer": "groceries",
    "m&s": "groceries",
    "co-op": "groceries",
    "coop": "groceries",
    "spar": "groceries",
    "farmfoods": "groceries",
    "iceland": "groceries",
    "mcdonalds": "dining_out",
    "mcdonald": "dining_out",
    "kfc": "dining_out",
    "subway": "dining_out",
    "greggs": "dining_out",
    "starbucks": "dining_out",
    "costa coffee": "dining_out",
    "costa": "dining_out",
    "pret": "dining_out",
    "nandos": "dining_out",
    "pizza express": "dining_out",
    "pizza hut": "dining_out",
    "dominos": "dining_out",
    "wagamama": "dining_out",
    "zizzi": "dining_out",
    "deliveroo": "food_delivery",
    "just eat": "food_delivery",
    "uber eats": "food_delivery",
    "amazon": "online_shopping",
    "ebay": "online_shopping",
    "etsy": "online_shopping",
    "asos": "clothing",
    "boohoo": "clothing",
    "next": "clothing",
    "h&m": "clothing",
    "zara": "clothing",
    "primark": "clothing",
    "nike": "clothing",
    "adidas": "clothing",
    "jd sports": "clothing",
    "sports direct": "clothing",
    "argos": "retail",
    "ikea": "retail",
    "b&q": "retail",
    "halfords": "retail",
    "currys": "electronics",
    "apple store": "electronics",
    # Health & Personal
    "boots": "pharmacy",
    "superdrug": "pharmacy",
    "lloyds pharmacy": "pharmacy",
    "pharmacy": "pharmacy",
    "prescription": "pharmacy",
    "nhs": "healthcare",
    "bupa": "healthcare",
    "vitality": "healthcare",
    "dentist": "dental",
    "dental": "dental",
    "optician": "optician",
    "specsavers": "optician",
    "vision express": "optician",
    "puregym": "gym",
    "the gym": "gym",
    "nuffield health": "gym",
    "nuffield": "gym",
    "david lloyd": "gym",
    "virgin active": "gym",
    "fitness first": "gym",
    "anytime fitness": "gym",
    "salon": "personal_care",
    "barber": "personal_care",
    "hair": "personal_care",
    # Leisure & Lifestyle
    "netflix": "streaming",
    "spotify": "streaming",
    "apple music": "streaming",
    "disney": "streaming",
    "amazon prime": "streaming",
    "now tv": "streaming",
    "paramount": "streaming",
    "apple tv": "streaming",
    "dazn": "streaming",
    "vue": "cinema",
    "odeon": "cinema",
    "cineworld": "cinema",
    "cinema": "cinema",
    "theatre": "entertainment",
    "ticketmaster": "entertainment",
    "eventbrite": "entertainment",
    "airbnb": "travel",
    "booking.com": "travel",
    "hotels.com": "travel",
    "holiday inn": "travel",
    "premier inn": "travel",
    "travelodge": "travel",
    "bet365": "gambling",
    "betfair": "gambling",
    "paddy power": "gambling",
    "william hill": "gambling",
    "ladbrokes": "gambling",
    "sky bet": "gambling",
    # Financial & Banking
    "atm": "cash_withdrawal",
    "cheque": "cheque",
    "paypal": "online_payment",
    "wise": "international_transfer",
    "revolut": "digital_banking",
    "monzo": "digital_banking",
    "starling": "digital_banking",
    "loan": "loan_payment",
    "refund": "refund",
    "cashback": "cashback",
    "fee": "bank_charge",
    "charge": "bank_charge",
    "overdraft": "overdraft_fee",
}


def _resolve_subcategory(match_text: Optional[str]) -> Optional[str]:
    """Return a subcategory string for the given matched keyword/merchant name.

    Looks up the normalised (lowercase, stripped) match_text in _SUBCATEGORY_MAP.
    Returns ``None`` when no subcategory is defined for the matched text.
    """
    if not match_text:
        return None
    return _SUBCATEGORY_MAP.get(match_text.lower().strip())


def _text_matches(text: Optional[str], pattern: str, match_type: str, case_sensitive: bool) -> bool:
    """Return True if text matches the given pattern according to match_type."""
    if text is None:
        return False
    if not case_sensitive:
        text = text.lower()
        pattern = pattern.lower()
    if match_type == "exact":
        return text == pattern
    if match_type == "startswith":
        return text.startswith(pattern)
    # default: contains
    return pattern in text


def _regex_flags(flags_str: Optional[str]) -> int:
    """Convert a comma-separated flags string to re flag bitmask."""
    flags = 0
    if not flags_str:
        return flags
    for token in flags_str.upper().split(","):
        token = token.strip()
        if token == "IGNORECASE":
            flags |= re.IGNORECASE
        elif token == "MULTILINE":
            flags |= re.MULTILINE
        elif token == "DOTALL":
            flags |= re.DOTALL
    return flags


def _resolve_canonical_name(transaction: Transaction, aliases: list) -> Optional[str]:
    """
    Resolve the transaction's description/merchant to a canonical merchant name
    by checking the provided alias list.

    Returns the first matching canonical_name, or None if no alias matches.
    The caller is responsible for loading the alias list (once per batch) to
    avoid repeated full table scans.
    """
    for alias in aliases:
        if _text_matches(transaction.description_raw, alias.alias_name, "contains", alias.case_sensitive):
            return alias.canonical_name
        if _text_matches(transaction.merchant_name, alias.alias_name, "contains", alias.case_sensitive):
            return alias.canonical_name
    return None


def apply_rules(db: Session, transaction: Transaction, aliases: Optional[list] = None) -> tuple[str, str, Optional[str], Optional[str]]:
    """Apply categorisation rules to a transaction in priority order.

    Priority:
      1. Manual override (highest)
      2. Merchant rules   (default priority 100, matched against description)
      3. Counterparty rules (default priority 150, matched against counterparty)
      4. Keyword rules    (default priority 200, matched against description,
                           description_normalised, reference, and counterparty_name)
      5. Regex rules      (default priority 300, matched against description and reference)
      6. Transaction-type fallback — derived from the payment-code prefix
         (e.g. DD → household_bills, BGC → income) without any DB queries.
      7. Default → "uncategorised"

    Args:
        aliases: Pre-loaded list of MerchantAlias rows. When processing many
                 transactions in a batch, pass the list loaded once by the
                 caller to avoid a DB query per transaction.

    Returns:
        (category_code, category_source, rule_id, subcategory)
        ``subcategory`` is a more granular label (e.g. "groceries", "streaming",
        "fuel") derived from the matched rule's keyword or merchant name, or
        ``None`` when no subcategory is defined.
    """
    # 1. Manual override
    override = (
        db.query(ManualOverride)
        .filter(ManualOverride.transaction_id == transaction.transaction_id)
        .first()
    )
    if override:
        return override.category, "manual", override.override_id, None

    # Resolve canonical merchant name via alias table.
    # Use pre-loaded aliases if provided, otherwise load from DB.
    if aliases is None:
        aliases = db.query(MerchantAlias).all()
    canonical_name = _resolve_canonical_name(transaction, aliases)

    # 2. Merchant rules — match against description or resolved canonical name
    merchant_rules = (
        db.query(MerchantRule)
        .filter(MerchantRule.enabled.is_(True))
        .order_by(MerchantRule.priority.asc())
        .all()
    )
    for rule in merchant_rules:
        if (
            _text_matches(transaction.description_raw, rule.merchant_name, rule.match_type, rule.case_sensitive) or
            (canonical_name and _text_matches(canonical_name, rule.merchant_name, rule.match_type, rule.case_sensitive))
        ):
            return rule.category, "merchant", rule.rule_id, _resolve_subcategory(rule.merchant_name)

    # 3. Counterparty rules — match against counterparty field
    counterparty_rules = (
        db.query(CounterpartyRule)
        .filter(CounterpartyRule.enabled.is_(True))
        .order_by(CounterpartyRule.priority.asc())
        .all()
    )
    for rule in counterparty_rules:
        if _text_matches(
            transaction.counterparty_name or transaction.counterparty,
            rule.counterparty,
            rule.match_type,
            rule.case_sensitive,
        ):
            return rule.category, "counterparty", rule.rule_id, _resolve_subcategory(rule.counterparty)

    # 4. Keyword rules — match against description (raw + normalised), reference,
    #    and counterparty_name so that transactions where the payment-type prefix
    #    has been stripped (e.g. DocumentAI returns "VODAFONE" rather than
    #    "DD VODAFONE") can still be matched via the counterparty field.
    keyword_rules = (
        db.query(KeywordRule)
        .filter(KeywordRule.enabled.is_(True))
        .order_by(KeywordRule.priority.asc())
        .all()
    )
    counterparty_text = transaction.counterparty_name or transaction.counterparty
    for rule in keyword_rules:
        if (
            _text_matches(transaction.description_raw, rule.keyword, rule.match_type, rule.case_sensitive)
            or _text_matches(transaction.description_normalised, rule.keyword, rule.match_type, rule.case_sensitive)
            or _text_matches(transaction.reference, rule.keyword, rule.match_type, rule.case_sensitive)
            or _text_matches(counterparty_text, rule.keyword, rule.match_type, rule.case_sensitive)
        ):
            return rule.category, "keyword", rule.rule_id, _resolve_subcategory(rule.keyword)

    # 5. Regex rules — match against description or reference
    regex_rules = (
        db.query(RegexRule)
        .filter(RegexRule.enabled.is_(True))
        .order_by(RegexRule.priority.asc())
        .all()
    )
    for rule in regex_rules:
        try:
            compiled = re.compile(rule.pattern, _regex_flags(rule.flags))
            if (transaction.description_raw and compiled.search(transaction.description_raw)) or \
                    (transaction.reference and compiled.search(transaction.reference)):
                return rule.category, "regex", rule.rule_id, None
        except re.error:
            # Skip rules with invalid regex patterns
            continue

    # 6. Transaction-type fallback — uses the payment-code prefix that was
    #    classified during extraction (e.g. DD → direct_debit → household_bills).
    #    This ensures good coverage even when no explicit rule matches.
    if transaction.transaction_type:
        fallback_cat = _TRANSACTION_TYPE_CATEGORY.get(transaction.transaction_type)
        if fallback_cat:
            return fallback_cat, "transaction_type", None, None

    # 7. Default
    return "uncategorised", "default", None, None
