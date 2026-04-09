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


def _resolve_canonical_name(db: Session, transaction: Transaction) -> Optional[str]:
    """
    Look up a canonical merchant name for this transaction by matching the
    raw description or merchant_name against all active MerchantAlias rows.

    Returns the first matching canonical_name, or None if no alias matches.
    """
    aliases = db.query(MerchantAlias).all()
    for alias in aliases:
        if _text_matches(transaction.description_raw, alias.alias_name, "contains", alias.case_sensitive):
            return alias.canonical_name
        if _text_matches(transaction.merchant_name, alias.alias_name, "contains", alias.case_sensitive):
            return alias.canonical_name
    return None


def apply_rules(db: Session, transaction: Transaction) -> tuple[str, str, Optional[str]]:
    """Apply categorisation rules to a transaction in priority order.

    Priority:
      1. Manual override (highest)
      2. Merchant rules   (default priority 100, matched against description)
      3. Counterparty rules (default priority 150, matched against counterparty)
      4. Keyword rules    (default priority 200, matched against description and reference)
      5. Regex rules      (default priority 300, matched against description and reference)
      6. Default → "uncategorised"

    Returns:
        (category_code, category_source, rule_id)
    """
    # 1. Manual override
    override = (
        db.query(ManualOverride)
        .filter(ManualOverride.transaction_id == transaction.transaction_id)
        .first()
    )
    if override:
        return override.category, "manual", override.override_id

    # Resolve canonical merchant name via alias table (used in merchant rule matching)
    canonical_name = _resolve_canonical_name(db, transaction)

    # 2. Merchant rules — match against description or resolved canonical name
    merchant_rules = (
        db.query(MerchantRule)
        .filter(MerchantRule.enabled.is_(True))
        .order_by(MerchantRule.priority.asc())
        .all()
    )
    for rule in merchant_rules:
        if _text_matches(transaction.description_raw, rule.merchant_name, rule.match_type, rule.case_sensitive):
            return rule.category, "merchant", rule.rule_id
        if canonical_name and _text_matches(canonical_name, rule.merchant_name, rule.match_type, rule.case_sensitive):
            return rule.category, "merchant", rule.rule_id

    # 3. Counterparty rules — match against counterparty field
    counterparty_rules = (
        db.query(CounterpartyRule)
        .filter(CounterpartyRule.enabled.is_(True))
        .order_by(CounterpartyRule.priority.asc())
        .all()
    )
    for rule in counterparty_rules:
        if _text_matches(transaction.counterparty, rule.counterparty, rule.match_type, rule.case_sensitive):
            return rule.category, "counterparty", rule.rule_id

    # 4. Keyword rules — match against description or reference
    keyword_rules = (
        db.query(KeywordRule)
        .filter(KeywordRule.enabled.is_(True))
        .order_by(KeywordRule.priority.asc())
        .all()
    )
    for rule in keyword_rules:
        if _text_matches(transaction.description_raw, rule.keyword, rule.match_type, rule.case_sensitive) or \
                _text_matches(transaction.reference, rule.keyword, rule.match_type, rule.case_sensitive):
            return rule.category, "keyword", rule.rule_id

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
                return rule.category, "regex", rule.rule_id
        except re.error:
            # Skip rules with invalid regex patterns
            continue

    # 6. Default
    return "uncategorised", "default", None
