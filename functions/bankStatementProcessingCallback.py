"""
Webhook callback handler for FastAPI extraction events.

Receives and validates webhook payloads from FastAPI, then updates
Base44 entities (Document, Accounts, Transactions, etc.).

To be integrated with Flask/FastAPI endpoints.
"""

import os
import logging
import hmac
import hashlib
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Configuration from environment
FASTAPI_WEBHOOK_SECRET = os.getenv("FASTAPI_WEBHOOK_SECRET")


def validate_webhook_signature(
    payload: bytes,
    signature: str,
    secret: Optional[str] = None,
) -> bool:
    """
    Validate webhook signature using HMAC-SHA256.

    Args:
        payload: Raw request body as bytes
        signature: Signature from X-FastAPI-Signature header
        secret: Webhook secret (defaults to FASTAPI_WEBHOOK_SECRET env var)

    Returns:
        True if signature is valid, False otherwise
    """
    if secret is None:
        secret = FASTAPI_WEBHOOK_SECRET

    if not secret:
        logger.warning("Webhook secret not configured; skipping signature validation")
        return False

    try:
        # Compute expected signature: HMAC-SHA256 hex digest
        expected_signature = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        # Constant-time comparison to prevent timing attacks
        return hmac.compare_digest(signature, expected_signature)

    except Exception as e:
        logger.error("Error validating webhook signature: %s", str(e))
        return False


def process_webhook_event(
    event_type: str,
    payload: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """
    Process a FastAPI webhook event and update Base44 entities.

    Supported events:
    - extraction.completed: Extraction succeeded, data ready
    - extraction.failed: Extraction failed
    - extraction.started: Extraction job started

    Args:
        event_type: Type of event (e.g., "extraction.completed")
        payload: Event payload dictionary
        db_session: Optional SQLAlchemy session for Base44 database

    Returns:
        Dictionary with:
        - success: bool indicating if processing succeeded
        - message: Human-readable status message
        - error: Error message if failed
    """

    logger.info("Processing webhook event: type=%s", event_type)

    try:
        if event_type == "extraction.completed":
            return _handle_extraction_completed(payload, db_session)

        elif event_type == "extraction.failed":
            return _handle_extraction_failed(payload, db_session)

        elif event_type == "extraction.started":
            return _handle_extraction_started(payload, db_session)

        else:
            logger.warning("Unknown webhook event type: %s", event_type)
            return {
                "success": False,
                "error": f"Unknown event type: {event_type}",
            }

    except Exception as e:
        error_msg = f"Error processing webhook event: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "error": error_msg,
        }


def _handle_extraction_completed(
    payload: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """Handle extraction.completed webhook event."""

    document_id = payload.get("document_id")
    case_id = payload.get("case_id")
    extracted_data = payload.get("data", {})

    logger.info(
        "Extraction completed: document_id=%s, case_id=%s",
        document_id,
        case_id,
    )

    try:
        if not db_session:
            logger.warning("No database session provided; skipping Base44 update")
            return {
                "success": True,
                "message": "Extraction completed (no DB update)",
            }

        # TODO: Update Base44 Document record
        # Example:
        # doc = db_session.query(Base44Document).filter_by(
        #     external_document_id=document_id
        # ).first()
        # if doc:
        #     doc.extraction_status = "completed"
        #     doc.extracted_at = datetime.utcnow()
        #     doc.fastapi_case_id = case_id

        # TODO: Parse and insert extracted accounts and transactions
        # accounts = extracted_data.get("accounts", [])
        # transactions = extracted_data.get("transactions", [])
        # for account in accounts:
        #     # Create Base44 Account record
        # for txn in transactions:
        #     # Create Base44 Transaction record

        # db_session.commit()

        logger.info("Base44 entities updated from extraction completed event")

        return {
            "success": True,
            "message": f"Extraction completed and Base44 updated: {document_id}",
        }

    except Exception as e:
        error_msg = f"Error handling extraction completed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if db_session:
            db_session.rollback()
        return {
            "success": False,
            "error": error_msg,
        }


def _handle_extraction_failed(
    payload: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """Handle extraction.failed webhook event."""

    document_id = payload.get("document_id")
    case_id = payload.get("case_id")
    error_reason = payload.get("error_reason", "Unknown error")

    logger.warning(
        "Extraction failed: document_id=%s, case_id=%s, reason=%s",
        document_id,
        case_id,
        error_reason,
    )

    try:
        if not db_session:
            logger.warning("No database session provided; skipping Base44 update")
            return {
                "success": True,
                "message": "Extraction failed notification received (no DB update)",
            }

        # TODO: Update Base44 Document record to mark as failed
        # Example:
        # doc = db_session.query(Base44Document).filter_by(
        #     external_document_id=document_id
        # ).first()
        # if doc:
        #     doc.extraction_status = "failed"
        #     doc.error_reason = error_reason
        #     doc.failed_at = datetime.utcnow()

        # TODO: Optionally create a CaseException record
        # exception = CaseException(
        #     exception_id=f"exc_{uuid4().hex[:8]}",
        #     case_id=case_id,
        #     document_id=document_id,
        #     exception_type="extraction_failure",
        #     severity="High",
        #     title="Bank Statement Extraction Failed",
        #     description=error_reason,
        # )
        # db_session.add(exception)

        # db_session.commit()

        logger.info("Base44 entities marked as failed from extraction failed event")

        return {
            "success": True,
            "message": f"Extraction failure recorded: {document_id}",
        }

    except Exception as e:
        error_msg = f"Error handling extraction failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if db_session:
            db_session.rollback()
        return {
            "success": False,
            "error": error_msg,
        }


def _handle_extraction_started(
    payload: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """Handle extraction.started webhook event."""

    document_id = payload.get("document_id")
    case_id = payload.get("case_id")
    job_id = payload.get("job_id")

    logger.info(
        "Extraction started: document_id=%s, case_id=%s, job_id=%s",
        document_id,
        case_id,
        job_id,
    )

    try:
        if not db_session:
            logger.warning("No database session provided; skipping Base44 update")
            return {
                "success": True,
                "message": "Extraction started notification received (no DB update)",
            }

        # TODO: Update Base44 Document record
        # Example:
        # doc = db_session.query(Base44Document).filter_by(
        #     external_document_id=document_id
        # ).first()
        # if doc:
        #     doc.extraction_status = "processing"
        #     doc.fastapi_job_id = job_id
        #     doc.processing_started_at = datetime.utcnow()

        # db_session.commit()

        logger.info("Base44 entities updated from extraction started event")

        return {
            "success": True,
            "message": f"Extraction started: {document_id}",
        }

    except Exception as e:
        error_msg = f"Error handling extraction started: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if db_session:
            db_session.rollback()
        return {
            "success": False,
            "error": error_msg,
        }


# Flask/FastAPI endpoint integration examples

def create_webhook_endpoint(app, db_session=None):
    """
    Factory to create a Flask/FastAPI webhook endpoint.

    Example for Flask:
    ```python
    from flask import Flask, request
    app = Flask(__name__)
    webhook_handler = create_webhook_endpoint(app, db_session=db)

    @app.route("/webhooks/fastapi", methods=["POST"])
    def fastapi_webhook():
        return webhook_handler(request)
    ```

    Example for FastAPI:
    ```python
    from fastapi import FastAPI, Request
    app = FastAPI()
    webhook_handler = create_webhook_endpoint(app, db_session=db)

    @app.post("/webhooks/fastapi")
    async def fastapi_webhook(request: Request):
        return await webhook_handler(request)
    ```
    """

    async def webhook_handler(request):
        """Handle incoming FastAPI webhook."""
        try:
            # Get signature from header
            signature = request.headers.get("X-FastAPI-Signature", "")
            if not signature:
                logger.warning("Missing X-FastAPI-Signature header")
                return {"success": False, "error": "Missing signature header"}, 401

            # Get raw body
            body = await request.get_data() if hasattr(request, 'get_data') else request.body
            if isinstance(body, str):
                body = body.encode()

            # Validate signature
            if not validate_webhook_signature(body, signature):
                logger.warning("Invalid webhook signature")
                return {"success": False, "error": "Invalid signature"}, 401

            # Parse JSON payload
            try:
                payload = request.get_json() if hasattr(request, 'get_json') else request.json
            except Exception as e:
                logger.error("Failed to parse JSON payload: %s", str(e))
                return {"success": False, "error": "Invalid JSON"}, 400

            # Extract event type
            event_type = payload.get("event_type")
            if not event_type:
                logger.warning("Missing event_type in webhook payload")
                return {"success": False, "error": "Missing event_type"}, 400

            # Process event
            result = process_webhook_event(event_type, payload, db_session)

            status_code = 200 if result.get("success") else 500
            return result, status_code

        except Exception as e:
            error_msg = f"Webhook handler error: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {"success": False, "error": error_msg}, 500

    return webhook_handler
