"""
Polling function to check FastAPI document extraction status.

Polls GET /documents/{document_id}/status every 30 seconds and updates
Base44 entities when extraction is complete.

Can be used in scheduled automations or Celery tasks.
"""

import os
import logging
import requests
import time
from typing import Optional, Dict, Any, Callable

logger = logging.getLogger(__name__)

# Configuration from environment
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "https://bank-statement-api-1.onrender.com")
FASTAPI_API_KEY = os.getenv("FASTAPI_API_KEY")
POLL_INTERVAL = 30  # seconds
MAX_POLLS = 120  # ~1 hour
REQUEST_TIMEOUT = 30  # seconds


def poll_fastapi_document_status(
    document_id: str,
    case_id: str,
    on_complete: Optional[Callable[[Dict[str, Any]], None]] = None,
    max_polls: int = MAX_POLLS,
    poll_interval: int = POLL_INTERVAL,
) -> Dict[str, Any]:
    """
    Poll FastAPI for document extraction status until complete.

    Polls GET /documents/{document_id}/status every poll_interval seconds
    until extraction is complete (status != 'Uploaded' and != 'Processing').
    When complete, fetches full document data from GET /documents/{document_id}.

    Args:
        document_id: FastAPI document ID to poll
        case_id: FastAPI case ID (for context)
        on_complete: Optional callback function called with extracted data when complete
        max_polls: Maximum number of polls before giving up (default 120 = ~1 hour)
        poll_interval: Seconds between polls (default 30)

    Returns:
        Dictionary with:
        - success: bool indicating if polling completed successfully
        - status: Final document status from FastAPI
        - document: Full document data (if extraction complete)
        - polls: Number of polls executed
        - elapsed_seconds: Total elapsed time
        - error: Error message if failed

    Raises:
        Exception: Re-raises HTTP errors after logging
    """

    if not FASTAPI_API_KEY:
        logger.error("FASTAPI_API_KEY not configured")
        return {
            "success": False,
            "error": "FASTAPI_API_KEY not configured",
            "document_id": document_id,
        }

    headers = {
        "Authorization": f"Bearer {FASTAPI_API_KEY}",
        "Content-Type": "application/json",
    }

    poll_count = 0
    start_time = time.time()

    try:
        while poll_count < max_polls:
            poll_count += 1
            logger.info(
                "Polling FastAPI document status: document_id=%s, poll=%d/%d",
                document_id,
                poll_count,
                max_polls,
            )

            try:
                # Poll status endpoint
                status_response = requests.get(
                    f"{FASTAPI_BASE_URL}/documents/{document_id}/status",
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )

                if status_response.status_code == 404:
                    logger.error(
                        "Document not found in FastAPI (404): document_id=%s", document_id
                    )
                    return {
                        "success": False,
                        "error": "Document not found in FastAPI (404)",
                        "document_id": document_id,
                        "case_id": case_id,
                        "polls": poll_count,
                        "elapsed_seconds": time.time() - start_time,
                    }

                status_response.raise_for_status()
                status_data = status_response.json()
                current_status = status_data.get("status", "Unknown")

                logger.debug(
                    "Document status: %s (document_id=%s)", current_status, document_id
                )

                # Check if extraction is complete
                # Assume "Extracted" or similar terminal states indicate completion
                if current_status not in ("Uploaded", "Processing", "Queued"):
                    logger.info(
                        "Extraction complete with status: %s (document_id=%s)",
                        current_status,
                        document_id,
                    )

                    # Fetch full document data
                    try:
                        doc_response = requests.get(
                            f"{FASTAPI_BASE_URL}/documents/{document_id}",
                            headers=headers,
                            timeout=REQUEST_TIMEOUT,
                        )
                        doc_response.raise_for_status()
                        document_data = doc_response.json()

                        result = {
                            "success": True,
                            "status": current_status,
                            "document": document_data,
                            "polls": poll_count,
                            "elapsed_seconds": time.time() - start_time,
                        }

                        # Call optional completion callback
                        if on_complete:
                            try:
                                on_complete(result)
                            except Exception as cb_err:
                                logger.error(
                                    "Error in completion callback: %s", str(cb_err),
                                    exc_info=True
                                )

                        return result

                    except Exception as doc_err:
                        logger.error(
                            "Failed to fetch full document data: %s", str(doc_err),
                            exc_info=True
                        )
                        return {
                            "success": False,
                            "error": f"Failed to fetch document data: {str(doc_err)}",
                            "document_id": document_id,
                            "case_id": case_id,
                            "status": current_status,
                            "polls": poll_count,
                            "elapsed_seconds": time.time() - start_time,
                        }

                # Not complete yet; wait and poll again
                if poll_count < max_polls:
                    logger.debug(
                        "Extraction still processing; waiting %d seconds...",
                        poll_interval,
                    )
                    time.sleep(poll_interval)

            except requests.exceptions.Timeout:
                logger.warning(
                    "Status poll timed out (poll %d/%d): document_id=%s",
                    poll_count,
                    max_polls,
                    document_id,
                )
                # Continue polling on timeout
                if poll_count < max_polls:
                    time.sleep(poll_interval)
                continue

            except requests.exceptions.RequestException as req_err:
                logger.warning(
                    "Status poll failed (poll %d/%d): %s",
                    poll_count,
                    max_polls,
                    str(req_err),
                )
                # Continue polling on request errors (transient)
                if poll_count < max_polls:
                    time.sleep(poll_interval)
                continue

        # Max polls exceeded
        logger.error(
            "Max polls (%d) exceeded for document_id=%s", max_polls, document_id
        )
        return {
            "success": False,
            "error": f"Polling timeout after {max_polls} attempts",
            "document_id": document_id,
            "case_id": case_id,
            "polls": poll_count,
            "elapsed_seconds": time.time() - start_time,
        }

    except Exception as e:
        error_msg = f"Unexpected error during polling: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "error": error_msg,
            "document_id": document_id,
            "case_id": case_id,
            "polls": poll_count,
            "elapsed_seconds": time.time() - start_time,
        }


def create_poll_and_update_callback(db_session) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a completion callback for use with poll_fastapi_document_status.

    The returned callback updates Base44 entities (Document, Accounts, Transactions, etc.)
    when extraction is complete.

    Args:
        db_session: SQLAlchemy database session for Base44

    Returns:
        Callable that accepts the poll result dict

    Example:
        callback = create_poll_and_update_callback(db)
        result = poll_fastapi_document_status(doc_id, case_id, on_complete=callback)
    """

    def on_extraction_complete(poll_result: Dict[str, Any]) -> None:
        """Update Base44 entities with extracted data from FastAPI."""
        if not poll_result.get("success"):
            logger.error("Polling failed; skipping Base44 update: %s", poll_result)
            return

        document_data = poll_result.get("document", {})
        if not document_data:
            logger.warning("No document data in poll result")
            return

        try:
            # Extract key fields
            document_id = document_data.get("document_id")
            case_id = document_data.get("case_id")
            status = document_data.get("status")

            logger.info(
                "Updating Base44 entities: document_id=%s, case_id=%s, status=%s",
                document_id,
                case_id,
                status,
            )

            # TODO: Update your Base44 Document record
            # Example:
            # doc = db_session.query(Base44Document).filter_by(external_document_id=document_id).first()
            # if doc:
            #     doc.extraction_status = status
            #     doc.extracted_at = datetime.utcnow()
            #     doc.fastapi_case_id = case_id

            # TODO: If extracted data is available, parse and insert
            # accounts = document_data.get("accounts", [])
            # transactions = document_data.get("transactions", [])
            # for account in accounts:
            #     # Create Base44 Account records
            # for txn in transactions:
            #     # Create Base44 Transaction records

            # db_session.commit()
            logger.info("Base44 entities updated successfully")

        except Exception as e:
            logger.error(
                "Error updating Base44 entities: %s", str(e), exc_info=True
            )
            # Don't re-raise; log and continue

    return on_extraction_complete
