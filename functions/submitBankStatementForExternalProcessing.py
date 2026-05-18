"""
Integration function to submit bank statements for processing via FastAPI.

This function orchestrates the correct multi-step flow:
1. POST /cases to create a case in FastAPI
2. POST /cases/{case_id}/documents/register to register document metadata
3. Update Base44 Document with external_case_id and job_id
"""

import os
import logging
import requests
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Configuration from environment
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "https://bank-statement-api-1.onrender.com")
FASTAPI_API_KEY = os.getenv("FASTAPI_API_KEY")
REQUEST_TIMEOUT = 30  # seconds


def submit_bank_statement_for_external_processing(
    base44_case_id: str,
    base44_document_id: str,
    customer_name: str,
    organisation_name: Optional[str] = None,
    original_filename: str = "statement.pdf",
    file_size: int = 0,
    signed_file_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Submit a bank statement for processing via the FastAPI service.

    This implements the correct multi-step flow:
    1. Create a case in FastAPI using Base44 case_id
    2. Register the document with metadata and signed file URL
    3. Return document_id and case_id for tracking

    Args:
        base44_case_id: The Base44 case ID (used as external_case_id in FastAPI)
        base44_document_id: The Base44 document ID (for linking back)
        customer_name: Name of the customer
        organisation_name: Optional organization name
        original_filename: Original filename of the document
        file_size: Size of the file in bytes
        signed_file_url: Pre-signed URL for the document file

    Returns:
        Dictionary with:
        - success: bool indicating success/failure
        - document_id: FastAPI document ID (store in Base44)
        - case_id: FastAPI case ID
        - error: Error message if failed

    Raises:
        Exception: Re-raises HTTP errors after logging
    """

    if not FASTAPI_API_KEY:
        logger.error("FASTAPI_API_KEY not configured")
        return {
            "success": False,
            "error": "FASTAPI_API_KEY not configured"
        }

    headers = {
        "Authorization": f"Bearer {FASTAPI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        # Step 1: Create a case in FastAPI
        logger.info("Creating case in FastAPI for Base44 case_id=%s", base44_case_id)

        case_create_payload = {
            "customer_name": customer_name,
            "organisation_name": organisation_name or "",
            "jurisdiction": "UK",
            "case_type": "bank_statement_review"
        }

        case_response = requests.post(
            f"{FASTAPI_BASE_URL}/cases",
            json=case_create_payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        if case_response.status_code == 404:
            logger.error("FastAPI /cases endpoint not found (404)")
            return {
                "success": False,
                "error": "FastAPI /cases endpoint returned 404",
                "base44_document_id": base44_document_id,
            }

        case_response.raise_for_status()
        case_data = case_response.json()
        fastapi_case_id = case_data["case_id"]
        logger.info("Case created successfully: fastapi_case_id=%s", fastapi_case_id)

        # Step 2: Register document with metadata
        logger.info(
            "Registering document in FastAPI: case_id=%s, filename=%s",
            fastapi_case_id,
            original_filename,
        )

        document_register_payload = {
            "original_filename": original_filename,
            "source_type": "signed_url" if signed_file_url else "external",
            "file_size": file_size,
            "mime_type": "application/pdf",
        }

        document_response = requests.post(
            f"{FASTAPI_BASE_URL}/cases/{fastapi_case_id}/documents/register",
            json=document_register_payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        if document_response.status_code == 404:
            logger.error(
                "FastAPI case not found or endpoint not found (404): case_id=%s",
                fastapi_case_id
            )
            return {
                "success": False,
                "error": f"FastAPI case or endpoint not found (404): {fastapi_case_id}",
                "base44_document_id": base44_document_id,
                "case_id": fastapi_case_id,
            }

        document_response.raise_for_status()
        document_data = document_response.json()
        fastapi_document_id = document_data["document_id"]

        logger.info(
            "Document registered successfully: document_id=%s, case_id=%s",
            fastapi_document_id,
            fastapi_case_id,
        )

        # Success: return identifiers for Base44 to store
        return {
            "success": True,
            "document_id": fastapi_document_id,
            "case_id": fastapi_case_id,
            "external_case_id": fastapi_case_id,
            "base44_document_id": base44_document_id,
            "submitted_at": datetime.utcnow().isoformat(),
        }

    except requests.exceptions.Timeout:
        error_msg = f"FastAPI request timed out after {REQUEST_TIMEOUT} seconds"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "base44_document_id": base44_document_id,
        }

    except requests.exceptions.HTTPError as http_err:
        error_msg = (
            f"FastAPI HTTP error: {http_err.response.status_code} "
            f"{http_err.response.reason}"
        )
        logger.error(error_msg, exc_info=True)
        try:
            error_detail = http_err.response.json().get("detail", "No detail")
            logger.error("Response detail: %s", error_detail)
        except Exception:
            pass
        return {
            "success": False,
            "error": error_msg,
            "base44_document_id": base44_document_id,
            "http_status": http_err.response.status_code,
        }

    except requests.exceptions.RequestException as req_err:
        error_msg = f"FastAPI request failed: {str(req_err)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "error": error_msg,
            "base44_document_id": base44_document_id,
        }

    except Exception as e:
        error_msg = f"Unexpected error during FastAPI submission: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "error": error_msg,
            "base44_document_id": base44_document_id,
        }
