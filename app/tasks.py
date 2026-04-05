import time
from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Document


@celery_app.task(bind=True, name="validate_document_task")
def validate_document_task(self, document_id: str):
    """Async task to validate a document"""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()
        
        if not document:
            return {"error": "Document not found"}
        
        # Simulate validation work
        time.sleep(2)
        
        document.status = "Validated"
        db.commit()
        
        return {
            "document_id": document_id,
            "status": "Validated",
            "message": "Document validation completed"
        }
    finally:
        db.close()


@celery_app.task(bind=True, name="extract_document_task")
def extract_document_task(self, document_id: str):
    """Async task to extract data from a document"""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()
        
        if not document:
            return {"error": "Document not found"}
        
        # Simulate extraction work
        time.sleep(3)
        
        document.status = "Extracted"
        db.commit()
        
        return {
            "document_id": document_id,
            "status": "Extracted",
            "message": "Document extraction completed"
        }
    finally:
        db.close()


@celery_app.task(bind=True, name="categorise_document_task")
def categorise_document_task(self, document_id: str):
    """Async task to categorise a document"""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.document_id == document_id).first()
        
        if not document:
            return {"error": "Document not found"}
        
        # Simulate categorisation work
        time.sleep(2)
        
        document.status = "Categorised"
        db.commit()
        
        return {
            "document_id": document_id,
            "status": "Categorised",
            "message": "Document categorisation completed"
        }
    finally:
        db.close()
