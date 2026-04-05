import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

# Only raise error if we're trying to use the database
# Allow app to start for health checks first
if not DATABASE_URL and os.getenv("APP_ENV") == "production":
    raise RuntimeError("DATABASE_URL environment variable is not set")

if DATABASE_URL:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
else:
    engine = None
    SessionLocal = None

Base = declarative_base()


def get_db():
    if not SessionLocal:
        raise RuntimeError("Database not configured. DATABASE_URL is missing.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
