from sqlalchemy import Column,Integer,String,Text,DateTime,JSON
from app.database import Base
from sqlalchemy.sql import func

class User(Base):
    __tablename__="users"
    id=Column(Integer,primary_key=True,index=True)
    username=Column(String(100),nullable=False)
    email=Column(String(255),unique=True,nullable=False)
    hashed_password=Column(String(255),nullable=False)

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), unique=True, nullable=False, index=True)
    task = Column(String(100), nullable=False)

    status = Column(String(20), default="pending", nullable=False)
    attempts = Column(Integer, default=0, nullable=False)

    payload = Column(JSON, nullable=False)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now()
    )

