import uuid
from fastapi import FastAPI,Depends,HTTPException
from pydantic import BaseModel,EmailStr
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from pwdlib import PasswordHash
from app.database import Base,engine,SessionLocal
from app.model import User,Job
from app.producer import send_job

app=FastAPI()
password_hash=PasswordHash.recommended()
Base.metadata.create_all(bind=engine)
class userregister(BaseModel):
    username:str
    email:EmailStr
    password:str
def get_db():
    db=SessionLocal()
    try:
        yield db
    finally:
        db.close()
@app.post("/register")
def register_user(user: userregister,
                  db: Session=Depends(get_db)):
    hashed_password=password_hash.hash(user.password)
    new_user=User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password
    )
    
    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="email already exist"
        )
    job = {
            "job_id": str(uuid.uuid4()),
            "task": "send_email",
            "user_id": new_user.id,
            "recipient": new_user.email,
            "subject": "Welcome to Job Queue!",
            "body": f"Hello {new_user.username}, welcome to our application!"
            }
    
    
    new_job = Job(
        job_id=job["job_id"],
        task=job["task"],
        status="pending",
        attempts=0,
        payload=job
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)
    send_job(job)

    return {
        "message":"user registered sucessfully",
        "user_id":new_user.id,
        "username":new_user.username,
        "email":new_user.email
    }