from kafka import KafkaConsumer,KafkaProducer
import json
import time
import os
import resend
from app.database import SessionLocal
from app.model import Job
from dotenv import load_dotenv
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import update, or_

load_dotenv()

resend.api_key = os.getenv("RESEND_API_KEY")

consumer=KafkaConsumer(
    "jobs",
    bootstrap_servers=["localhost:9092"],
    group_id="job_workers",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value:json.loads(value.decode("utf-8"))
)
producer=KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=lambda value:json.dumps(value).encode("utf-8"))
max_attempts=3
worker_id = str(uuid.uuid4())
lease_seconds = 30

print(f"Worker started: {worker_id}")

def process_job(job):
    print(f"Processing job:{job}")
    params = {
        "from": "onboarding@resend.dev",
        "to": [job["recipient"]],
        "subject": job["subject"],
        "html": f"<p>{job['body']}</p>"
    }

    email = resend.Emails.send(params)

    print(f"Email sent successfully. Email ID: {email['id']}")

    

def send_to_dl(job,error):
    job["error"]=str(error)
    job["status"]="failed"
    producer.send("jobs.dlq",value=job)
    producer.flush()
    print(f"job send to DLQ:{job['job_id']}")


for message in consumer:
    job = message.value

    db = SessionLocal()

    try:
        db_job = db.query(Job).filter(
            Job.job_id == job["job_id"]
        ).first()

        if db_job is None:
            print(f"Job not found in MySQL: {job['job_id']}")
            continue
        if db_job.status in ["completed", "dead"]:
            print(f"Job is already {db_job.status}. Skipping.")
            consumer.commit()
            continue
        now=datetime.now(timezone.utc).replace(tzinfo=None)
        result=db.execute(
            update(Job)
            .where(
                Job.job_id==job["job_id"],
                or_(
                    Job.status=="pending",
                    (Job.status=="processing") &
                    (Job.lease_until.is_not(None)) &
                    (Job.lease_until < now)
                )
           )
           .values(status="processing",
                   locked_by=worker_id,
                   lease_until=now + timedelta(seconds=lease_seconds))
        )
        db.commit()
        if result.rowcount==0:
            print(f"could not claim job:{job['job_id']}")
            continue
        db.refresh(db_job)

        for attempt in range(1, max_attempts + 1):
            try:
                db_job.attempts = attempt
                db.commit()

                process_job(job)

                db_job.status = "completed"
                db_job.last_error = None
                db_job.locked_by = None
                db_job.lease_until = None
                db.commit()
                
                consumer.commit()

                print(f"Job completed: {job['job_id']}")
                break

            except Exception as error:
                db_job.last_error = str(error)
                db.commit()

                print(
                    f"Attempt {attempt} failed for "
                    f"job {job['job_id']}: {error}"
                )

                if attempt == max_attempts:
                    send_to_dl(job, error)

                    db_job.status = "dead"
                    db_job.locked_by=None
                    db_job.lease_until=None
                    db.commit()
                    consumer.commit()

                else:
                    time.sleep(2 ** (attempt - 1))

    finally:
        db.close()
