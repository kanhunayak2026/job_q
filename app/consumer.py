from kafka import KafkaConsumer,KafkaProducer
import json
import time
import os
import resend
from app.database import SessionLocal
from app.model import Job
from dotenv import load_dotenv

load_dotenv()

resend.api_key = os.getenv("RESEND_API_KEY")

consumer=KafkaConsumer(
    "jobs",
    bootstrap_servers=["localhost:9092"],
    group_id="job_workers",
    auto_offset_reset="earliest",
    value_deserializer=lambda value:json.loads(value.decode("utf-8"))
)
producer=KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer=lambda value:json.dumps(value).encode("utf-8"))
max_attempts=3

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

        db_job.status = "processing"
        db.commit()

        for attempt in range(1, max_attempts + 1):
            try:
                db_job.attempts = attempt
                db.commit()

                process_job(job)

                db_job.status = "completed"
                db_job.last_error = None
                db.commit()

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
                    db.commit()

                else:
                    time.sleep(2 ** (attempt - 1))

    finally:
        db.close()
