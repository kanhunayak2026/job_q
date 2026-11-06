from app.producer import send_job
job={"job_id": "job_001",
    "task": "send_email",
    "status": "pending"}
send_job(job)
print("job sent sucessfully")
