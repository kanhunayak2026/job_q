from kafka import KafkaConsumer,KafkaProducer
import json
import time
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
    

def send_to_dl(job,error):
    job["error"]=str(error)
    job["status"]="failed"
    producer.send("jobs.dlq",value=job)
    producer.flush()
    print(f"job send to DLQ:{job['job_id']}")

for message in consumer:
    job=message.value
    for attempt in range(1,max_attempts+1):
        try:
            process_job(job)
            print(f"job completed:{job['job_id']}")
        except Exception as error:
            print(f"attempt {attempt} failed"f"for job{job['job_id']}:{error}")
            if attempt==max_attempts:
                send_to_dl(job,error)
            else:
                time.sleep(2**(attempt-1))
