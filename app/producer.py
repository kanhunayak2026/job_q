from kafka import KafkaProducer
import json
producer=KafkaProducer(bootstrap_servers=["localhost:9092"],
                       value_serializer=lambda value: json.dumps(value).encode("utf-8"))
def send_job(job):
    producer.send("jobs", value=job)
    producer.flush()