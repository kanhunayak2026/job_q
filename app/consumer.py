from kafka import KafkaConsumer
import json
consumer=KafkaConsumer(
    "jobs",
    bootstrap_servers=["localhost:9092"],
    group_id="job_workers",
    auto_offset_reset="earliest",
    value_deserializer=lambda value:json.loads(value.decode("utf-8"))
)
for message in consumer:
    job=message.value
    print("recevied job:",job)


