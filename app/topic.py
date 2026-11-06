from kafka.admin import KafkaAdminClient,NewTopic
admin_client=KafkaAdminClient(bootstrap_servers="localhost:9092",client_id="job_queue_admin")
topic=NewTopic(name="jobs",num_partitions=1,replication_factor=1)
admin_client.create_topics([topic])
print("job created sucessfully")
admin_client.close()