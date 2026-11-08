from kafka.admin import KafkaAdminClient,NewTopic
admin_client=KafkaAdminClient(bootstrap_servers="localhost:9092",client_id="job_queue_admin")
topics=NewTopic(name="jobs",num_partitions=1,replication_factor=1),NewTopic(name="jobs.dlq",num_partitions=1,replication_factor=1)
exist_topic=admin_client.list_topics()
missing_topic=[topic for topic in topics
               if topic.name not in exist_topic]
if missing_topic:
    admin_client.create_topics(
        new_topics=missing_topic,
        validate_only=False
    )
    print("new topic create sucessfully")
else:
    print("topic already exist")

admin_client.close()