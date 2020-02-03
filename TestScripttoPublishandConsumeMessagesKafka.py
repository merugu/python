import ssl
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

ssl.match_hostname = lambda cert, hostname: True
mytopic='topic_name'
mygroupid='group_name'
mytopicpartitions = 10

consumer = KafkaConsumer(bootstrap_servers='your bootstrap server',
                          group_id=mygroupid,
                          security_protocol='SSL',
                          ssl_check_hostname=True,
                          ssl_cafile='caroot.pem',
                          ssl_certfile='certificate.pem',
                          ssl_keyfile='key.pem')

producer = KafkaProducer(bootstrap_servers='your bootstrap server',
                          security_protocol='SSL',
                          ssl_check_hostname=True,
                          ssl_cafile='caroot.pem',
                          ssl_certfile='certificate.pem',
                          ssl_keyfile='key.pem')

# Write a hello world to the topic
producer.send(mytopic, bytes("Hello World","utf8"))
producer.flush()

# Set topic and partition
topicPartition=[TopicPartition(mytopic, p) for p in range(0,mytopicpartitions)]
consumer.assign(topicPartition)
consumer.seek_to_beginning()

# Read and print all messages from test topic
for msg in consumer:
    print("Message: ", msg)
