from confluent_kafka import Consumer, KafkaException
import sys

bootstrap_servers = 'kafka:9092'
group_id = 'my_consumer_group'  # Change this to your desired consumer group ID

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

def consume_from_kafka():
    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe(['test_topic'])  # Subscribe to the 'test_topic'

        while True:
            message = consumer.poll(timeout=1.0)  # Poll for messages with a timeout of 1 second

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, the consumer has reached the end of the topic
                    continue
                else:
                    # Handle other Kafka-related errors
                    raise KafkaException(message.error())
            else:
                # Process the received message
                print('Received message: {}'.format(message.value().decode('utf-8')))

    except KeyboardInterrupt:
        # Handle keyboard interrupt
        sys.stderr.write('Aborted by user\n')

    finally:
        # Clean up
        consumer.close()

# Start consuming messages
consume_from_kafka()

