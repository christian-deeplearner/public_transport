"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKER_URL = 'PLAINTEXT://localhost:9092'
    SCHEMA_REGISTRY_URL = 'http://localhost:8081'

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

        self.producer = AvroProducer({
            'bootstrap.servers': self.BROKER_URL,
            'on_delivery': delivery_report,
            'schema.registry.url': self.SCHEMA_REGISTRY_URL
        }, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if self.topic_name not in Producer.existing_topics:
            admin_client = AdminClient(self.broker_properties)
            new_topic = NewTopic(
                self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )

            fs = admin_client.create_topics([new_topic])

            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info("Topic {} created".format(topic))
                except Exception as e:
                    logger.error("Failed to create topic {}: {}".format(topic, e))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("\nFlushing records...")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
