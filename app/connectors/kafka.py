"""
Kafka Producer for publishing FeatureEvents and ActionEvents to Confluent Cloud
"""

from confluent_kafka import Producer
from typing import Dict, Any
import json
import logging

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Kafka producer for publishing events to Confluent Cloud"""

    def __init__(self, bootstrap_servers: str, api_key: str, api_secret: str):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': api_key,
            'sasl.password': api_secret,
            'client.id': 'rec-sys-producer',
            'enable.idempotence': True,  # Exactly-once semantics
            'acks': 'all',  # Wait for all replicas
        }
        self.producer = Producer(self.config)
        logger.info(f"Kafka producer initialized: {bootstrap_servers}")

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f"Kafka delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )

    def publish_feature_event(
        self,
        request_id: int,
        event_time: int,
        feature_data: Dict[str, Any]
    ) -> None:
        """
        Publish a FeatureEvent to Kafka

        Args:
            request_id: Unique request ID
            event_time: Unix timestamp in milliseconds
            feature_data: Dict containing user_embedding, context, candidates, etc.
        """
        event = {
            "request_id": request_id,
            "event_time": event_time,
            "feature_data": json.dumps(feature_data)
        }

        self.producer.produce(
            'feature-event-topic',
            key=str(request_id).encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=self._delivery_callback
        )
        self.producer.poll(0)  # Trigger callbacks without blocking
        logger.info(f"Published FeatureEvent for request_id={request_id}")

    def publish_action_event(
        self,
        request_id: int,
        event_time: int,
        action_data: Dict[str, Any]
    ) -> None:
        """
        Publish an ActionEvent to Kafka

        Args:
            request_id: Unique request ID (same as corresponding FeatureEvent)
            event_time: Unix timestamp in milliseconds
            action_data: Dict containing user_id, product_id, action, label, etc.
        """
        event = {
            "request_id": request_id,
            "event_time": event_time,
            "action_data": json.dumps(action_data)
        }

        self.producer.produce(
            'action-event-topic',
            key=str(request_id).encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=self._delivery_callback
        )
        self.producer.poll(0)
        logger.info(f"Published ActionEvent for request_id={request_id}")

    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush pending messages

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue
        """
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush")
        return remaining

    def close(self):
        """Close the producer and flush all pending messages"""
        logger.info("Closing Kafka producer...")
        self.producer.flush()
        logger.info("Kafka producer closed")


# Singleton instance
_kafka_producer: KafkaProducer | None = None


def get_kafka_producer(settings) -> KafkaProducer:
    """
    Get or create singleton Kafka producer instance

    Args:
        settings: Application settings containing Kafka configuration

    Returns:
        KafkaProducer instance
    """
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            api_key=settings.kafka_api_key,
            api_secret=settings.kafka_api_secret
        )
    return _kafka_producer
