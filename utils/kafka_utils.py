from kafka import KafkaProducer
from typing import Any, Dict
import logging

class KafkaWrapper:
    def __init__(self, bootstrap_servers: list[str] = ['broker:29092'], config: Dict[str, Any] = None):
        default_config = {
            'max_block_ms': 5000
        }
        if config:
            default_config.update(config)
            
        logging.info("Create Kafka Producer")
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **default_config)

    def send_data(self, topic: str, value: bytes):
        try:
            future = self.producer.send(topic=topic, value=value)
            future.get(timeout=10)  # Wait for the send to complete
            logging.info(f"Message sent successfully to topic {topic}")
        except Exception as e:
            logging.error(f"Failed to send message to topic {topic}. Error: {str(e)}")

    def close(self):
        self.producer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()