"""
Redis Producer - Publishes transactions to Redis Stream
Lightweight alternative to Kafka for M3 development
"""

import redis
import json
import time
from datetime import datetime
from loguru import logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class TransactionProducer:
    """Produces transactions to Redis Stream"""

    def __init__(self):
        """Initialize Redis connection"""
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.stream_name = os.getenv('REDIS_STREAM_NAME', 'transactions')

        # Connect to Redis
        try:
            self.client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True
            )
            # Test connection
            self.client.ping()
            logger.info(f"✓ Connected to Redis at {self.redis_host}:{self.redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"✗ Failed to connect to Redis: {e}")
            raise

    def publish_transaction(self, transaction):
        """
        Publish single transaction to Redis Stream

        Args:
            transaction (dict): Transaction data

        Returns:
            str: Message ID from Redis
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in transaction:
                transaction['timestamp'] = datetime.now().isoformat()

            # Publish to stream
            message_id = self.client.xadd(
                self.stream_name,
                {'data': json.dumps(transaction)}
            )

            return message_id

        except Exception as e:
            logger.error(f"Failed to publish transaction: {e}")
            raise

    def publish_batch(self, transactions):
        """
        Publish batch of transactions efficiently

        Args:
            transactions (list): List of transaction dicts

        Returns:
            list: List of message IDs
        """
        try:
            # Use pipeline for batch operations (faster!)
            pipeline = self.client.pipeline()

            for txn in transactions:
                if 'timestamp' not in txn:
                    txn['timestamp'] = datetime.now().isoformat()

                pipeline.xadd(
                    self.stream_name,
                    {'data': json.dumps(txn)}
                )

            # Execute all at once
            message_ids = pipeline.execute()
            logger.info(f"✓ Published {len(transactions)} transactions")

            return message_ids

        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            raise

    def get_stream_length(self):
        """Get current number of messages in stream"""
        return self.client.xlen(self.stream_name)

    def get_stream_info(self):
        """Get detailed stream information"""
        return self.client.xinfo_stream(self.stream_name)

    def close(self):
        """Close Redis connection"""
        self.client.close()
        logger.info("Redis connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize producer
    producer = TransactionProducer()

    # Example transaction
    sample_transaction = {
        'transaction_id': 'txn_12345',
        'user_id': 'user_001',
        'amount': 150.50,
        'merchant': 'Amazon',
        'category': 'shopping',
        'is_fraud': False
    }

    # Publish single transaction
    msg_id = producer.publish_transaction(sample_transaction)
    print(f"Published transaction with ID: {msg_id}")

    # Publish batch
    batch = [
        {'transaction_id': f'txn_{i}', 'amount': i * 10.0, 'is_fraud': False}
        for i in range(10)
    ]
    producer.publish_batch(batch)

    # Check stream
    print(f"Stream length: {producer.get_stream_length()}")

    producer.close()