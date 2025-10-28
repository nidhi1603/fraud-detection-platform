"""
Redis Consumer - Consumes transactions from Redis Stream
Uses consumer groups for fault tolerance (like Kafka)
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


class TransactionConsumer:
    """Consumes transactions from Redis Stream"""

    def __init__(self, consumer_name='worker-1'):
        """
        Initialize Redis consumer

        Args:
            consumer_name (str): Unique name for this consumer
        """
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.stream_name = os.getenv('REDIS_STREAM_NAME', 'transactions')
        self.consumer_group = os.getenv('REDIS_CONSUMER_GROUP', 'fraud-detection-group')
        self.consumer_name = consumer_name

        # Connect to Redis
        try:
            self.client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True
            )
            self.client.ping()
            logger.info(f"✓ Connected to Redis at {self.redis_host}:{self.redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"✗ Failed to connect to Redis: {e}")
            raise

        # Create consumer group (if doesn't exist)
        self._create_consumer_group()

    def _create_consumer_group(self):
        """Create consumer group for distributed processing"""
        try:
            self.client.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id='0',
                mkstream=True
            )
            logger.info(f"✓ Created consumer group: {self.consumer_group}")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"✓ Consumer group already exists: {self.consumer_group}")
            else:
                raise

    def consume_messages(self, count=10, block_ms=1000):
        """
        Consume messages from stream

        Args:
            count (int): Number of messages to read at once
            block_ms (int): Milliseconds to wait for new messages

        Returns:
            list: List of (message_id, transaction) tuples
        """
        try:
            # Read from consumer group
            messages = self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_name: '>'},  # '>' means only new messages
                count=count,
                block=block_ms
            )

            if not messages:
                return []

            # Parse messages
            parsed_messages = []
            for stream_name, message_list in messages:
                for message_id, data in message_list:
                    transaction = json.loads(data['data'])
                    parsed_messages.append((message_id, transaction))

            return parsed_messages

        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return []

    def acknowledge_message(self, message_id):
        """
        Acknowledge message processing (removes from pending)

        Args:
            message_id (str): Redis message ID
        """
        try:
            self.client.xack(self.stream_name, self.consumer_group, message_id)
        except Exception as e:
            logger.error(f"Error acknowledging message {message_id}: {e}")

    def process_transaction(self, transaction):
        """
        Process a single transaction
        Override this method for custom processing

        Args:
            transaction (dict): Transaction data
        """
        # Default: just log it
        logger.info(f"Processing transaction: {transaction.get('transaction_id', 'unknown')}")

        # TODO: Add fraud detection logic here
        # - Feature engineering
        # - ML prediction
        # - Save to database

        return transaction

    def start_consuming(self, batch_size=100):
        """
        Start consuming messages in a loop

        Args:
            batch_size (int): Messages to process per batch
        """
        logger.info(f"🚀 Starting consumer: {self.consumer_name}")
        logger.info(f"📊 Consumer group: {self.consumer_group}")
        logger.info(f"⚡ Batch size: {batch_size}")

        processed_count = 0

        try:
            while True:
                # Consume batch of messages
                messages = self.consume_messages(count=batch_size)

                if not messages:
                    # No new messages, wait a bit
                    logger.info("No new messages, waiting...")
                    time.sleep(1)
                    continue

                # Process each message
                for message_id, transaction in messages:
                    try:
                        # Process transaction
                        self.process_transaction(transaction)

                        # Acknowledge successful processing
                        self.acknowledge_message(message_id)

                        processed_count += 1

                        if processed_count % 100 == 0:
                            logger.info(f"✓ Processed {processed_count} transactions")

                    except Exception as e:
                        logger.error(f"Error processing message {message_id}: {e}")
                        # Don't acknowledge - message will be retried

        except KeyboardInterrupt:
            logger.info(f"🛑 Consumer stopped. Processed {processed_count} transactions")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise

    def get_pending_messages(self):
        """Get messages that were consumed but not acknowledged"""
        pending = self.client.xpending(self.stream_name, self.consumer_group)
        return pending

    def close(self):
        """Close Redis connection"""
        self.client.close()
        logger.info("Redis connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize consumer
    consumer = TransactionConsumer(consumer_name='worker-1')

    # Start consuming (runs until Ctrl+C)
    consumer.start_consuming(batch_size=100)