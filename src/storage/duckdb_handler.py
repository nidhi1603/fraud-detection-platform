"""
DuckDB Handler - Analytics database for fraud detection
Lightweight alternative to ClickHouse for M3 development
"""

import duckdb
import pandas as pd
from pathlib import Path
from loguru import logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class FraudDataStore:
    """DuckDB-based data store for fraud detection analytics"""

    def __init__(self, db_path=':memory:'):
        """
        Initialize DuckDB connection

        Args:
            db_path (str): Database path (':memory:' for in-memory)
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

        logger.info(f"✓ Connected to DuckDB at {db_path}")

        # Create tables
        self._create_tables()

    def _create_tables(self):
        """Create database schema"""

        # Main transactions table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR PRIMARY KEY,
                user_id VARCHAR,
                amount DECIMAL(10,2),
                merchant VARCHAR,
                category VARCHAR,
                timestamp TIMESTAMP,
                is_fraud BOOLEAN,
                fraud_score DECIMAL(3,2),
                device_id VARCHAR,
                location VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Fraud alerts table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                alert_id VARCHAR PRIMARY KEY,
                transaction_id VARCHAR,
                alert_type VARCHAR,
                severity VARCHAR,
                details VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
            )
        """)

        logger.info("✓ Database schema created")

    def insert_transaction(self, transaction):
        """
        Insert single transaction

        Args:
            transaction (dict): Transaction data
        """
        try:
            self.conn.execute("""
                INSERT INTO transactions 
                (transaction_id, user_id, amount, merchant, category, 
                 timestamp, is_fraud, fraud_score, device_id, location)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                transaction.get('transaction_id'),
                transaction.get('user_id'),
                transaction.get('amount'),
                transaction.get('merchant'),
                transaction.get('category'),
                transaction.get('timestamp'),
                transaction.get('is_fraud', False),
                transaction.get('fraud_score', 0.0),
                transaction.get('device_id'),
                transaction.get('location')
            ])

        except Exception as e:
            logger.error(f"Error inserting transaction: {e}")

    def insert_batch(self, transactions_df):
        """
        Insert batch of transactions from DataFrame

        Args:
            transactions_df (pd.DataFrame): Transactions dataframe
        """
        try:
            # Register DataFrame as temporary table
            self.conn.register('temp_transactions', transactions_df)

            # Insert from temp table
            self.conn.execute("""
                INSERT INTO transactions 
                SELECT * FROM temp_transactions
            """)

            # Unregister temp table
            self.conn.unregister('temp_transactions')

            logger.info(f"✓ Inserted {len(transactions_df)} transactions")

        except Exception as e:
            logger.error(f"Error inserting batch: {e}")

    def get_fraud_stats(self):
        """Get fraud statistics"""
        result = self.conn.execute("""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
                ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate,
                ROUND(AVG(amount), 2) as avg_amount,
                ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) as fraud_amount
            FROM transactions
        """).fetchdf()

        return result

    def get_fraud_by_hour(self):
        """Get fraud rate by hour of day"""
        result = self.conn.execute("""
            SELECT 
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as total_txns,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
                ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate
            FROM transactions
            GROUP BY hour
            ORDER BY hour
        """).fetchdf()

        return result

    def get_top_merchants(self, limit=10):
        """Get top merchants by transaction volume"""
        result = self.conn.execute(f"""
            SELECT 
                merchant,
                COUNT(*) as transaction_count,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
                ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate,
                ROUND(SUM(amount), 2) as total_amount
            FROM transactions
            GROUP BY merchant
            ORDER BY transaction_count DESC
            LIMIT {limit}
        """).fetchdf()

        return result

    def get_recent_fraud(self, limit=10):
        """Get recent fraudulent transactions"""
        result = self.conn.execute(f"""
            SELECT 
                transaction_id,
                user_id,
                amount,
                merchant,
                timestamp,
                fraud_score
            FROM transactions
            WHERE is_fraud = true
            ORDER BY timestamp DESC
            LIMIT {limit}
        """).fetchdf()

        return result

    def get_user_stats(self, user_id):
        """Get statistics for specific user"""
        result = self.conn.execute("""
            SELECT 
                user_id,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
                ROUND(AVG(amount), 2) as avg_amount,
                MIN(timestamp) as first_transaction,
                MAX(timestamp) as last_transaction
            FROM transactions
            WHERE user_id = ?
            GROUP BY user_id
        """, [user_id]).fetchdf()

        return result

    def export_to_parquet(self, output_path):
        """
        Export transactions to Parquet file

        Args:
            output_path (str): Output file path
        """
        try:
            self.conn.execute(f"""
                COPY transactions 
                TO '{output_path}' 
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            logger.info(f"✓ Exported to {output_path}")

        except Exception as e:
            logger.error(f"Error exporting to Parquet: {e}")

    def load_from_parquet(self, parquet_path):
        """
        Load transactions from Parquet file

        Args:
            parquet_path (str): Path to Parquet file
        """
        try:
            self.conn.execute(f"""
                INSERT INTO transactions
                SELECT * FROM read_parquet('{parquet_path}')
            """)

            logger.info(f"✓ Loaded from {parquet_path}")

        except Exception as e:
            logger.error(f"Error loading from Parquet: {e}")

    def execute_query(self, query):
        """
        Execute custom SQL query

        Args:
            query (str): SQL query

        Returns:
            pd.DataFrame: Query results
        """
        return self.conn.execute(query).fetchdf()

    def close(self):
        """Close database connection"""
        self.conn.close()
        logger.info("Database connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize database
    db = FraudDataStore()

    # Example transaction
    sample_txn = {
        'transaction_id': 'txn_test_001',
        'user_id': 'user_001',
        'amount': 150.50,
        'merchant': 'Amazon',
        'category': 'shopping',
        'timestamp': '2025-10-28 19:00:00',
        'is_fraud': False,
        'fraud_score': 0.12,
        'device_id': 'device_123',
        'location': 'New York, NY'
    }

    # Insert transaction
    db.insert_transaction(sample_txn)

    # Get fraud stats
    stats = db.get_fraud_stats()
    print("\n📊 Fraud Statistics:")
    print(stats)

    # Get fraud by hour
    by_hour = db.get_fraud_by_hour()
    print("\n⏰ Fraud by Hour:")
    print(by_hour)

    # Get top merchants
    merchants = db.get_top_merchants()
    print("\n🏪 Top Merchants:")
    print(merchants)

    db.close()