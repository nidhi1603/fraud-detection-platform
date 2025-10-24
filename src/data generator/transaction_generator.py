"""
Transaction Generator for Fraud Detection System
Generates realistic credit card transactions with fraud patterns
"""

import random
from datetime import datetime, timedelta
from typing import Dict, List
import json
from faker import Faker

# Initialize Faker for generating realistic fake data
fake = Faker()


class TransactionGenerator:
    """
    Generates realistic credit card transactions with both normal and fraudulent patterns.

    This class simulates a real-world transaction stream where:
    - 95% of transactions are legitimate
    - 5% are fraudulent (with specific patterns)
    """

    def __init__(self, seed: int = 42):
        """
        Initialize the generator with configuration.

        Args:
            seed: Random seed for reproducibility (same seed = same data)
        """
        random.seed(seed)
        Faker.seed(seed)

        # Merchant categories (like real credit card statements)
        self.merchant_categories = [
            'grocery',
            'gas_station',
            'restaurant',
            'online_shopping',
            'electronics',
            'travel',
            'entertainment',
            'healthcare',
            'utilities',
            'education'
        ]

        # Store user profiles for consistency
        # Real users have patterns: they shop at similar places, similar amounts
        self.user_profiles = self._create_user_profiles(num_users=1000)

        # Known fraudsters (for creating fraud rings in Neo4j later)
        self.known_fraudsters = self._create_fraudster_profiles(num_fraudsters=50)

    def _create_user_profiles(self, num_users: int) -> Dict:
        """
        Create realistic user profiles with spending habits.

        Real people have patterns:
        - Student: small transactions, mostly food
        - Professional: larger transactions, travel
        - Retired: consistent small amounts
        """
        profiles = {}

        for i in range(num_users):
            user_id = f"user_{i:04d}"

            # Different user types
            user_type = random.choice(['student', 'professional', 'retired', 'business'])

            if user_type == 'student':
                avg_amount = random.uniform(10, 100)
                preferred_categories = ['grocery', 'restaurant', 'entertainment']
            elif user_type == 'professional':
                avg_amount = random.uniform(50, 500)
                preferred_categories = ['restaurant', 'travel', 'online_shopping', 'electronics']
            elif user_type == 'retired':
                avg_amount = random.uniform(20, 150)
                preferred_categories = ['grocery', 'healthcare', 'utilities']
            else:  # business
                avg_amount = random.uniform(200, 2000)
                preferred_categories = ['electronics', 'online_shopping', 'travel']

            profiles[user_id] = {
                'user_type': user_type,
                'avg_amount': avg_amount,
                'preferred_categories': preferred_categories,
                'usual_location': (float(fake.latitude()), float(fake.longitude())),
                'usual_device': fake.uuid4(),
                'card_number': fake.credit_card_number()
            }

        return profiles

    def _create_fraudster_profiles(self, num_fraudsters: int) -> List[Dict]:
        """
        Create fraudster profiles for generating fraud patterns.

        Fraudsters have different patterns than normal users:
        - Rapid transactions
        - High amounts
        - Unusual locations
        - Stolen cards
        """
        fraudsters = []

        for i in range(num_fraudsters):
            fraudsters.append({
                'fraudster_id': f"fraudster_{i:03d}",
                'stolen_cards': [fake.credit_card_number() for _ in range(random.randint(3, 10))],
                'target_categories': random.sample(self.merchant_categories, k=3),
                'operating_locations': [
                    (float(fake.latitude()), float(fake.longitude()))
                    for _ in range(random.randint(2, 5))
                ]
            })

        return fraudsters

    def generate_normal_transaction(self) -> Dict:
        """
        Generate a legitimate transaction based on user profile.

        Returns:
            Dictionary containing transaction details
        """
        # Pick a random user
        user_id = random.choice(list(self.user_profiles.keys()))
        user_profile = self.user_profiles[user_id]

        # Transaction follows user's pattern
        amount = random.gauss(user_profile['avg_amount'], user_profile['avg_amount'] * 0.3)
        amount = max(5.0, amount)  # No negative amounts!
        amount = round(amount, 2)

        # Pick from user's preferred categories
        category = random.choice(user_profile['preferred_categories'])

        # Transaction near user's usual location (with small variance)
        lat, lon = user_profile['usual_location']
        lat += random.uniform(-0.1, 0.1)  # Small location variance
        lon += random.uniform(-0.1, 0.1)

        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'amount': amount,
            'merchant_id': f"merchant_{random.randint(1, 500):04d}",
            'merchant_category': category,
            'merchant_name': fake.company(),
            'card_number': user_profile['card_number'][-4:],  # Last 4 digits only (security!)
            'device_id': user_profile['usual_device'],
            'ip_address': fake.ipv4(),
            'location_lat': round(lat, 6),
            'location_lon': round(lon, 6),
            'is_fraud': 0,
            'fraud_type': None
        }

        return transaction

    def generate_fraud_transaction(self) -> Dict:
        """
        Generate a fraudulent transaction with suspicious patterns.

        Fraud types:
        1. High amount fraud: Unusually large purchase
        2. Rapid succession: Multiple transactions in minutes
        3. Unusual location: Transaction far from usual location
        4. Stolen card: Using someone else's card
        """
        fraud_type = random.choice([
            'high_amount',
            'rapid_succession',
            'unusual_location',
            'stolen_card'
        ])

        # Pick a fraudster
        fraudster = random.choice(self.known_fraudsters)

        # Base transaction
        transaction = self.generate_normal_transaction()

        # Modify based on fraud type
        if fraud_type == 'high_amount':
            transaction['amount'] = round(random.uniform(5000, 20000), 2)
            transaction['merchant_category'] = 'electronics'  # Common high-fraud category

        elif fraud_type == 'rapid_succession':
            transaction['amount'] = round(random.uniform(500, 2000), 2)
            # In real system, we'd generate multiple transactions in seconds
            # For now, we mark this one as part of rapid succession

        elif fraud_type == 'unusual_location':
            # Transaction from different country
            new_location = random.choice(fraudster['operating_locations'])
            transaction['location_lat'] = new_location[0]
            transaction['location_lon'] = new_location[1]
            transaction['amount'] = round(random.uniform(1000, 5000), 2)

        elif fraud_type == 'stolen_card':
            # Using stolen card
            transaction['card_number'] = random.choice(fraudster['stolen_cards'])[-4:]
            transaction['device_id'] = fake.uuid4()  # Different device
            transaction['ip_address'] = fake.ipv4()  # Different IP
            transaction['amount'] = round(random.uniform(200, 3000), 2)

        # Mark as fraud
        transaction['is_fraud'] = 1
        transaction['fraud_type'] = fraud_type
        transaction['fraudster_id'] = fraudster['fraudster_id']

        return transaction

    def generate_batch(self, num_transactions: int = 100, fraud_ratio: float = 0.05) -> List[Dict]:
        """
        Generate a batch of transactions with specified fraud ratio.

        Args:
            num_transactions: Total number of transactions to generate
            fraud_ratio: Proportion of fraudulent transactions (0.05 = 5%)

        Returns:
            List of transaction dictionaries
        """
        num_fraud = int(num_transactions * fraud_ratio)
        num_normal = num_transactions - num_fraud

        transactions = []

        # Generate normal transactions
        for _ in range(num_normal):
            transactions.append(self.generate_normal_transaction())

        # Generate fraud transactions
        for _ in range(num_fraud):
            transactions.append(self.generate_fraud_transaction())

        # Shuffle so fraud is distributed randomly (like real life!)
        random.shuffle(transactions)

        return transactions

    def generate_stream(self, transactions_per_second: int = 10, fraud_ratio: float = 0.05):
        """
        Generate continuous stream of transactions (for Kafka later).

        Args:
            transactions_per_second: Rate of transaction generation
            fraud_ratio: Proportion of fraudulent transactions
        """
        print(f"Starting transaction stream: {transactions_per_second} transactions/second")
        print(f"Fraud ratio: {fraud_ratio * 100}%")
        print("Press Ctrl+C to stop\n")

        try:
            while True:
                batch = self.generate_batch(
                    num_transactions=transactions_per_second,
                    fraud_ratio=fraud_ratio
                )

                for txn in batch:
                    if txn['is_fraud'] == 1:
                        print(f"🚨 FRAUD: ${txn['amount']:.2f} - {txn['fraud_type']}")
                    else:
                        print(f"✓ Normal: ${txn['amount']:.2f} - {txn['merchant_category']}")

                # Wait 1 second before next batch
                import time
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n\nStopping transaction stream...")
            print("Stream stopped.")


# Test the generator when running this file directly
if __name__ == "__main__":
    """
    This runs when you execute: python transaction_generator.py
    It's for testing the generator before hooking it up to Kafka.
    """
    print("=" * 60)
    print("TRANSACTION GENERATOR TEST")
    print("=" * 60)

    # Create generator
    generator = TransactionGenerator()

    # Generate sample batch
    print("\n📊 Generating 20 sample transactions...\n")
    batch = generator.generate_batch(num_transactions=20, fraud_ratio=0.1)

    # Display summary
    fraud_count = sum(1 for txn in batch if txn['is_fraud'] == 1)
    normal_count = len(batch) - fraud_count

    print(f"✅ Generated {len(batch)} transactions")
    print(f"   Normal: {normal_count}")
    print(f"   Fraud: {fraud_count}")
    print("\n" + "=" * 60)
    print("Sample Transactions:")
    print("=" * 60 + "\n")

    # Show first 5 transactions
    for i, txn in enumerate(batch[:5], 1):
        fraud_indicator = "🚨 FRAUD" if txn['is_fraud'] == 1 else "✓ Normal"
        print(f"{i}. {fraud_indicator}")
        print(f"   User: {txn['user_id']}")
        print(f"   Amount: ${txn['amount']:.2f}")
        print(f"   Category: {txn['merchant_category']}")
        print(f"   Merchant: {txn['merchant_name']}")
        if txn['is_fraud'] == 1:
            print(f"   Fraud Type: {txn['fraud_type']}")
        print()

    print("=" * 60)
    print("\n✨ Generator is working! Ready to connect to Kafka.\n")