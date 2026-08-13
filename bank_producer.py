#!/usr/bin/env python3
"""
🏦 Ethiopia Inter-Bank Settlement Producer
Simulates real-time settlement transactions from private banks to NBE
"""

from kafka import KafkaProducer
import json
import time
import random
import argparse
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================================================
# CONFIGURATION
# ================================================
BANKS = ['CBE', 'Dashen', 'Awash', 'Bunna', 'Oromia', 'Wegagen', 'Ahadu', 'Zemen', 'Amhara']
CURRENCIES = ['ETB', 'USD', 'EUR']
TRANSACTION_TYPES = ['settlement', 'transfer', 'withdrawal', 'deposit', 'payment', 'salary', 'loan']

# ================================================
# DATA GENERATORS
# ================================================
def generate_transaction():
    """Generate a realistic bank transaction"""
    source = random.choice(BANKS)
    destination = random.choice([b for b in BANKS if b != source] or BANKS)
    
    # 80% chance ETB, 20% foreign currency
    currency = 'ETB' if random.random() < 0.8 else random.choice(['USD', 'EUR'])
    
    # Amount based on currency
    if currency == 'ETB':
        amount = round(random.uniform(100, 5000000), 2)
    else:
        amount = round(random.uniform(100, 50000), 2)
    
    # Random status (mostly completed)
    status = random.choices(
        ['pending', 'processing', 'completed', 'failed'],
        weights=[5, 10, 80, 5]
    )[0]
    
    return {
        'transaction_id': f'TXN-{datetime.now().strftime("%Y%m%d")}-{random.randint(10000, 99999)}',
        'timestamp': int(time.time() * 1000),
        'timestamp_iso': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'source_bank': source,
        'destination_bank': destination,
        'amount': amount,
        'currency': currency,
        'amount_etb': amount if currency == 'ETB' else round(amount * 55, 2),  # Approx conversion
        'transaction_type': random.choice(TRANSACTION_TYPES),
        'status': status,
        'reference': f'REF-{random.randint(100000, 999999)}',
        'description': random.choice([
            'Salary payment', 'Invoice settlement', 'Loan disbursement',
            'Savings transfer', 'Trade payment', 'Remittance',
            'Inter-bank transfer', 'Customer withdrawal'
        ]),
        'priority': random.choices(['HIGH', 'NORMAL', 'LOW'], weights=[10, 80, 10])[0]
    }

# ================================================
# PRODUCER
# ================================================
def run_producer(topic, rate, duration):
    """Run the producer"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks=1,
        retries=3
    )
    
    logger.info(f"🏦 Starting producer for topic: {topic}")
    logger.info(f"📊 Rate: {rate} tx/sec, Duration: {duration}s")
    logger.info("=" * 60)
    
    start_time = time.time()
    count = 0
    
    try:
        while time.time() - start_time < duration:
            tx = generate_transaction()
            producer.send(topic, value=tx)
            count += 1
            
            if count % 10 == 0:
                logger.info(f"✅ Sent {count} transactions")
            
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        logger.info("⏹️ Producer stopped by user")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"✅ Done! Sent {count} transactions to Kafka topic: {topic}")

# ================================================
# MAIN
# ================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ethiopia Inter-Bank Settlement Producer')
    parser.add_argument('--topic', default='nbe_settlement_topic', help='Kafka topic')
    parser.add_argument('--rate', type=int, default=5, help='Transactions per second')
    parser.add_argument('--duration', type=int, default=30, help='Run duration in seconds')
    args = parser.parse_args()
    
    run_producer(args.topic, args.rate, args.duration)
