#!/usr/bin/env python3
"""
🏦 Ethiopia Inter-Bank Settlement Consumer
Three consumer groups: Fraud Detection, Regulatory Audit, Settlement Aggregator
"""

from kafka import KafkaConsumer
import json
import time
import datetime
import subprocess
import os
import threading
import logging
from collections import defaultdict
import socket

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================================================
# HDFS UTILITY
# ================================================
class HDFSWriter:
    """Handles writing data to HDFS"""
    
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.ensure_directory()
    
    def ensure_directory(self):
        """Create HDFS directory if it doesn't exist"""
        try:
            subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', self.base_dir], 
                          capture_output=True, check=False)
            logger.info(f"📁 HDFS directory ready: {self.base_dir}")
        except Exception as e:
            logger.error(f"Error creating HDFS directory: {e}")
    
    def write_batch(self, filename, data):
        """Write batch data to HDFS"""
        hdfs_path = f'{self.base_dir}/{filename}'
        
        # Write to temp file first
        temp_file = f'/tmp/{filename}'
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(data)
        
        # Upload to HDFS
        result = subprocess.run(
            ['hdfs', 'dfs', '-put', '-f', temp_file, hdfs_path],
            capture_output=True, text=True
        )
        
        os.remove(temp_file)
        
        if result.returncode == 0:
            return True
        else:
            logger.error(f"Error writing to HDFS: {result.stderr}")
            return False

# ================================================
# CONSUMER GROUP 1: SETTLEMENT AGGREGATOR
# ================================================
class SettlementAggregator:
    """Aggregates settlement transactions by bank"""
    
    def __init__(self, group_id='settlement_aggregator', batch_size=10, flush_interval=5):
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = time.time()
        self.hdfs_writer = HDFSWriter('/user/abulu13/settlement_data')
        self.stats = defaultdict(int)
        
        logger.info(f"📊 Initializing Settlement Aggregator (group: {group_id})")
        
        self.consumer = KafkaConsumer(
            'nbe_settlement_topic',
            bootstrap_servers='localhost:9092',
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
    
    def process(self):
        """Process messages"""
        logger.info("📊 Settlement Aggregator listening...")
        
        try:
            for message in self.consumer:
                tx = message.value
                self.buffer.append(tx)
                self.stats[tx['source_bank']] += 1
                
                if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush) >= self.flush_interval:
                    self.flush()
        except KeyboardInterrupt:
            self.flush()
            logger.info("📊 Settlement Aggregator stopped")
    
    def flush(self):
        """Flush buffer to HDFS"""
        if not self.buffer:
            return
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
        filename = f'settlement_batch_{timestamp}.json'
        
        data = '\n'.join(json.dumps(tx) for tx in self.buffer)
        
        if self.hdfs_writer.write_batch(filename, data):
            logger.info(f"✅ Settlement: Wrote {len(self.buffer)} transactions to HDFS")
        
        self.buffer = []
        self.last_flush = time.time()
    
    def get_stats(self):
        """Get processing statistics"""
        return dict(self.stats)

# ================================================
# CONSUMER GROUP 2: FRAUD DETECTION
# ================================================
class FraudDetector:
    """Detects suspicious transactions"""
    
    def __init__(self, group_id='fraud_detector', batch_size=10, flush_interval=5):
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = time.time()
        self.hdfs_writer = HDFSWriter('/user/abulu13/fraud_alerts')
        self.alert_count = 0
        
        logger.info(f"🚨 Initializing Fraud Detector (group: {group_id})")
        
        self.consumer = KafkaConsumer(
            'nbe_settlement_topic',
            bootstrap_servers='localhost:9092',
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
    
    def process(self):
        """Process messages"""
        logger.info("🚨 Fraud Detector listening...")
        
        try:
            for message in self.consumer:
                tx = message.value
                alerts = self.analyze_transaction(tx)
                
                if alerts:
                    self.buffer.extend(alerts)
                    self.alert_count += len(alerts)
                    
                if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush) >= self.flush_interval:
                    self.flush()
        except KeyboardInterrupt:
            self.flush()
            logger.info("🚨 Fraud Detector stopped")
    
    def analyze_transaction(self, tx):
        """Analyze transaction for fraud rules"""
        alerts = []
        
        # Rule 1: High amount
        if tx['currency'] == 'ETB' and tx['amount'] > 1000000:
            alerts.append({
                'alert_type': 'HIGH_AMOUNT',
                'severity': 'HIGH',
                'transaction': tx,
                'reason': f"Transaction amount {tx['amount']} {tx['currency']} exceeds threshold",
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Rule 2: Rapid transfers between same banks
        # (Simplified - would need state tracking in real system)
        
        # Rule 3: Failed transactions
        if tx['status'] == 'failed':
            alerts.append({
                'alert_type': 'FAILED_TRANSACTION',
                'severity': 'MEDIUM',
                'transaction': tx,
                'reason': f"Transaction failed: {tx.get('description', 'Unknown reason')}",
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return alerts
    
    def flush(self):
        """Flush alerts to HDFS"""
        if not self.buffer:
            return
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
        filename = f'fraud_alerts_{timestamp}.json'
        
        data = '\n'.join(json.dumps(alert) for alert in self.buffer)
        
        if self.hdfs_writer.write_batch(filename, data):
            logger.info(f"🚨 Fraud: Wrote {len(self.buffer)} alerts to HDFS")
        
        self.buffer = []
        self.last_flush = time.time()
    
    def get_stats(self):
        """Get processing statistics"""
        return {'alert_count': self.alert_count}

# ================================================
# CONSUMER GROUP 3: REGULATORY AUDIT
# ================================================
class RegulatoryAudit:
    """Logs all transactions for regulatory compliance"""
    
    def __init__(self, group_id='regulatory_audit', batch_size=10, flush_interval=5):
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = time.time()
        self.hdfs_writer = HDFSWriter('/user/abulu13/audit_logs')
        self.record_count = 0
        
        logger.info(f"📋 Initializing Regulatory Audit (group: {group_id})")
        
        self.consumer = KafkaConsumer(
            'nbe_settlement_topic',
            bootstrap_servers='localhost:9092',
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
    
    def process(self):
        """Process messages"""
        logger.info("📋 Regulatory Audit listening...")
        
        try:
            for message in self.consumer:
                tx = message.value
                
                audit_record = {
                    'audit_id': f'AUDIT-{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}-{random.randint(1000, 9999)}',
                    'audit_timestamp': datetime.datetime.now().isoformat(),
                    'audit_status': 'logged',
                    'audit_source': socket.gethostname(),
                    'transaction': tx
                }
                
                self.buffer.append(audit_record)
                self.record_count += 1
                
                if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush) >= self.flush_interval:
                    self.flush()
        except KeyboardInterrupt:
            self.flush()
            logger.info("📋 Regulatory Audit stopped")
    
    def flush(self):
        """Flush audit records to HDFS"""
        if not self.buffer:
            return
        
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
        filename = f'audit_batch_{timestamp}.json'
        
        data = '\n'.join(json.dumps(record) for record in self.buffer)
        
        if self.hdfs_writer.write_batch(filename, data):
            logger.info(f"📋 Audit: Wrote {len(self.buffer)} records to HDFS")
        
        self.buffer = []
        self.last_flush = time.time()
    
    def get_stats(self):
        """Get processing statistics"""
        return {'record_count': self.record_count}

# ================================================
# MAIN
# ================================================
if __name__ == '__main__':
    import random
    
    logger.info("🏦 Starting NBE Settlement Pipeline...")
    logger.info("=" * 60)
    
    aggregator = SettlementAggregator()
    fraud = FraudDetector()
    audit = RegulatoryAudit()
    
    # Run in separate threads
    threads = [
        threading.Thread(target=aggregator.process, name='Aggregator'),
        threading.Thread(target=fraud.process, name='FraudDetector'),
        threading.Thread(target=audit.process, name='Audit')
    ]
    
    for t in threads:
        t.daemon = True
        t.start()
        time.sleep(0.5)  # Stagger starts
    
    logger.info("✅ All consumer groups started! Press Ctrl+C to stop...")
    
    try:
        while True:
            time.sleep(10)
            # Print stats
            stats = {
                'settlement': aggregator.get_stats(),
                'fraud': fraud.get_stats(),
                'audit': audit.get_stats()
            }
            # Keep alive
    except KeyboardInterrupt:
        logger.info("⏹️ Shutting down...")
        aggregator.flush()
        fraud.flush()
        audit.flush()
        logger.info("✅ All consumer groups stopped")
