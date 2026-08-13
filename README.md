# 🏦 Ethiopia Inter-Bank Settlement Pipeline

An end-to-end big data streaming pipeline simulating the National Bank of Ethiopia (NBE) receiving real-time settlement transactions from private banks.

## Architecture
Bank Producer → Kafka → 3 Consumer Groups → HDFS
├── Fraud Detection
├── Regulatory Audit
└── Settlement Aggregator
## Technologies
- Apache Kafka
- Apache Zookeeper
- Hadoop HDFS
- Python
- Streamlit

## Running Locally
```bash
pip install -r requirements.txt
python3 bank_producer.py
python3 bank_consumer.py
streamlit run streamlit_app.py
