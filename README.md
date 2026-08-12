# 🏦 Ethiopia Real-Time Inter-Bank Settlement & Fraud Detection Pipeline

[![Live Demo](https://img.shields.io/badge/🌐_Live_Demo-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit)](https://nathenael11-nbe-settlement-pipeline.streamlit.app)
[![GitHub](https://img.shields.io/badge/📂_Source_Code-GitHub-181717?style=for-the-badge&logo=github)](https://github.com/Nathenael11/nbe-settlement-pipeline)
[![Python](https://img.shields.io/badge/🐍_Python-3.10+-3776AB?style=for-the-badge&logo=python)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/📨_Apache_Kafka-3.4.0-231F20?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Hadoop](https://img.shields.io/badge/🐘_Apache_Hadoop-3.3.4-66CCFF?style=for-the-badge&logo=apachehadoop)](https://hadoop.apache.org/)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Live Demo](#-live-demo)
- [System Architecture](#-system-architecture)
- [Features](#-features)
- [Technology Stack](#-technology-stack)
- [Fraud Detection Rules](#-fraud-detection-rules)
- [Design Decisions](#-design-decisions)
- [Project Structure](#-project-structure)
- [Installation & Setup](#-installation--setup)
- [Running the Pipeline](#-running-the-pipeline)
- [Dashboard Screenshots](#-dashboard-screenshots)
- [Team](#-team)
- [Acknowledgments](#-acknowledgments)

---

## 📖 Overview

This project implements an **end-to-end big data streaming pipeline** that simulates the **National Bank of Ethiopia (NBE)** receiving real-time inter-bank settlement transactions from private commercial banks including **CBE, Dashen, Awash, Bunna, Oromia, Wegagen, and Ahadu Bank**.

The system ingests high-velocity financial events via **Apache Kafka**, processes them through **three independent consumer groups** (Fraud Detection, Regulatory Audit, Settlement Aggregation), and persists batched outputs to **Hadoop HDFS** using an in-memory buffering strategy to prevent the NameNode small-files problem.

### 🎯 Key Objectives

| Objective | Description |
|-----------|-------------|
| **Real-Time Processing** | Process inter-bank transactions as they occur |
| **Fraud Detection** | Identify suspicious transactions in real-time |
| **Regulatory Compliance** | Maintain complete audit trail for all transactions |
| **Settlement Aggregation** | Group transactions for daily bank reconciliation |
| **Scalable Storage** | Store all data in HDFS with batch optimization |

---

## 🚀 Live Demo

### 🌐 Access the Live Dashboard

> **[Click here to view the live dashboard →](https://nathenael11-nbe-settlement-pipeline.streamlit.app)**

The dashboard provides real-time visualization of:
- 📊 Transaction flow between banks
- 🚨 Fraud alerts and suspicious activity
- 📋 Complete audit logs
- 📈 Transaction analytics and visualizations

---
## 🏗️ System Architecture

┌─────────────────────────────────────────────────────────────────────────────┐
│ NBE SETTLEMENT PIPELINE │
├─────────────────────────────────────────────────────────────────────────────┤
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ PRODUCER │───▶│ KAFKA │───▶│ CONSUMER │ │
│ │ (Bank TX) │ │ (Topic: │ │ (3 Groups) │ │
│ │ Generator │ │ nbe_settle- │ │ │ │
│ │ │ │ ment_topic) │ │ ┌──────────┴──────────┐ │
│ └──────────────┘ └──────────────┘ │ │ │ │
│ │ ▼ ▼ │
│ │ ┌─────────┐ ┌─────────┐ │
│ │ │ FRAUD │ │ AUDIT │ │
│ │ │DETECTION│ │ RECORDER│ │
│ │ └─────────┘ └─────────┘ │
│ │ │ │ │
│ │ ▼ ▼ │
│ │ ┌─────────────────────────┐│
│ │ │ SETTLEMENT AGGREGATOR ││
│ │ └─────────────────────────┘│
│ │ │ │
│ │ ▼ │
│ │ ┌─────────────────────────┐│
│ │ │ HDFS STORAGE ││
│ │ │ ┌─────────────────────┐││
│ │ │ │ settlement_data/ │││
│ │ │ │ fraud_alerts/ │││
│ │ │ │ audit_logs/ │││
│ │ │ └─────────────────────┘││
│ │ └─────────────────────────┘│
│ │ │
└─────────────────────────────────────────────────────────────────────────────┘

---

### Data Flow Explanation

| Step | Component | Description |
|------|-----------|-------------|
| **1** | **Producer** | Generates realistic bank settlement transactions |
| **2** | **Kafka** | Acts as a distributed message queue, buffering the stream |
| **3a** | **Fraud Detection** | Analyzes transactions for suspicious patterns |
| **3b** | **Regulatory Audit** | Logs all transactions for compliance |
| **3c** | **Settlement Aggregator** | Groups transactions for bank reconciliation |
| **4** | **HDFS** | Stores batched data permanently |

---

## ✨ Features

### 🏦 Bank Transaction Simulation
- 7+ Ethiopian commercial banks
- Multiple transaction types (settlement, transfer, deposit, withdrawal)
- Realistic amounts and timestamps
- Multi-currency support (ETB, USD, EUR)

### 🚨 Real-Time Fraud Detection
- **High Amount Alert:** Flags transactions > 1,000,000 ETB
- **Failed Transaction Detection:** Monitors transaction status
- **Suspicious Pattern Analysis:** Identifies unusual patterns

### 📋 Regulatory Compliance
- Complete audit trail of ALL transactions
- Timestamped records for regulatory review
- Immutable storage in HDFS

### 📊 Interactive Dashboard
- Real-time transaction viewer
- Fraud alert monitoring
- Transaction analytics
- Bank-wise statistics
- Visualizations (pie charts, histograms)

### 💾 Smart Batching Strategy
- Configurable batch size (default: 10 messages)
- Time-based flush intervals
- Prevents the HDFS small-files problem

---

## 🛠️ Technology Stack

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| **Operating System** | Ubuntu 22.04 (WSL2) | 22.04 | Linux environment |
| **Programming Language** | Python | 3.10+ | Core logic development |
| **Stream Processing** | Apache Kafka | 3.4.0 | Message queue & streaming |
| **Coordination** | Apache Zookeeper | 3.6.3 | Kafka broker management |
| **Storage** | Apache Hadoop HDFS | 3.3.4 | Distributed file storage |
| **Web Framework** | Streamlit | Latest | Interactive dashboard |
| **Data Visualization** | Plotly, Pandas | Latest | Charts & analytics |
| **Version Control** | Git & GitHub | - | Source code management |

---

## 🚨 Fraud Detection Rules

| Rule ID | Rule Name | Description | Severity | Action |
|---------|-----------|-------------|----------|--------|
| **FRD-01** | High Amount Alert | Transaction exceeds 1,000,000 ETB | HIGH | Immediate notification |
| **FRD-02** | Failed Transaction | Transaction status = 'failed' | MEDIUM | Log for investigation |
| **FRD-03** | Rapid Transfer | Multiple high-value transfers from same bank | MEDIUM | Flag for review |

---

## 📐 Design Decisions

| Decision | Choice | Justification | Theory Connection |
|----------|--------|---------------|-------------------|
| **Partitions** | 3 | Allows parallel processing while maintaining ordering | Topics, Partitions, Offsets |
| **Batch Size** | 10 | Balances memory usage and file count | Small-files problem mitigation |
| **Flush Interval** | 5 seconds | Ensures timely data persistence | Real-time vs. throughput tradeoff |
| **Replication Factor** | 1 | Single-node development environment | Replication limitations |
| **Producer acks** | 1 | Balance between speed and reliability | Durability settings |
| **Consumer Groups** | 3 | Independent processing pipelines | Consumer group parallelism |

---

## 📁 Project Structure
nbe-settlement-pipeline/
│
├── bank_producer.py # 🏦 Transaction generator
├── bank_consumer.py # 🔄 Three consumer groups
├── streamlit_app.py # 📊 Interactive dashboard
├── requirements.txt # 📦 Python dependencies
├── README.md # 📖 This file
└── .gitignore # 🚫 Git ignore rules

### File Descriptions

| File | Description |
|------|-------------|
| **bank_producer.py** | Generates fake bank transactions with realistic data |
| **bank_consumer.py** | Three consumer groups: Fraud Detection, Regulatory Audit, Settlement Aggregator |
| **streamlit_app.py** | Web dashboard for real-time monitoring |
| **requirements.txt** | All Python dependencies |
| **README.md** | Complete project documentation |

---

## 🔧 Installation & Setup

### Prerequisites

- Ubuntu 22.04 (WSL2 on Windows)
- Java 17 (OpenJDK)
- Python 3.10+
- Apache Kafka 3.4.0
- Apache Hadoop 3.3.4

### Step 1: Clone the Repository

```bash
git clone https://github.com/Nathenael11/nbe-settlement-pipeline.git
cd nbe-settlement-pipeline
Step 2: Install Python Dependencies

pip install -r requirements.txt
Step 3: Start Zookeeper
bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
Step 4: Start Kafka Broker
bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
Step 5: Start HDFS
bash
start-dfs.sh
Step 6: Create Kafka Topic
bash
cd ~/kafka
bin/kafka-topics.sh --create \
  --topic nbe_settlement_topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
🚀 Running the Pipeline
1. Start the Producer
bash
python3 bank_producer.py --rate 5 --duration 30
2. Start the Consumer
bash
python3 bank_consumer.py
3. Launch the Dashboard
bash
streamlit run streamlit_app.py
4. Verify Data in HDFS
bash
hdfs dfs -ls /user/abulu13/settlement_data/
hdfs dfs -cat /user/abulu13/settlement_data/batch_*.json | head -5

The live dashboard is available at: https://nathenael11-nbe-settlement-pipeline.streamlit.app

Feature	Description
Transactions Viewer	View all transactions in a sortable table
Fraud Alerts	See flagged suspicious transactions
Visualizations	Pie charts and histograms of transaction data
System Status	Real-time monitoring of HDFS, Kafka, and pipeline

🙏 Acknowledgments

Apache Foundation - For Kafka and Hadoop

Streamlit - For the amazing dashboard framework

Big Data Course Instructor - For guidance and support

📜 License
This project was developed for educational purposes as part of a Big Data Analytics course assignment.

📞 Contact
Author: Nathenael Ermias

GitHub: Nathenael11

Project Link: nbe-settlement-pipeline

Live Demo: Streamlit App

⭐ If You Found This Useful
⭐ Star the repository on GitHub

🔗 Share the live demo

📧 Reach out with questions


