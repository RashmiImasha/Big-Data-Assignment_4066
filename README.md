# Big-Data-Assignment_4066 - Kafka Real-time Order Processing

## Project Overview
This project demonstrates a **real-time order processing system** using **Apache Kafka**.

- Producer sends orders to Kafka
- Consumer processes orders with **retry logic**
- Failed messages go to a **Dead Letter Queue (DLQ)**
- Running average of order prices is calculated in real-time

---

**Features**
- Kafka Producer & Consumer
- Retry logic with 3 max attempts
- Dead Letter Queue (DLQ)
- Real-time price aggregation
- Manual Avro schema integration

## Setup

1. **Install dependencies**
```bash
python -m venv venv
# Activate virtual environment
# Linux/macOS
source venv/bin/activate
# Windows
venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```
2. **Start Kafka** (Docker or local setup)

## Running the Project

**Start Producer**
```bash
python producer.py
```
**Start Consumer**
```bash
python consumer.py
```








