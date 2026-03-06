# Retail Banking Data Platform

An end-to-end **data engineering project** simulating a modern retail banking data platform.  
The system generates banking transactions, ingests operational data from PostgreSQL, and processes it into a scalable analytics warehouse using Snowflake and a medallion architecture.

This project demonstrates how real-world financial data platforms are designed for **scalable ingestion, historical tracking, and analytical modeling**.

---

# Architecture Overview

The system follows a **Medallion Architecture** with Bronze, Silver, and Gold layers.


Banking Simulator
↓
PostgreSQL (Operational DB)
↓
Python Ingestion Pipeline
↓
Snowflake Data Warehouse

    Bronze → Raw Data
    Silver → Cleaned + SCD Models
    Gold → Analytics Models

↓

BI / Analytics


---

# Architecture Diagram

<img src="docs/architecture.png" width="800">

---

# Tech Stack

| Layer | Technology |
|-----|-----|
Data Simulation | Python |
Operational Database | PostgreSQL |
Ingestion Pipeline | Python |
Data Warehouse | Snowflake |
Transformation Layer | dbt |
Analytics | SQL / BI tools |

---

# Project Components

## 1 Banking Transaction Simulator

A Python-based simulator generates realistic banking activity including:

- Salary deposits
- ATM withdrawals
- POS purchases
- Bill payments
- Transfers

The generated transactions are written into PostgreSQL.

Example tables:





