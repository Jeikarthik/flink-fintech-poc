# Pre-Delinquency Intervention Engine — Feature Engineering & Storage

A real-time financial risk detection pipeline built with **Apache Flink**, **Kafka**, **PostgreSQL**, **Redis**, **Feast**, **Spark**, **Airflow**, and **FastAPI**. The system proactively identifies customers at risk of pre-delinquency by analyzing transaction streams and computing behavioral features in real time.

## Architecture

```
┌──────────────┐     ┌─────────┐     ┌──────────────────────────┐     ┌───────┐     ┌─────────────┐
│  Transaction │────▶│  Kafka  │────▶│  Flink Feature Engine    │────▶│ Redis │────▶│  FastAPI     │
│  Simulator   │     │ topics  │     │  (enrich + 7d windows)   │     │(Feast)│     │  Scoring    │
└──────────────┘     └─────────┘     └──────────────────────────┘     └───────┘     └─────────────┘
                                              │                            ▲
                                     ┌────────▼────────┐          ┌───────┴───────┐
                                     │   PostgreSQL    │          │   Airflow     │
                                     │ (offline store) │◀─────────│ (materialize) │
                                     └────────▲────────┘          └───────────────┘
                                              │
                                     ┌────────┴────────┐
                                     │   Spark Batch   │
                                     │ (nightly jobs)  │
                                     └─────────────────┘
```

## Project Structure

```
flink-fintech-poc/
├── docker/
│   ├── docker-compose.yml        # Full stack: Kafka, Postgres, Flink, Redis, Spark, Airflow, Scoring
│   ├── init-db.sql               # DB schema (7 tables + 100 MCC seed records)
│   └── flink/Dockerfile          # Flink image with Kafka connector
├── flink-jobs/                   # Java — Flink streaming jobs
│   ├── pom.xml
│   └── src/main/java/flinkfintechpoc/
│       ├── jobs/
│       │   ├── FeatureEngineeringJob.java    # Core pipeline: Kafka → Enrich → Aggregate → Redis
│       │   ├── CustomerAnalyticsJob.java     # Demo: customer count per minute
│       │   └── CustomerDebeziumJob.java      # Demo: CDC event processing
│       ├── models/
│       │   ├── TransactionEvent.java         # Raw banking transaction
│       │   ├── AccountUpdateEvent.java       # Account balance change
│       │   ├── EnrichedTransaction.java      # Transaction + merchant risk
│       │   └── CustomerFeatureVector.java    # 7-day rolling features
│       ├── functions/
│       │   ├── MerchantRiskEnricher.java     # Broadcast join with risk lookup
│       │   ├── FeatureAggregator.java        # Keyed state + timers for rolling windows
│       │   └── RedisFeatureSink.java         # Flink 2.0 Sink2 API → Redis
│       └── simulator/
│           └── TransactionSimulatorJob.java  # Seeds DB + streams events to Kafka
├── feast/                        # Python — Feature store definitions
│   └── feature_repo/
│       ├── feature_store.yaml    # Redis online + Postgres offline
│       └── features.py           # Entity, FeatureViews, FeatureService
├── spark/
│   └── batch_features.py         # PySpark: salary delay, utility delay, spend trend
├── airflow/
│   └── dags/
│       └── batch_features_dag.py # Daily: Spark batch → Feast materialize
├── scoring-service/              # Python — FastAPI risk scoring
│   ├── main.py                   # /score/{id}, /features/{id}, /health
│   ├── Dockerfile
│   └── requirements.txt
├── app/                          # Legacy Clojure simulator (original POC)
└── mise.toml                     # Task runner config
```

## Features Computed

### Real-Time (Flink — 7-day sliding window)
| Feature | Description |
|---------|-------------|
| `discretionary_spend_7d` | Sum of dining/shopping/entertainment debits |
| `atm_withdrawals_count_7d` | Number of ATM withdrawals |
| `lending_app_txn_count_7d` | Transactions to payday lending MCCs |
| `weighted_lending_risk_7d` | Cumulative merchant risk score for lending |
| `failed_autodebits_count_7d` | Failed auto-debit/standing order count |
| `total_txn_count_7d` | Total transaction count |
| `avg_txn_amount_7d` | Average transaction amount |

### Batch (Spark — nightly)
| Feature | Description |
|---------|-------------|
| `salary_delay_days` | Days since expected salary credit |
| `utility_payment_avg_delay_days` | Average utility bill payment delay |
| `discretionary_spend_trend` | Spend ratio vs. previous period |

## Quick Start

### Prerequisites
- Java 17+, Maven 3.8+
- Docker Desktop
- Python 3.11+ (for Feast, Spark, Scoring Service)
- [mise](https://mise.jdx.dev/) (optional, for task runner)

### 1. Start Infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

This starts: Kafka, PostgreSQL (with schema auto-init), Flink (JobManager + TaskManager), Redis, Spark (master + worker), Airflow, Kafka Connect (Debezium), Kafka UI, and the Scoring Service.

### 2. Build Flink Jobs

```bash
cd flink-jobs && mvn clean package -DskipTests
```

### 3. Run the Transaction Simulator

```bash
# Directly
java -cp flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar flinkfintechpoc.simulator.TransactionSimulatorJob

# Or via mise
mise run simulate:transactions
```

Seeds 50 customers into PostgreSQL and continuously streams realistic banking transactions to Kafka.

### 4. Submit the Feature Engineering Job

```bash
# Directly
flink run -c flinkfintechpoc.jobs.FeatureEngineeringJob flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar

# Or via mise
mise run flink:run:feature-engineering
```

### 5. Apply Feast Definitions

```bash
pip install -r feast/requirements.txt
cd feast/feature_repo && feast apply
```

### 6. Score a Customer

```bash
# Get raw features
curl http://localhost:8000/features/CUST-0001

# Get risk score
curl -X POST http://localhost:8000/score/CUST-0001
```

## Web UIs

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Flink Dashboard | http://localhost:8081 |
| Airflow | http://localhost:8082 (admin/admin) |
| Spark Master | http://localhost:8084 |
| Scoring Service | http://localhost:8000/docs |

## Available Tasks (mise)

| Task | Description |
|------|-------------|
| `docker:start:all` | Start full Docker stack |
| `flink:build` | Build Flink JAR |
| `flink:run:feature-engineering` | Run the feature engineering pipeline |
| `flink:run:customer-analytics` | Run customer analytics demo |
| `flink:run:customer-debezium` | Run CDC event processor |
| `simulate:transactions` | Run the transaction simulator |
| `feast:apply` | Register Feast feature definitions |
| `feast:materialize` | Sync offline → online store |
| `spark:batch-features` | Run Spark batch job |
| `scoring:start` | Start FastAPI locally |
| `postgres:connect` | Open psql shell |

## Database Schema

The PostgreSQL database (`outbox_demo`) contains:

- **`customers`** — Customer demographics, credit score, salary day
- **`accounts`** — Savings/current/credit accounts with balances
- **`transactions`** — Full transaction history with MCC codes
- **`merchant_risk_scores`** — 100 pre-seeded MCC risk categories
- **`customer_features_offline`** — Batch + streaming feature snapshots
- **`risk_scores`** — Predicted risk probability history
- **`interventions`** — Outreach tracking and outcomes

## Tech Stack

| Layer | Technology |
|-------|------------|
| Stream Processing | Apache Flink 2.0 (Java) |
| Message Bus | Apache Kafka (Confluent) |
| CDC | Debezium |
| Feature Store | Feast (Redis online + Postgres offline) |
| Batch Processing | Apache Spark 3.5 (PySpark) |
| Orchestration | Apache Airflow 2.8 |
| Scoring Service | FastAPI (Python) |
| Database | PostgreSQL 16 |
| Cache/Online Store | Redis 7 |
