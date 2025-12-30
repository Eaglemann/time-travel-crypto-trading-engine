# Crypto Lakehouse: Real-Time Data Platform

A scalable, end-to-end Streaming Data Lakehouse built to capture, store, and visualize real-time cryptocurrency trade data from Binance.

## Architecture

This project implements the "Lakehouse" architecture, combining the flexibility of a Data Lake with the management features of a Data Warehouse.

### Zone 1: Producer
- **Role**: Ingests raw trade data from Binance WebSocket.
- **Tech**: Python, Kafka

### Zone 2: The Lake
- **Role**: Streams data into Iceberg tables on MinIO object storage.
- **Tech**: Apache Spark, Iceberg, Nessie, MinIO

### Zone 3: Warehouse
- **Role**: Provides SQL interface to query data in the lake.
- **Tech**: Trino, Nessie Catalog

### Zone 4: Viz
- **Role**: Visualizes trends and historical data.
- **Tech**: Apache Superset

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+ (Recommended: use 'uv' or 'venv')
- Git

### Start Infrastructure
Spin up the container cluster (Kafka, MinIO, Nessie, Trino, Superset):

```bash
docker-compose up -d --build
```

### Start Data Ingestion

**Terminal A: The Producer**  
(Connects to Binance and pushes trades to Kafka)
```bash
python ingestion/producer.py
```

**Terminal B: The Spark Stream**  
(Reads from Kafka and commits parquet files to the Lake)
```bash
python processing/spark-job.py
```

## Usage

### Accessing the Dashboard
- **URL**: http://localhost:8088
- **Login**: admin / admin

### Querying Data (SQL)
You can query data using Trino (via Superset SQL Lab or CLI).

**Connection String (Superset)**:  
`trino://admin@trino:8080/nessie/crypto`

**Sample Query**:
```sql
SELECT
  from_unixtime(timestamp / 1000) as event_time,
  symbol,
  price,
  volume
FROM binance_trades
ORDER BY timestamp DESC
LIMIT 10;
```

### Time Travel
Since we use Nessie and Iceberg, you can query the database as it looked in the past.

Query the table state as of 5 minutes ago:
```sql
SELECT count(*)
FROM nessie.crypto.binance_trades
FOR TIMESTAMP AS OF (current_timestamp - interval '5' minute);
```

## Project Structure
- `ingestion/` - Python scripts for fetching websocket data and Spark structured streaming jobs.
- `processing/` - Spark structured streaming jobs.
- `warehouse/` - Configuration for Trino and Nessie.
- `visualization/` - Custom Dockerfile for Superset (includes Trino drivers).
- `docker-compose.yml` - Infrastructure definition.
- `requirements.txt` - Python dependencies.
- `checkpoint_dir/` - Checkpoint data for streaming jobs.