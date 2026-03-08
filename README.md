# Intro to Big Data — Assignment 1
## Foursquare Dataset Analysis with PostgreSQL, Citus, ScyllaDB, and MongoDB

## Dataset Setup

Download the dataset from Yandex Disk:
**https://disk.yandex.com/d/IdEPW_IfBDs_KQ**

After downloading, place all files into the `data/` folder inside this directory:
```
assignment_1/
└── data/
    ├── my_users.csv
    ├── my_checkins_anonymized.tsv
    ├── my_friendship_before.tsv
    ├── my_friendship_after.tsv
    └── my_POIs.tsv
```

## Requirements

- Docker + Docker Compose
- Python 3.12+
- Python packages: `pip install psycopg2-binary pymongo cassandra-driver pandas matplotlib`

## How to Run

### 1. Start all databases
```bash
docker compose up -d
```

### 2. Prepare data
```bash
python3 select_my_users_slice.py
python3 prepare_data.py
```

### 3. Setup schemas
```bash
python3 setup_postgres.py
python3 setup_citus.py
python3 setup_scylladb.py
python3 setup_mongodb.py
```

### 4. Ingest data
```bash
python3 ingest_postgres.py
python3 ingest_citus.py
python3 ingest_scylladb.py
python3 ingest_mongodb.py
```

### 5. Run analytical queries
```bash
python3 queries_q1.py
python3 queries_q2.py
python3 queries_q3.py
python3 queries_q4.py
```

### 6. Generate performance charts
```bash
python3 performance_chart.py
```
