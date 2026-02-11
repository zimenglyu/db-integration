# CPS-5721 PySpark PostgreSQL Integration

This project demonstrates how to integrate Apache PySpark with PostgreSQL. You will read CSV datasets from disk using Spark, then write the data into a PostgreSQL table over JDBC.

The repository contains two CSV files under `dataset/`. Your task is to load the data into a PostgreSQL table and implement the logic that appends the second dataset into the same table.

## Project Layout

```
├── dataset/
│   ├── historical_purchases_1.csv
│   └── historical_purchases_2.csv
├── src/
│   ├── config.py                       # Runtime configuration from .env files
│   └── student_load_second_dataset.py  # Your implementation goes here
├── .env.example                        # Environment variable template
├── .github/workflows/lint.yml          # CI: Black formatting check
└── requirements.txt
```

## Prerequisites

- Python 3.11 or later
- Java JDK 17 (required by PySpark, which runs on the JVM)
- A running PostgreSQL instance

PySpark itself is installed as a Python package via `pip` (see Environment Setup below), so you do not need to install Apache Spark separately. However, Java must be installed first.

**macOS:**

```bash
brew install openjdk@17
```

After installing, follow the Homebrew output to symlink it or add it to your PATH. You can verify with:

```bash
java -version
```

**WSL / Ubuntu:**

```bash
sudo apt update && sudo apt install openjdk-17-jdk -y
```

Verify:

```bash
java -version
```

## Environment Setup

### 1) Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

This installs PySpark, python-dotenv (for loading `.env` files), and Black (the code formatter).

### 3) Configure your environment variables

```bash
cp .env.example .env.local
```

Open `.env.local` and update the values to match your local PostgreSQL setup:

- `PG_HOST` — hostname of your PostgreSQL server (default: `localhost`)
- `PG_PORT` — port (default: `5432`)
- `PG_DATABASE` — the database name to use
- `PG_USER` — your PostgreSQL username
- `PG_PASSWORD` — your PostgreSQL password (leave blank if not set)
- `PG_TABLE` — the target table (default: `public.historical_purchases`)

The application loads `.env.local` first, then falls back to `.env`. The `.env.local` file is gitignored so your credentials stay out of version control.

## Assignment

### Step 1: Load dataset 1 into PostgreSQL

Before writing any code, make sure `historical_purchases_1.csv` is loaded into your PostgreSQL table. How you do this is up to you.

### Step 2: Append dataset 2

Edit `src/student_load_second_dataset.py` and implement the `append_second_dataset` function. Then run:

```bash
python src/student_load_second_dataset.py
```

The script will print the row count before and after your load so you can verify the append worked.

## CI

On every push and pull request, GitHub Actions runs `black --check src` to enforce consistent code formatting. Make sure your code is formatted before pushing:

```bash
black src
```
