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

**macOS (Homebrew):**

```bash
brew install openjdk@17
```

Then add Java to your PATH and set `JAVA_HOME`. Add these lines to your `~/.zshrc` (or `~/.bashrc`):

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
```

Restart your terminal or run `source ~/.zshrc`, then verify:

```bash
java -version
```

**WSL / Ubuntu:**

```bash
sudo apt update && sudo apt install openjdk-17-jdk -y
```

Java should be on your PATH automatically. Verify:

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

**Note:** If you installed PostgreSQL via Homebrew on macOS, the default user is your macOS username (not `postgres`), and no password is required. Set `PG_USER` to your system username and leave `PG_PASSWORD` blank.

The application loads `.env.local` first, then falls back to `.env`. The `.env.local` file is gitignored so your credentials stay out of version control.

## Assignment

### Step 1: Load dataset 1 into PostgreSQL

Before writing any code, make sure `historical_purchases_1.csv` is loaded into your PostgreSQL table. How you do this is up to you.

### Step 2: Append dataset 2

Edit `src/student_load_second_dataset.py` and implement the `append_second_dataset` function. Then run:

```bash
python src/student_load_second_dataset.py
```

The script will print the row count before and after your load so you can verify the append worked. Expected counts:

- Dataset 1: 20 rows
- Dataset 2: 80 rows
- Total after append: 100 rows

## CI

On every push and pull request, GitHub Actions runs `black --check src` to enforce consistent code formatting. Make sure your code is formatted before pushing:

```bash
black src
```
