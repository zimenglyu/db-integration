# CPS-5721 PySpark ETL Template

This repository is a GitHub Classroom starter template.

## Assignment Goal

1. Load `dataset/historical_purchases_1.csv` into your local PostgreSQL table.
2. Implement `src/student_load_second_dataset.py` so dataset 2 is appended into the same table.
3. Push your code. GitHub Actions will run lint checks automatically.

## Project Layout

- `dataset/`: two input CSV files
- `src/config.py`: all runtime configuration from env files
- `src/load_first_dataset.py`: helper script to load dataset 1 via Spark JDBC
- `src/student_load_second_dataset.py`: student TODO implementation target
- `.env.example`: shared config template
- `.env.local`: local-only config (ignored by git)
- `.github/workflows/lint.yml`: Black + Markdown link checks

## Setup (macOS or WSL)

### 1) Install Python and Spark

Install Python 3.11+ and Spark on your machine (macOS or WSL). Ensure `python3`, `pip`, and `spark-submit` are available in your shell.

### 2) Create virtual environment and install packages

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Configure local environment

```bash
cp .env.example .env
# Update values to match your local PostgreSQL settings
```

You can also keep local overrides in `.env.local`.

## Running

### Starter data expectation

Before students implement code, the first dataset (`historical_purchases_1.csv`) should already exist in the PostgreSQL assignment table.

### Student task: load dataset 2

Edit `src/student_load_second_dataset.py` and implement `append_second_dataset`.

Then run:

```bash
source .venv/bin/activate
set -a; source .env; set +a
spark-submit --packages org.postgresql:postgresql:42.7.3 src/student_load_second_dataset.py
```

## CI Checks on GitHub

On each push or pull request, GitHub Actions runs:

- `black --check src`
- Markdown link checks
