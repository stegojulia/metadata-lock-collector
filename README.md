# Metadata Lock Analyzer

A tool for analyzing metadata locks in MySQL databases.

## Setup

1. Clone the repository
2. Install dependencies: `poetry install`
3. Copy `.env.example` to `.env` and fill in your database credentials

## Usage

### Scenario Mode

Run a predefined scenario with multiple queries:

```bash
poetry run python main.py --mode scenario --queries "INSERT INTO test_table (id, val) VALUES (1, 'test')" "ALTER TABLE test_table ADD COLUMN new_col INT" "UPDATE test_table SET new_col = 1 WHERE id = 1"
```


### Collect Mode

Collect metadata locks, optionally running a DDL query:

```bash
poetry runpython main.py --mode collect --ddl "ALTER TABLE test_table ADD COLUMN new_col INT"
```

To collect locks without running any queries, omit the `--ddl` argument:

```bash
poetry runpython main.py --mode collect
```

#### Arguments for Scenario Mode:

- `--mode scenario`: Run in scenario mode.
- `--queries`: Exactly three queries must be provided in the following order:
  1. First query: DML (INSERT, UPDATE, or DELETE)
  2. Second query: DDL (CREATE, ALTER, DROP, TRUNCATE, or RENAME)
  3. Third query: DML (INSERT, UPDATE, or DELETE)
- `--with-parent`: Optional flag to set up the database with a parent table.

#### Example for Scenario Mode:

```poetry run python main.py --mode scenario --queries "INSERT INTO test_table (id,val) VALUES (5, 'anteater');" "ALTER TABLE test_table ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;" "INSERT INTO test_table (id,val) VALUES (6, 'armadillo');"```
