# Metadata Lock Analyzer

A tool for analyzing metadata locks in MySQL databases.

## Requirements

- Python 3.9+
- Poetry
- Running MySQL server

## Setup

1. Clone the repository
2. Install dependencies: `poetry install`
3. Copy `.env.example` to `.env` and fill in your database credentials

## Usage

### Collect Mode

Collect metadata locks, optionally running a DDL query:

```bash
poetry run python main.py --mode collect --ddl "ALTER TABLE test_table ADD COLUMN new_col INT"
```

To collect locks without running any queries, omit the `--ddl` argument:

```
poetry run python main.py --mode collect
```

### Scenario Mode

Run a predefined scenario with multiple queries:

```bash
poetry run python main.py --mode scenario --query-types T1_insert T2_alter T3_insert --with-parent
```

#### Arguments for Scenario Mode:

- `--mode scenario`: Run in scenario mode.
- `--query-types`: Exactly three query types must be provided in the following order:
  1. First query type: T1 (INSERT, UPDATE, or SELECT)
  2. Second query type: T2 (CREATE or ALTER)
  3. Third query type: T3 (INSERT, UPDATE, or SELECT)
- `--with-parent`: Optional flag to set up the database with a parent table.

#### Available Query Types:

- T1_insert: Insert a record into test_table
- T1_update: Update a record in test_table
- T1_select: Select a record from test_table
- T2_create: Create a child_table with a foreign key to test_table
- T2_alter: Add a column to test_table
- T3_insert: Insert another record into test_table
- T3_update: Update another record in test_table
- T3_select: Select another record from test_table

#### Example for Scenario Mode:

```
poetry run python main.py --mode scenario --query-types T1_insert T2_alter T3_insert --with-parent
```

This command will:
1. Start a transaction and insert a record into test_table (T1)
2. Add a column to test_table (T2)
3. Start a transaction and insert another record into test_table (T3)
4. Roll back the transaction started in step 1

The `--with-parent` flag will set up the database with a parent table.

## Troubleshooting

### No locks are being collected.

Ensure that performance_schema is enabled in your MySQL server. On some MySQL versions you may also need to enable metadata lock instrumentation. Check the MySQL documentation for your version for more information.
