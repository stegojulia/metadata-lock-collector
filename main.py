import argparse
import re
from database import setup_database, cleanup_database
from query_runner import run_queries_with_locks
from utils import format_locks
from lock_collector import collect_locks_continuously


def get_predefined_queries(query_type):
    queries = {
        "T1_insert": "INSERT INTO test_table (id, val) VALUES (5, 'anteater')",
        "T1_update": "UPDATE test_table SET val = 'updated_anteater' WHERE id = 5",
        "T1_select": "SELECT * FROM test_table WHERE id = 5",
        "T2_create": """CREATE TABLE child_table (
    id INT PRIMARY KEY,
    test_table_id INT,
    val VARCHAR(255),
    CONSTRAINT fk_test_table FOREIGN KEY (test_table_id) REFERENCES test_table (id)
)""",
        "T2_alter": "ALTER TABLE test_table ADD COLUMN val2 VARCHAR(255) DEFAULT NULL",
        "T3_insert": "INSERT INTO test_table (id, val) VALUES (6, 'armadillo')",
        "T3_update": "UPDATE test_table SET val = 'updated_armadillo' WHERE id = 6",
        "T3_select": "SELECT * FROM test_table WHERE id = 6",
    }
    return queries.get(query_type, None)


def run_scenario(queries, with_parent=False):
    setup_database(with_parent)

    print("Running queries...")
    queries = [get_predefined_queries(qt) for qt in queries]
    if None in queries:
        raise ValueError("Invalid query type provided")

    locks = run_queries_with_locks(queries)

    print("\nMetadata locks collected (in order):")
    print(format_locks(locks))

    print("Cleaning up...")
    cleanup_database()


def run_continuous():
    print("Starting continuous lock collection...")
    locks = collect_locks_continuously()
    print("\nLocks collected:")
    print(format_locks(locks))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run queries and collect metadata locks."
    )
    parser.add_argument(
        "--mode", choices=["scenario", "continuous"], required=True, help="Run mode"
    )
    parser.add_argument(
        "--query-types", nargs=3, required=True, help="Three query types to run"
    )

    parser.add_argument(
        "--with-parent",
        action="store_true",
        help="Set up database with parent table (scenario mode only)",
    )
    args = parser.parse_args()

    if args.mode == "scenario":
        if not args.query_types:
            parser.error("--query-types is required when mode is 'scenario'")
        run_scenario(args.query_types, args.with_parent)
    elif args.mode == "continuous":
        if args.query_types or args.with_parent:
            parser.error(
                "--queries and --with-parent are not applicable in continuous mode"
            )
        run_continuous()
