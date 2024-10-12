import argparse
import re
from database import setup_database, cleanup_database
from query_runner import run_queries_with_locks
from utils import format_locks
from lock_collector import collect_locks_continuously

def is_dml_query(query):
    return re.match(r'^\s*(INSERT|UPDATE|DELETE)', query.strip().upper()) is not None

def is_ddl_query(query):
    return re.match(r'^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME)', query.strip().upper()) is not None

def validate_scenario_queries(queries):
    if len(queries) != 3:
        raise ValueError("Exactly three queries must be provided for scenario mode: DML, DDL, DML")
    if not is_dml_query(queries[0]) or not is_ddl_query(queries[1]) or not is_dml_query(queries[2]):
        raise ValueError("Queries for scenario mode must be in the order: DML, DDL, DML")

def run_scenario(queries, with_parent=False):
    validate_scenario_queries(queries)
    setup_database(with_parent)
    
    print("Running queries...")
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
    parser = argparse.ArgumentParser(description="Run queries and collect metadata locks.")
    parser.add_argument("--mode", choices=["scenario", "continuous"], required=True, help="Run mode")
    parser.add_argument("--queries", nargs="*", help="Queries to execute in scenario mode (3 required: DML, DDL, DML)")
    parser.add_argument("--with-parent", action="store_true", help="Set up database with parent table (scenario mode only)")
    args = parser.parse_args()

    if args.mode == "scenario":
        if not args.queries:
            parser.error("--queries is required when mode is 'scenario'")
        run_scenario(args.queries, args.with_parent)
    elif args.mode == "continuous":
        if args.queries or args.with_parent:
            parser.error("--queries and --with-parent are not applicable in continuous mode")
        run_continuous()
