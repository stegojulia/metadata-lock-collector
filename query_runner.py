import threading
import time
from database import get_connection
from config import DB_CONFIG
from lock_collector import LockCollector

def run_query(query, delay):
    time.sleep(delay)
    conn = get_connection(DB_CONFIG)
    cursor = conn.cursor()
    try:
        print(f"Executing query: {query}")
        cursor.execute(query)
        if "INSERT" in query.upper():
            time.sleep(10)  # Hold the transaction open for 10 seconds
            print(f"Rolling back: {query}")
            conn.rollback()
        else:
            conn.commit()
    except Exception as e:
        print(f"Error executing query: {query}")
        print(f"Error message: {str(e)}")
        conn.rollback()
        print("Transaction rolled back.")
    finally:
        cursor.close()
        conn.close()

def run_queries_with_locks(queries):
    lock_collector = LockCollector()
    lock_collector.start_collecting()

    threads = []
    for i, query in enumerate(queries):
        delay = i * 2  # 2 second delay between starting each query
        thread = threading.Thread(target=run_query, args=(query, delay))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    lock_collector.stop_collecting()
    return lock_collector.get_locks()

def session_1(query, config=DB_CONFIG):
    conn = get_connection(config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        print(f"Executing S1 query: {query}")
        cursor.execute(query)
        time.sleep(12)  # Keep the transaction open for 12 seconds
        print("Rolling back S1...")
        cursor.execute("ROLLBACK;")
    finally:
        print("Closing S1")
        cursor.close()
        conn.close()

def ddl_session(query, config=DB_CONFIG):
    time.sleep(2)  # Wait to make sure session 1 starts first
    conn = get_connection(config)
    cursor = conn.cursor()
    try:
        print(f"Executing DDL query: {query}")
        cursor.execute(query)
        time.sleep(10)
    finally:
        print("Closing DDL session")
        cursor.close()
        conn.close()

def dml_session(query, config=DB_CONFIG):
    time.sleep(5)  # Wait to make sure session 2 starts first
    conn = get_connection(config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        print(f"Executing S3 query: {query}")
        cursor.execute(query)
        result = cursor.fetchall()
        if result:
            print(result)
        time.sleep(8)  # Keep the transaction open for 8 seconds
    finally:
        print("Rolling back S3")
        cursor.execute("ROLLBACK;")
        print("Closing S3")
        cursor.close()
        conn.close()
