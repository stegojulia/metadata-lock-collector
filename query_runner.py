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
