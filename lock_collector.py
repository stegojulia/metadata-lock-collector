import threading
import time
from config import PERFORMANCE_SCHEMA_CONFIG, COLLECTION_INTERVAL
from database import get_connection

class LockCollector:
    def __init__(self):
        self.locks = []
        self.lock = threading.Lock()
        self.is_collecting = False
        self.collection_thread = None

    def start_collecting(self):
        self.is_collecting = True
        self.collection_thread = threading.Thread(target=self._collect_locks_continuously)
        self.collection_thread.start()

    def stop_collecting(self):
        self.is_collecting = False
        if self.collection_thread:
            self.collection_thread.join()

    def _collect_locks_continuously(self):
        while self.is_collecting:
            self._collect_locks_once()
            time.sleep(COLLECTION_INTERVAL)

    def _collect_locks_once(self):
        conn = get_connection(PERFORMANCE_SCHEMA_CONFIG)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
            SELECT 
                OBJECT_TYPE, 
                OBJECT_SCHEMA, 
                OBJECT_NAME, 
                LOCK_TYPE, 
                LOCK_STATUS, 
                OWNER_EVENT_ID, 
                OWNER_THREAD_ID
            FROM performance_schema.metadata_locks
            WHERE OBJECT_SCHEMA != 'performance_schema'
            AND OBJECT_SCHEMA != 'mysql';
            """
            )

            for lock in cursor:
                with self.lock:
                    if lock not in self.locks:
                        self.locks.append(lock)
                        print(f"New lock detected: {lock}")
        finally:
            cursor.close()
            conn.close()

    def get_locks(self):
        return self.locks

def collect_locks_continuously(duration=30,ddl=None):
    collector = LockCollector()
    collector.start_collecting()
    if ddl:
        conn = get_connection()
        cursor = conn.cursor()
        print(f"Running DDL: {ddl}")
        cursor.execute(ddl)
    time.sleep(duration)
    collector.stop_collecting()
    if ddl:
        cursor.close()
        conn.close()
    return collector.get_locks()
