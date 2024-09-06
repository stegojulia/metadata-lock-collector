import mysql.connector
import time
import threading

# MySQL database connection
db_config = {
    "user": "root",
    "password": "strong_password",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "performance_schema",
}

table1_db_config = {
    "user": "root",
    "password": "strong_password",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "lock_test",
}

collection_duration = 30
collection_interval = 0.01
# Use a set to store unique locks based on the combination of key fields
unique_locks = set()


# Function to run the query and collect unique lock data
def query_metadata_locks(cursor):
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
    AND OBJECT_SCHEMA != 'metadata_locks';;
    """
    )

    # Process the result and add unique locks to the set
    for (
        object_type,
        object_schema,
        object_name,
        lock_type,
        lock_status,
        owner_event_id,
        owner_thread_id,
    ) in cursor:
        lock_key = (
            object_type,
            object_schema,
            object_name,
            lock_type,
            lock_status,
            owner_event_id,
            owner_thread_id,
        )
        unique_locks.add(lock_key)


# Function to start a transaction in session 1
def session_1():
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        cursor.execute('INSERT INTO `test_table` VALUES (5, "five");')
        time.sleep(10)  # Keep the transaction open for 20 seconds
        cursor.execute("ROLLBACK;")  # Rollback after 20 seconds
    finally:
        cursor.close()
        conn.close()


def session_2():
    time.sleep(1)  # Wait to make sure session 1 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE `test_table` ADD COLUMN val2 INT;")
        time.sleep(10)
    finally:
        cursor.close()
        conn.close()


# Function to start another transaction in session 3
def session_3():
    time.sleep(1)  # Wait to make sure session 1 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        cursor.execute('INSERT INTO `test_table` VALUES (6, "six");')
        time.sleep(5)  # Keep the transaction open for 5 seconds
    finally:
        cursor.close()
        conn.close()


def collect_locks(duration, interval):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        start_time = time.time()
        while time.time() - start_time < duration:
            # Query the metadata locks and collect unique data
            query_metadata_locks(cursor)
            time.sleep(interval)
    finally:
        cursor.close()
        conn.close()


def main():

    # Start the sessions in parallel using threads
    session1_thread = threading.Thread(target=session_1)
    session2_thread = threading.Thread(target=session_2)
    session3_thread = threading.Thread(target=session_3)

    # Start the lock collection in parallel
    duration = collection_duration
    interval = collection_interval  # 10 milliseconds
    lock_collection_thread = threading.Thread(
        target=collect_locks, args=(duration, interval)
    )

    # Start all threads
    session1_thread.start()
    session2_thread.start()
    session3_thread.start()
    lock_collection_thread.start()

    # Wait for all threads to finish
    session1_thread.join()
    session2_thread.join()
    session3_thread.join()
    lock_collection_thread.join()

    # Print deduplicated locks sorted by table (OBJECT_SCHEMA, OBJECT_NAME)
    sorted_locks = sorted(
        unique_locks, key=lambda x: (x[1], x[2])
    )  # Sort by OBJECT_SCHEMA, OBJECT_NAME

    print(f"Total unique locks collected: {len(sorted_locks)}")
    for lock in sorted_locks:
        print(lock)


if __name__ == "__main__":
    main()
