import mysql.connector
import time
import threading
from tabulate import tabulate

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

collection_duration = 40
collection_interval = 0.001
# Use a set to store unique locks based on the combination of key fields
unique_locks = set()


def setup_database():
    # Drop table if it exists, then create a new one
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()

    print("Setting up test tables...")
    cursor.execute("DROP TABLE IF EXISTS `test_table`;")
    cursor.execute(
        """
    CREATE TABLE `test_table` (
        `id` int NOT NULL AUTO_INCREMENT,
        `val` varchar(255) DEFAULT NULL,
        PRIMARY KEY (`id`)
    );                   
    """
    )
    cursor.execute(
        """
    INSERT INTO `test_table` VALUES 
        (1, "one"), (2, "two"), (3, "three"), (4, "four");"""
    )

    print("Test tables set up complete")


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
    AND OBJECT_SCHEMA != 'mysql';;
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
        print('S1: INSERT INTO `test_table` VALUES (5, "five");')
        cursor.execute('INSERT INTO `test_table` VALUES (5, "five");')
        time.sleep(10)  # Keep the transaction open for 20 seconds
        print("Rolling back S1...")
        cursor.execute("rollback;")
    finally:
        cursor.close()
        conn.close()


def session_2():
    time.sleep(2)  # Wait to make sure session 1 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        print("S2: ALTER")
        cursor.execute(
            "ALTER TABLE `test_table` ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;"
        )
        time.sleep(8)
    finally:
        print("S2 close")
        cursor.close()
        conn.close()


# Function to start another transaction in session 3
def session_3():
    time.sleep(4)  # Wait to make sure session 1 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        print("S3")
        cursor.execute('INSERT INTO `test_table` ( `id`,  `val`) VALUES (6, "six");')
        time.sleep(10)  # Keep the transaction open for 5 seconds
    finally:
        print("S3 close")
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


def cleanup_database():
    # Drop the table to clean up
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS `test_table`;")


def main():

    setup_database()

    # Start the sessions in parallel using threads
    session1_thread = threading.Thread(target=session_1)
    session2_thread = threading.Thread(target=session_2)
    session3_thread = threading.Thread(target=session_3)

    # Start the lock collection in parallel
    duration = collection_duration
    interval = collection_interval
    lock_collection_thread = threading.Thread(
        target=collect_locks, args=(duration, interval)
    )

    lock_collection_thread.start()
    # Start all threads
    session1_thread.start()
    session2_thread.start()
    session3_thread.start()

    # Wait for all threads to finish
    session1_thread.join()
    session2_thread.join()
    session3_thread.join()
    lock_collection_thread.join()

    cleanup_database()

    # Sorting locks by OBJECT_SCHEMA and OBJECT_NAME
    sorted_locks = sorted(
        unique_locks,
        key=lambda x: (x[6] if x[6] is not None else float("inf"), x[2] or ""),
    )

    # Prepare table data
    table_data = [
        (
            lock[0],  # OBJECT_TYPE
            lock[1],  # OBJECT_SCHEMA
            lock[2],  # OBJECT_NAME
            lock[3],  # LOCK_TYPE
            lock[4],  # LOCK_STATUS
            lock[5],  # OWNER_EVENT_ID
            lock[6],  # OWNER_THREAD_ID
        )
        for lock in sorted_locks
    ]

    # Define table headers
    headers = [
        "Object Type",
        "Object Schema",
        "Object Name",
        "Lock Type",
        "Lock Status",
        "Owner Event ID",
        "Owner Thread ID",
    ]

    # Print the table
    print(tabulate(table_data, headers=headers, tablefmt="grid"))


if __name__ == "__main__":
    main()
