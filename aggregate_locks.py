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

collection_duration = 30
collection_interval = 0.00001
# Use a set to store unique locks based on the combination of key fields
unique_locks = set()


def setup_database(parent):
    # Drop table if it exists, then create a new one
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()

    cleanup_database()

    print("Setting up test tables...")
    if parent:
        print("Setting up parent table...")
        cursor.execute(
            """
    CREATE TABLE `parent` (
        `id` int NOT NULL AUTO_INCREMENT,
        `val` varchar(255) DEFAULT NULL,
        PRIMARY KEY (`id`)
    );                   
    """
        )
        cursor.execute(
            "INSERT INTO `parent` VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');"
        )
        print("Setting up the main table...")
        cursor.execute(
            """ CREATE TABLE `test_table` (
                `id` int NOT NULL AUTO_INCREMENT,
                `val` varchar(10) DEFAULT NULL,
                `parent_id` int DEFAULT NULL,
                PRIMARY KEY (`id`),
                CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`)
                REFERENCES `parent` (`id`)
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
                );"""
        )
    else:
        cursor.execute(
            """
        CREATE TABLE `test_table` (
            `id` int NOT NULL AUTO_INCREMENT,
            `val` varchar(255) DEFAULT NULL,
            PRIMARY KEY (`id`)
        );                   
        """
        )

    print("Adding example records...")
    cursor.execute(
        """
        INSERT INTO `test_table` (id,val) VALUES 
        (1, "one"), (2, "two"), (3, "three"), (4,"four");"""
    )
    print("Tables ready")


def check_metadata_locks_reset():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    query_metadata_locks(cursor)


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
        if lock_key not in unique_locks:
            print(lock_key)
        unique_locks.add(lock_key)


# Function to start a transaction in session 1
def session_1(query):
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        print(f"Executing S1 query: {query}")
        # cursor.execute('INSERT INTO `test_table` VALUES (5, "five");')
        cursor.execute(query)
        time.sleep(10)  # Keep the transaction open for 20 seconds
        print("Rolling back S1...")
        cursor.execute("rollback;")
    finally:
        print("Closing S1")
        cursor.close()
        conn.close()


def session_2(query):
    time.sleep(2)  # Wait to make sure session 1 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        print(f"Executing S2 query: {query}")
        cursor.execute(query)
        time.sleep(10)
    finally:
        print("Closing S2")
        cursor.close()
        conn.close()


# Function to start another transaction in session 3
def session_3(query):
    time.sleep(3)  # Wait to make sure session 2 starts first
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("START TRANSACTION;")
        print(f"Executing S3 query: {query}")
        cursor.execute(query)
        time.sleep(3)  # Keep the transaction open for 5 seconds
    finally:
        print("Rolling back S3")
        cursor.execute("rollback;")
        print("Closing S3")
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
    print("Ensuring the database is clean and ready...")
    conn = mysql.connector.connect(**table1_db_config)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS `child`;")
    cursor.execute("DROP TABLE IF EXISTS `test_table`;")
    cursor.execute("DROP TABLE IF EXISTS `parent`;")


def aggregate_locks(name, query_1, query_2, query_3, parent=False):
    print("----------------------------")
    print(f"Running the {name} scenario")
    print("----------------------------")

    global unique_locks
    unique_locks.clear()

    setup_database(parent)

    # Start the sessions in parallel using threads
    session1_thread = threading.Thread(target=session_1, args=(query_1,))
    session2_thread = threading.Thread(target=session_2, args=(query_2,))
    session3_thread = threading.Thread(target=session_3, args=(query_3,))

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


# if __name__ == "__main__":
#     main()
