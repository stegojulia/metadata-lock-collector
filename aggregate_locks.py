import mysql.connector
import time
import collections

# MySQL database connection
db_config = {
    'user': 'root',
    'password': 'strong_password',
    'host': '127.0.0.1',
    'port': 3307,
    'database': 'performance_schema'
}
# Use a set to store unique locks based on the combination of key fields
unique_locks = set()

# Function to run the query and collect unique lock data
def query_metadata_locks(cursor):
    cursor.execute("""
    SELECT 
        OBJECT_TYPE, 
        OBJECT_SCHEMA, 
        OBJECT_NAME, 
        LOCK_TYPE, 
        LOCK_STATUS, 
        OWNER_EVENT_ID, 
        OWNER_THREAD_ID
    FROM performance_schema.metadata_locks;
    """)
    
    # Process the result and add unique locks to the set
    for (object_type, object_schema, object_name, lock_type, lock_status, owner_event_id, owner_thread_id) in cursor:
        lock_key = (object_type, object_schema, object_name, lock_type, lock_status, owner_event_id, owner_thread_id)
        unique_locks.add(lock_key)

def main():
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Set the duration and frequency
        duration = 60 * 5  # Run for 5 minutes
        # duration = 10
        interval = 0.01  #10 milliseconds (0.01 second)
        start_time = time.time()

        while time.time() - start_time < duration:
            # Query the metadata locks and collect unique data
            query_metadata_locks(cursor)

            # Wait for the next interval
            time.sleep(interval)

    finally:
        # Close the database connection
        cursor.close()
        conn.close()

    # Print the deduplicated locks
    print(f"Total unique locks collected: {len(unique_locks)}")
    for lock in unique_locks:
        print(lock)

if __name__ == "__main__":
    main()
