import os
# import mysql.connector
# import time
import aggregate_locks
import threading

query = "ALTER TABLE `test_table` ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;"

db_config = {
    "user": os.environ.get('MYSQL_USER'),
    "password": "strong_password",
    "host": os.environ.get('MYSQL_HOST'),
    "port": os.environ.get('MYSQL_PORT'),
    "database": os.environ.get('DATABASE')
}

aggregate_locks.setup_database(db_config)

lock_collection_thread = threading.Thread(
        target=aggregate_locks.collect_locks, args=(30, 0.0001, db_config)
    )
ddl_session_thread = threading.Thread(target=aggregate_locks.ddl_session, args=(query,db_config))

lock_collection_thread.start()
ddl_session_thread.start()

ddl_session_thread.join()
lock_collection_thread.join()
