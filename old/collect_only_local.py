import os
# import mysql.connector
# import time
import aggregate_locks
import threading

query = "ALTER TABLE `test_table` ADD COLUMN val2 VARCHAR(255) DEFAULT NULL;"

ps_config = {
    "user": "julia.jablonska",
    "password": "strong_password",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "performance_schema"
}

db_config = {
    "user": "julia.jablonska",
    "password": "strong_password",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "lock_test"
}

aggregate_locks.setup_database(db_config)

lock_collection_thread = threading.Thread(
        target=aggregate_locks.collect_locks, args=(30, 0.0001, ps_config)
    )
ddl_session_thread = threading.Thread(target=aggregate_locks.ddl_session, args=(query,db_config))

lock_collection_thread.start()
ddl_session_thread.start()

ddl_session_thread.join()
lock_collection_thread.join()
