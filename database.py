import mysql.connector
from config import DB_CONFIG


def get_connection(db_config=None):
    if db_config is None:
        db_config = DB_CONFIG
    return mysql.connector.connect(**db_config)


def setup_database(with_parent=False):
    conn = get_connection()
    cursor = conn.cursor()

    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS child_table")
    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute("DROP TABLE IF EXISTS parent_table")

    if with_parent:
        print("Creating parent table")
        cursor.execute(
            """
            CREATE TABLE parent_table (
                id INT PRIMARY KEY,
                val VARCHAR(255)
            )
        """
        )

        print("Creating main table")
        cursor.execute(
            """
            CREATE TABLE test_table (
                id INT PRIMARY KEY,
                parent_id INT,
                val VARCHAR(255),
                FOREIGN KEY (parent_id) REFERENCES parent_table(id)
            )
        """
        )
    else:
        print("Creating main table only")
        cursor.execute(
            """
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            val VARCHAR(255)
        )
    """
        )

    conn.commit()
    cursor.close()
    conn.close()


def cleanup_database():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS child_table")
    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute("DROP TABLE IF EXISTS parent_table")

    conn.commit()
    cursor.close()
    conn.close()
