import mysql.connector
from mysql.connector import Error
import pandas as pd

def connect_to_db(db_config):
    """Create a database connection."""
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print("Successfully connected to the database.")
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")

def insert_dataframe_into_table(df, table_name, db_config):
    """Inserts the given DataFrame into the specified MySQL table."""
    conn = None
    try:
        conn = connect_to_db(db_config)
        cursor = conn.cursor()
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s' for _ in df.columns])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        data_tuples = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        print(f"{cursor.rowcount} rows were inserted.")
    except Error as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def create_table_from_df(df, table_name, db_config):
    """Creates a MySQL table based on the schema of a pandas DataFrame."""
    conn = None
    try:
        conn = connect_to_db(db_config)
        cursor = conn.cursor()
        cols = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, {cols})"
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table `{table_name}` created successfully.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def delete_rows(table_name, condition, db_config):
    """Deletes rows from a table based on a condition."""
    conn = None
    try:
        conn = connect_to_db(db_config)
        cursor = conn.cursor()
        delete_query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(delete_query)
        conn.commit()
        print(f"{cursor.rowcount} rows were deleted.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def truncate_table(table_name, db_config):
    """Truncates a table."""
    conn = None
    try:
        conn = connect_to_db(db_config)
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_query)
        conn.commit()
        print(f"Table `{table_name}` has been truncated.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def create_view(view_name, base_table, condition, db_config):
    """Creates a view based on a base table and a condition."""
    conn = None
    try:
        conn = connect_to_db(db_config)
        cursor = conn.cursor()
        create_view_query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {base_table} WHERE {condition}"
        cursor.execute(create_view_query)
        conn.commit()
        print(f"View `{view_name}` has been created.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
