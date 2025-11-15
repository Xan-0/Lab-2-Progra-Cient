import sqlite3
import pandas as pd

schema = """
BEGIN;

CREATE TABLE IF NOT EXISTS host (
    id INTEGER PRIMARY KEY,
    os TEXT NOT NULL,
    hostname TEXT NOT NULL,
    environment TEXT NOT NULL,
    country TEXT NOT NULL,
    node INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS log (
    id_log INTEGER PRIMARY KEY,
    id_server INTEGER NOT NULL,
    timestamp DATETIME NOT NULL,
    request_type TEXT NOT NULL,
    response_time_ms INTEGER NOT NULL,
    status_code INTEGER NOT NULL,
    user TEXT NOT NULL,
    FOREIGN KEY (id_server) REFERENCES host(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS maintenance (
    id_maintenance INTEGER PRIMARY KEY,
    id_server INTEGER NOT NULL,
    date DATETIME NOT NULL,
    type TEXT NOT NULL,
    duration_min INTEGER NOT NULL,
    technician TEXT NOT NULL,
    notes TEXT NOT NULL,
    FOREIGN KEY (id_server) REFERENCES host(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

path = ''

def create_base_tables():
    with sqlite3.connect(path) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        conn.commit()
        conn.executescript(schema)
        conn.commit()

def set_path(serv_path):
    global path
    path = serv_path

def insert_df(table_name, df):
    with sqlite3.connect(path) as conn:
        if len(df) == 0:
            return
        df.to_sql(table_name, conn, if_exists='replace')

def get_df(table_name):
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name};", conn)

def read_query(query):
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(query, conn)

def println(s):
    print(s)
    print("")