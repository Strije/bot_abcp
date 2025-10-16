import sqlite3
from datetime import datetime
DB_NAME = "bot_data.db"
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        phone TEXT,
        user_id TEXT
    )""" )
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_number TEXT PRIMARY KEY,
        user_id TEXT,
        status TEXT,
        message_id INTEGER,
        last_update TIMESTAMP
    )""" )
    conn.commit()
    conn.close()
def add_user(telegram_id, phone, user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO users VALUES (?, ?, ?)", (telegram_id, phone, user_id))
    conn.commit()
    conn.close()
def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT telegram_id, phone, user_id FROM users")
    users = [dict(zip(["telegram_id", "phone", "user_id"], row)) for row in cur.fetchall()]
    conn.close()
    return users
def get_order_status(order_number):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT status FROM orders WHERE order_number=?", (order_number,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None
def get_order_message(order_number):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT message_id FROM orders WHERE order_number=?", (order_number,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None
def update_order_status(order_number, user_id, status, message_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO orders (order_number, user_id, status, message_id, last_update)
    VALUES (?, ?, ?, ?, ?)
    """, (order_number, user_id, status, message_id, datetime.now()))
    conn.commit()
    conn.close()
