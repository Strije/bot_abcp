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

    # Если не передан message_id, сохраняем существующее значение, чтобы не терять
    # привязку к сообщению с обновлениями.
    if message_id is None:
        cur.execute("SELECT message_id FROM orders WHERE order_number=?", (order_number,))
        row = cur.fetchone()
        if row and row[0] is not None:
            message_id = row[0]

    cur.execute(
        """
    INSERT OR REPLACE INTO orders (order_number, user_id, status, message_id, last_update)
    VALUES (?, ?, ?, ?, ?)
    """,
        (order_number, user_id, status, message_id, datetime.now()),
    )
    conn.commit()
    conn.close()


def get_user_order_snapshots(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "SELECT order_number, status, message_id FROM orders WHERE user_id=?",
        (str(user_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "order_number": row[0],
            "status": row[1],
            "message_id": row[2],
        }
        for row in rows
    ]


def clear_order_message(order_number):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "UPDATE orders SET message_id=NULL WHERE order_number=?",
        (order_number,),
    )
    conn.commit()
    conn.close()


def get_user_id_by_order_number(order_number: str) -> str | None:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM orders WHERE order_number=?",
        (str(order_number),),
    )
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        return str(row[0])
    return None
