"""The shop's own data: a tiny SQLite table of orders (separate from scheduleFire's job storage)."""

import os
import sqlite3
import threading
import time

_local = threading.local()


def _db_path() -> str:
    return os.environ.get("SHOP_DB", os.path.join("data", "shop.db"))


def _conn() -> sqlite3.Connection:
    path = _db_path()
    conn = getattr(_local, "conn", None)
    if conn is None or getattr(_local, "path", None) != path:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        conn = sqlite3.connect(path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            payment_attempts INTEGER NOT NULL DEFAULT 0,
            confirmation_sent INTEGER NOT NULL DEFAULT 0,
            invoice TEXT,
            review_requested INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL)""")
        _local.conn, _local.path = conn, path
    return conn


def create_order(email: str, amount: float) -> int:
    cur = _conn().execute("INSERT INTO orders (email, amount, created_at) VALUES (?, ?, ?)",
                          (email, amount, time.time()))
    return int(cur.lastrowid)


def get(order_id: int) -> dict:
    row = _conn().execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if row is None:
        raise LookupError(f"order {order_id} does not exist")
    return dict(row)


def all_orders() -> list:
    return [dict(r) for r in _conn().execute("SELECT * FROM orders ORDER BY id DESC")]


def update(order_id: int, **fields) -> None:
    allowed = {"status", "confirmation_sent", "invoice", "review_requested"}
    if not fields or set(fields) - allowed:
        raise ValueError(f"cannot update {sorted(fields)}")
    assignments = ", ".join(f"{name} = ?" for name in fields)
    _conn().execute(f"UPDATE orders SET {assignments} WHERE id = ?", (*fields.values(), order_id))


def add_payment_attempt(order_id: int) -> int:
    conn = _conn()
    conn.execute("UPDATE orders SET payment_attempts = payment_attempts + 1 WHERE id = ?", (order_id,))
    return int(conn.execute("SELECT payment_attempts FROM orders WHERE id = ?", (order_id,)).fetchone()[0])


def sales_summary() -> dict:
    row = _conn().execute(
        "SELECT COUNT(*) AS orders, COALESCE(SUM(CASE WHEN status IN ('paid', 'invoiced') THEN amount END), 0) AS revenue "
        "FROM orders").fetchone()
    return {"orders": row["orders"], "revenue": round(row["revenue"], 2)}
