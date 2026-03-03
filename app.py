import os
import sqlite3
import psycopg2
from psycopg2.extras import DictRow
from flask import Flask, jsonify, request, send_from_directory, g, has_app_context
from datetime import datetime, timedelta
import random
import os
import secrets
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from logger import logger, log_request, log_error, log_db_operation
from backup import (
    create_backup,
    restore_backup,
    list_backups,
    delete_backup,
    export_json,
)
from config import config

env = os.environ.get("FLASK_ENV", "development")
app = Flask(__name__)
app.config.from_object(config.get(env, config["default"]))
config["default"].init_app(app)

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address

    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        storage_uri="memory://",
        enabled=app.config.get("RATELIMIT_ENABLED", False),
    )
except ImportError:
    limiter = None
    logger.warning("Flask-Limiter not installed, rate limiting disabled")

DB_NAME = app.config.get("DATABASE_PATH", "proerp.db")
DATABASE_URL = os.environ.get("DATABASE_URL")
IS_POSTGRES = bool(DATABASE_URL)


def make_cursor(conn):
    return conn.cursor()


def execute_query(cursor, *args, **kwargs):
    if not args:
        return cursor

    # Handle both (cursor, sql, params) and (cursor, conn, sql, params)
    if isinstance(args[0], str):
        sql = args[0]
        params = args[1] if len(args) > 1 else kwargs.get("params")
    else:
        sql = args[1]
        params = args[2] if len(args) > 2 else kwargs.get("params")

    if IS_POSTGRES:
        if params:
            sql = sql.replace("?", "%s")
        # SQLite to PostgreSQL syntax mappings
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace(
            "DATETIME DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
        if "INSERT OR IGNORE" in sql:
            sql = sql.replace("INSERT OR IGNORE", "INSERT")
            if "VALUES" in sql:
                sql += " ON CONFLICT DO NOTHING"

    cursor.execute(sql, params if params is not None else [])
    return cursor


def get_scalar(cursor, *args, **kwargs):
    execute_query(cursor, *args, **kwargs)
    row = cursor.fetchone()
    if not row:
        return None
    try:
        # SQLite Row supports index-based access
        return row[0]
    except (TypeError, KeyError, IndexError):
        # PostgreSQL dict_row or other non-indexable object
        if hasattr(row, "values") and callable(row.values):
            return list(row.values())[0]
        elif isinstance(row, dict):
            return list(row.values())[0]
        return row


def get_db():
    if has_app_context():
        if "db" not in g:
            if IS_POSTGRES:
                g.db = psycopg.connect(DATABASE_URL, row_factory=rows.dict_row)
            else:
                g.db = sqlite3.connect(DB_NAME, check_same_thread=False)
                g.db.row_factory = sqlite3.Row
                g.db.execute("PRAGMA journal_mode=WAL")
                g.db.execute("PRAGMA synchronous=NORMAL")
                g.db.execute("PRAGMA cache_size=10000")
        return g.db
    else:
        if IS_POSTGRES:
            conn = psycopg.connect(DATABASE_URL, row_factory=rows.dict_row)
        else:
            conn = sqlite3.connect(DB_NAME)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
        return conn


@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def get_user_org_id(user_id):
    if user_id is None:
        return None
    cache_key = f"user_org_{user_id}"
    if cache_key not in g:
        conn = get_db()
        c = make_cursor(conn)
        execute_query(c, "SELECT organisation_id FROM users WHERE id = ?", (user_id,))
        user = c.fetchone()
        g.setdefault(cache_key, user["organisation_id"] if user else None)
    return g.get(cache_key)


def calculate_gst(subtotal, gst_rate, is_inter_state):
    gst_amount = subtotal * (gst_rate / 100)
    if is_inter_state:
        cgst = sgst = 0
        igst = gst_amount
    else:
        cgst = sgst = gst_amount / 2
        igst = 0
    total = subtotal + cgst + sgst + igst
    return cgst, sgst, igst, total


def check_inter_state(org_id, party_id):
    conn = get_db()
    c = make_cursor(conn)
    execute_query(c, "SELECT state FROM organisations WHERE id = ?", (org_id,))
    org_row = c.fetchone()
    company_state = org_row["state"] if org_row else None

    execute_query(c, "SELECT state FROM parties WHERE id = ?", (party_id,))
    party_row = c.fetchone()
    party_state = party_row["state"] if party_row else None

    return company_state and party_state and company_state != party_state


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user_id = request.headers.get("X-User-Id")
        if not user_id:
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)

    return decorated


def validate_json(*required_fields):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not request.is_json:
                return jsonify({"error": "Content-Type must be application/json"}), 400
            data = request.get_json()
            missing = [f for f in required_fields if f not in data or not data[f]]
            if missing:
                return jsonify(
                    {"error": f"Missing required fields: {', '.join(missing)}"}
                ), 400
            return f(*args, **kwargs)

        return decorated

    return decorator


@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": "Bad request"}), 400


@app.errorhandler(401)
def unauthorized(e):
    return jsonify({"error": "Unauthorized"}), 401


@app.errorhandler(403)
def forbidden(e):
    return jsonify({"error": "Forbidden"}), 403


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(429)
def rate_limit_exceeded(e):
    return jsonify({"error": "Rate limit exceeded. Please try again later."}), 429


@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {str(e)}")
    return jsonify({"error": "Internal server error"}), 500


@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    if app.config.get("SESSION_COOKIE_SECURE"):
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
    return response


def init_db():
    conn = get_db()
    c = make_cursor(conn)

    if not IS_POSTGRES:
        execute_query(c, "PRAGMA journal_mode=WAL")

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS organisations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        gstin TEXT,
        pan TEXT,
        phone TEXT,
        email TEXT,
        address TEXT,
        state TEXT,
        city TEXT,
        gst_type TEXT DEFAULT 'regular',
        bank_name TEXT,
        bank_account TEXT,
        bank_ifsc TEXT,
        bank_branch TEXT,
        default_gst_rate REAL DEFAULT 18,
        payment_terms TEXT,
        footer_note TEXT,
        invoice_template TEXT DEFAULT 'classic',
        quotation_template TEXT DEFAULT 'classic',
        po_template TEXT DEFAULT 'classic',
        is_active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    )

    execute_query(c, "SELECT id FROM organisations LIMIT 1")
    if not c.fetchone():
        execute_query(
            c,
            conn,
            """INSERT INTO organisations (name, gstin, state) VALUES (?, ?, ?)""",
            ("My Company", "", "Maharashtra"),
        )

    execute_query(c, "SELECT id FROM organisations LIMIT 1")
    first_org = c.fetchone()
    try:
        org_id = first_org["id"] if first_org else 1
    except (TypeError, KeyError, IndexError):
        org_id = first_org[0] if first_org else 1

    for table, col in [
        ("users", "organisation_id"),
        ("parties", "organisation_id"),
        ("items", "organisation_id"),
        ("invoices", "organisation_id"),
        ("transactions", "organisation_id"),
        ("expenses", "organisation_id"),
        ("purchase_orders", "organisation_id"),
        ("quotations", "organisation_id"),
    ]:
        if IS_POSTGRES:
            execute_query(
                c,
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                (table,),
            )
            cols = [row["column_name"] for row in c.fetchall()]
        else:
            execute_query(c, f"PRAGMA table_info({table})")
            cols = [row[1] for row in c.fetchall()]

        if col not in cols:
            execute_query(
                c,
                conn,
                f"ALTER TABLE {table} ADD COLUMN {col} INTEGER REFERENCES organisations(id)",
            )
            execute_query(
                c, c, f"UPDATE {table} SET {col} = ? WHERE {col} IS NULL", (org_id,)
            )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS parties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        type TEXT CHECK(type IN ('customer', 'vendor')) NOT NULL,
        gstin TEXT,
        phone TEXT,
        email TEXT,
        address TEXT,
        state TEXT,
        city TEXT,
        opening_balance REAL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    )

    if IS_POSTGRES:
        execute_query(
            c,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'parties'",
        )
        columns = [row["column_name"] for row in c.fetchall()]
    else:
        execute_query(c, "PRAGMA table_info(parties)")
        columns = [row[1] for row in c.fetchall()]

    for col, default in [("city", None), ("pan", None), ("place_of_supply", None)]:
        if col not in columns:
            execute_query(c, f"ALTER TABLE parties ADD COLUMN {col} TEXT")

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        hsn_code TEXT,
        sku TEXT,
        unit TEXT DEFAULT 'PCS',
        rate REAL DEFAULT 0,
        gst_rate REAL DEFAULT 18,
        opening_stock INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_no TEXT NOT NULL,
        party_id INTEGER,
        type TEXT CHECK(type IN ('sale', 'purchase')) NOT NULL,
        date TEXT NOT NULL,
        subtotal REAL DEFAULT 0,
        cgst REAL DEFAULT 0,
        sgst REAL DEFAULT 0,
        igst REAL DEFAULT 0,
        total REAL DEFAULT 0,
        notes TEXT,
        is_inter_state INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (party_id) REFERENCES parties(id)
    )""",
    )

    if IS_POSTGRES:
        execute_query(
            c,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'invoices'",
        )
        columns = [row["column_name"] for row in c.fetchall()]
    else:
        execute_query(c, "PRAGMA table_info(invoices)")
        columns = [row[1] for row in c.fetchall()]

    if "status" not in columns:
        execute_query(
            c, "ALTER TABLE invoices ADD COLUMN status TEXT DEFAULT 'pending'"
        )
    if "is_inter_state" not in columns:
        execute_query(
            c, conn, "ALTER TABLE invoices ADD COLUMN is_inter_state INTEGER DEFAULT 0"
        )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS invoice_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        rate REAL NOT NULL,
        gst_rate REAL NOT NULL,
        amount REAL NOT NULL,
        FOREIGN KEY (invoice_id) REFERENCES invoices(id),
        FOREIGN KEY (item_id) REFERENCES items(id)
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        type TEXT CHECK(type IN ('receipt', 'payment', 'journal')) NOT NULL,
        party_id INTEGER,
        amount REAL NOT NULL,
        mode TEXT CHECK(mode IN ('cash', 'bank', 'upi', 'card')),
        reference_no TEXT,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (party_id) REFERENCES parties(id)
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY,
        company_name TEXT DEFAULT 'My Company',
        company_gstin TEXT,
        company_pan TEXT,
        company_address TEXT,
        company_phone TEXT,
        company_email TEXT,
        state TEXT DEFAULT 'Maharashtra',
        gst_type TEXT DEFAULT 'regular',
        bank_name TEXT,
        bank_account TEXT,
        bank_ifsc TEXT,
        bank_branch TEXT,
        default_gst_rate REAL DEFAULT 18,
        payment_terms TEXT,
        footer_note TEXT,
        invoice_template TEXT DEFAULT 'classic',
        quotation_template TEXT DEFAULT 'classic',
        po_template TEXT DEFAULT 'classic'
    )""",
    )

    if IS_POSTGRES:
        execute_query(
            c, "INSERT INTO settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
        )
    else:
        execute_query(c, "INSERT OR IGNORE INTO settings (id) VALUES (1)")

    if IS_POSTGRES:
        execute_query(
            c,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'settings'",
        )
        columns = [row["column_name"] for row in c.fetchall()]
    else:
        execute_query(c, "PRAGMA table_info(settings)")
        columns = [row[1] for row in c.fetchall()]
    for col in [
        "company_pan",
        "bank_name",
        "bank_account",
        "bank_ifsc",
        "bank_branch",
        "default_gst_rate",
        "payment_terms",
        "footer_note",
    ]:
        if col not in columns:
            execute_query(c, f"ALTER TABLE settings ADD COLUMN {col} TEXT")

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        name TEXT NOT NULL,
        email TEXT,
        role TEXT CHECK(role IN ('admin', 'accountant', 'staff')) NOT NULL,
        avatar TEXT,
        is_active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS module_access (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT CHECK(role IN ('admin', 'accountant', 'staff')) NOT NULL,
        module TEXT NOT NULL,
        can_view INTEGER DEFAULT 1,
        can_add INTEGER DEFAULT 0,
        can_edit INTEGER DEFAULT 0,
        can_delete INTEGER DEFAULT 0,
        UNIQUE(role, module)
    )""",
    )

    execute_query(c, "SELECT id FROM users WHERE username = ?", ("admin",))
    if not c.fetchone():
        hashed_password = generate_password_hash("admin123")
        execute_query(
            c,
            conn,
            """INSERT INTO users (username, password, name, email, role, avatar) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
            (
                "admin",
                hashed_password,
                "Administrator",
                "admin@company.com",
                "admin",
                None,
            ),
        )

        modules = [
            "dashboard",
            "parties",
            "items",
            "invoices",
            "transactions",
            "expenses",
            "purchase_orders",
            "quotations",
            "reports",
            "organisation",
            "settings",
        ]

        for mod in modules:
            execute_query(
                c,
                conn,
                """INSERT INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                         VALUES (?, ?, ?, ?, ?, ?)""",
                ("admin", mod, 1, 1, 1, 1),
            )

        for mod in modules:
            execute_query(
                c,
                conn,
                """INSERT INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                         VALUES (?, ?, ?, ?, ?, ?)""",
                ("accountant", mod, 1, 1, 1, 0),
            )

        execute_query(
            c,
            conn,
            """INSERT INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
            ("staff", "dashboard", 1, 0, 0, 0),
        )
        execute_query(
            c,
            conn,
            """INSERT INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
            ("staff", "invoices", 1, 1, 0, 0),
        )
        execute_query(
            c,
            conn,
            """INSERT INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
            ("staff", "items", 1, 0, 0, 0),
        )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        category TEXT NOT NULL,
        description TEXT,
        amount REAL NOT NULL,
        gst_rate REAL DEFAULT 0,
        cgst REAL DEFAULT 0,
        sgst REAL DEFAULT 0,
        igst REAL DEFAULT 0,
        mode TEXT DEFAULT 'cash',
        reference_no TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS purchase_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        po_no TEXT NOT NULL,
        party_id INTEGER,
        date TEXT NOT NULL,
        delivery_date TEXT,
        subtotal REAL DEFAULT 0,
        cgst REAL DEFAULT 0,
        sgst REAL DEFAULT 0,
        igst DEFAULT 0,
        total REAL DEFAULT 0,
        notes TEXT,
        status TEXT DEFAULT 'pending',
        is_inter_state INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (party_id) REFERENCES parties(id)
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS purchase_order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        po_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        rate REAL NOT NULL,
        gst_rate REAL NOT NULL,
        amount REAL NOT NULL,
        FOREIGN KEY (po_id) REFERENCES purchase_orders(id),
        FOREIGN KEY (item_id) REFERENCES items(id)
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS quotations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_no TEXT NOT NULL,
        party_id INTEGER,
        date TEXT NOT NULL,
        valid_until TEXT,
        subtotal REAL DEFAULT 0,
        cgst REAL DEFAULT 0,
        sgst REAL DEFAULT 0,
        igst DEFAULT 0,
        total REAL DEFAULT 0,
        notes TEXT,
        status TEXT DEFAULT 'pending',
        is_inter_state INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (party_id) REFERENCES parties(id)
    )""",
    )

    execute_query(
        c,
        conn,
        """CREATE TABLE IF NOT EXISTS quotation_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        rate REAL NOT NULL,
        gst_rate REAL NOT NULL,
        amount REAL NOT NULL,
        FOREIGN KEY (quote_id) REFERENCES quotations(id),
        FOREIGN KEY (item_id) REFERENCES items(id)
    )""",
    )

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(date)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_type ON invoices(type)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_party_id ON invoices(party_id)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_org_id ON invoices(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_type_date ON invoices(type, date)",
        "CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice_id ON invoice_items(invoice_id)",
        "CREATE INDEX IF NOT EXISTS idx_invoice_items_item_id ON invoice_items(item_id)",
        "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)",
        "CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type)",
        "CREATE INDEX IF NOT EXISTS idx_transactions_org_id ON transactions(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_transactions_party_id ON transactions(party_id)",
        "CREATE INDEX IF NOT EXISTS idx_parties_org_id ON parties(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_parties_type ON parties(type)",
        "CREATE INDEX IF NOT EXISTS idx_items_org_id ON items(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)",
        "CREATE INDEX IF NOT EXISTS idx_expenses_org_id ON expenses(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_purchase_orders_org_id ON purchase_orders(organisation_id)",
        "CREATE INDEX IF NOT EXISTS idx_quotations_org_id ON quotations(organisation_id)",
    ]

    for idx in indexes:
        execute_query(c, idx)

    conn.commit()
    conn.close()
    logger.info("Database initialized with optimizations")


def generate_invoice_no(invoice_type):
    conn = get_db()
    c = make_cursor(conn)
    today = datetime.now().strftime("%Y%m%d")
    prefix = "SI" if invoice_type == "sale" else "PI"
    execute_query(
        c,
        conn,
        "SELECT COUNT(*) as count FROM invoices WHERE type = ?",
        (invoice_type,),
    )
    count = c.fetchone()["count"] + 1
    return f"{prefix}/{today}/{str(count).zfill(4)}"


@app.route("/")
def index():
    return send_from_directory("public", "index.html")


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'sale' """,
    )
    sales = c.fetchone()["total"] or 0

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'purchase' """,
    )
    purchases = c.fetchone()["total"] or 0

    execute_query(c, "SELECT COUNT(*) as count FROM parties")
    parties = c.fetchone()["count"]

    execute_query(c, "SELECT COUNT(*) as count FROM items")
    items = c.fetchone()["count"]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'receipt' """,
    )
    receipts = c.fetchone()["total"] or 0

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'payment' """,
    )
    payments = c.fetchone()["total"] or 0

    return jsonify(
        {
            "sales": sales,
            "purchases": purchases,
            "parties": parties,
            "items": items,
            "receipts": receipts,
            "payments": payments,
            "profit": sales - purchases,
        }
    )


@app.route("/api/charts", methods=["GET"])
def chart_data():
    range_type = request.args.get("range", "12months")

    now = datetime.now()

    if range_type == "calyear":
        start_date = f"{now.year}-01-01"
        end_date = f"{now.year}-12-31"
    elif range_type == "finyear":
        if now.month >= 4:
            start_date = f"{now.year}-04-01"
            end_date = f"{now.year + 1}-03-31"
        else:
            start_date = f"{now.year - 1}-04-01"
            end_date = f"{now.year}-03-31"
    else:
        start_date = f"{now.year - 1}-{now.month:02d}-{now.day:02d}"
        end_date = f"{now.year}-{now.month:02d}-{now.day:02d}"

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT substr(date, 1, 7) as month,
                 SUM(CASE WHEN type = 'sale' THEN total ELSE 0 END) as sales,
                 SUM(CASE WHEN type = 'purchase' THEN total ELSE 0 END) as purchases
                 FROM invoices
                 WHERE date >= ? AND date <= ?
                 GROUP BY month ORDER BY month""",
        (start_date, end_date),
    )
    monthly_data = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT 
        COALESCE(SUM(CASE WHEN type = 'receipt' THEN amount ELSE 0 END), 0) as income,
        (SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE date >= ? AND date <= ?) as expenses,
        (SELECT COALESCE(SUM(cgst + sgst + igst), 0) FROM invoices WHERE type = 'sale' AND date >= ? AND date <= ?) as gst_collected
        FROM transactions WHERE type = 'receipt' AND date >= ? AND date <= ?""",
        (start_date, end_date, start_date, end_date, start_date, end_date),
    )
    result = c.fetchone()

    execute_query(
        c,
        conn,
        """SELECT p.name, SUM(i.total) as total 
                 FROM invoices i JOIN parties p ON i.party_id = p.id 
                 WHERE i.type = 'sale' AND i.date >= ? AND i.date <= ?
                 GROUP BY p.id ORDER BY total DESC LIMIT 5""",
        (start_date, end_date),
    )
    top_parties = [dict(row) for row in c.fetchall()]

    return jsonify(
        {
            "monthly": monthly_data,
            "income": result["income"],
            "expenses": result["expenses"],
            "gst_collected": result["gst_collected"],
            "top_parties": top_parties,
        }
    )


def paginate_query(query, params, page=1, limit=50):
    offset = (page - 1) * limit
    return query + " LIMIT ? OFFSET ?", params + [limit, offset]


@app.route("/api/parties", methods=["GET"])
def get_parties():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    search = request.args.get("search", "")

    conn = get_db()
    c = make_cursor(conn)

    where_clause = "WHERE organisation_id = ?"
    params = [org_id]

    if search:
        where_clause += " AND name LIKE ?"
        params.append(f"%{search}%")

    total = get_scalar(c, f"SELECT COUNT(*) FROM parties {where_clause}", params)

    query = f"SELECT * FROM parties {where_clause} ORDER BY name LIMIT ? OFFSET ?"
    execute_query(c, query, params + [limit, (page - 1) * limit])
    parties = [dict(row) for row in c.fetchall()]

    return jsonify(
        {
            "data": parties,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }
    )


@app.route("/api/parties/<int:id>", methods=["GET"])
def get_party(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT * FROM parties WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    row = c.fetchone()
    party = dict(row) if row else None
    return jsonify(party)


@app.route("/api/parties", methods=["POST"])
def create_party():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json

    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Name is required"}), 400

    phone = data.get("phone", "")
    if phone and (len(phone) > 10 or not phone.isdigit()):
        return jsonify({"error": "Phone must be 10 digits"}), 400

    gstin = data.get("gstin", "").strip().upper()
    if gstin and len(gstin) != 15:
        return jsonify({"error": "GSTIN must be 15 characters"}), 400

    email = data.get("email", "").strip()
    if email and "@" not in email:
        return jsonify({"error": "Invalid email format"}), 400

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """INSERT INTO parties (name, type, gstin, pan, phone, email, address, state, city, place_of_supply, opening_balance, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            name,
            data.get("type"),
            gstin,
            data.get("pan"),
            phone,
            email,
            data.get("address"),
            data.get("state"),
            data.get("city"),
            data.get("place_of_supply"),
            data.get("opening_balance", 0),
            org_id,
        ),
    )
    conn.commit()
    party_id = c.lastrowid
    log_db_operation("INSERT", "parties", party_id)
    return jsonify({"id": party_id})


@app.route("/api/parties/<int:id>", methods=["PUT"])
def update_party(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json

    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Name is required"}), 400

    phone = data.get("phone", "")
    if phone and (len(phone) > 10 or not phone.isdigit()):
        return jsonify({"error": "Phone must be 10 digits"}), 400

    email = data.get("email", "").strip()

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """UPDATE parties SET name=?, type=?, gstin=?, pan=?, phone=?, email=?, address=?, state=?, city=?, place_of_supply=?, opening_balance=?
                 WHERE id=? AND organisation_id=?""",
        (
            name,
            data.get("type"),
            data.get("gstin", "").strip().upper(),
            data.get("pan"),
            phone,
            email,
            data.get("address"),
            data.get("state"),
            data.get("city"),
            data.get("place_of_supply"),
            data.get("opening_balance", 0),
            id,
            org_id,
        ),
    )
    conn.commit()
    log_db_operation("UPDATE", "parties", id)
    return jsonify({"success": True})


@app.route("/api/parties/<int:id>", methods=["DELETE"])
def delete_party(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM parties WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    log_db_operation("DELETE", "parties", id)
    return jsonify({"success": True})


@app.route("/api/parties/bulk-delete", methods=["POST"])
def bulk_delete_parties():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"success": True})
    conn = get_db()
    c = make_cursor(conn)
    placeholders = ",".join("?" * len(ids))
    execute_query(
        c,
        conn,
        f"DELETE FROM parties WHERE id IN ({placeholders}) AND organisation_id = ?",
        ids + [org_id],
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/items", methods=["GET"])
def get_items():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    search = request.args.get("search", "")

    conn = get_db()
    c = make_cursor(conn)

    where_clause = "WHERE organisation_id = ?"
    params = [org_id]

    if search:
        where_clause += " AND name LIKE ?"
        params.append(f"%{search}%")

    total = get_scalar(c, f"SELECT COUNT(*) FROM items {where_clause}", params)

    query = f"SELECT * FROM items {where_clause} ORDER BY name LIMIT ? OFFSET ?"
    execute_query(c, query, params + [limit, (page - 1) * limit])
    items = [dict(row) for row in c.fetchall()]

    return jsonify(
        {
            "data": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }
    )


@app.route("/api/items/<int:id>", methods=["GET"])
def get_item(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT * FROM items WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    row = c.fetchone()
    item = dict(row) if row else None
    return jsonify(item)


@app.route("/api/items", methods=["POST"])
def create_item():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """INSERT INTO items (name, hsn_code, sku, unit, rate, gst_rate, opening_stock, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["name"],
            data.get("hsn_code"),
            data.get("sku"),
            data.get("unit", "PCS"),
            data.get("rate", 0),
            data.get("gst_rate", 18),
            data.get("opening_stock", 0),
            org_id,
        ),
    )
    conn.commit()
    item_id = c.lastrowid
    log_db_operation("INSERT", "items", item_id)
    return jsonify({"id": item_id})


@app.route("/api/items/<int:id>", methods=["PUT"])
def update_item(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """UPDATE items SET name=?, hsn_code=?, sku=?, unit=?, rate=?, gst_rate=?, opening_stock=?
                 WHERE id=? AND organisation_id=?""",
        (
            data["name"],
            data.get("hsn_code"),
            data.get("sku"),
            data.get("unit"),
            data.get("rate", 0),
            data.get("gst_rate", 18),
            data.get("opening_stock", 0),
            id,
            org_id,
        ),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/items/<int:id>", methods=["DELETE"])
def delete_item(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c, conn, "DELETE FROM items WHERE id = ? AND organisation_id = ?", (id, org_id)
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/items/bulk-delete", methods=["POST"])
def bulk_delete_items():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"success": True})
    conn = get_db()
    c = make_cursor(conn)
    placeholders = ",".join("?" * len(ids))
    execute_query(
        c,
        conn,
        f"DELETE FROM items WHERE id IN ({placeholders}) AND organisation_id = ?",
        ids + [org_id],
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/invoices", methods=["GET"])
def get_invoices():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    inv_type = request.args.get("type", "")

    conn = get_db()
    c = make_cursor(conn)

    where_clause = "WHERE i.organisation_id = ?"
    params = [org_id]

    if inv_type:
        where_clause += " AND i.type = ?"
        params.append(inv_type)

    total = get_scalar(c, f"SELECT COUNT(*) FROM invoices i {where_clause}", params)

    execute_query(
        c,
        conn,
        f"""SELECT i.*, p.name as party_name, p.state as party_state FROM invoices i
                 LEFT JOIN parties p ON i.party_id = p.id {where_clause} 
                 ORDER BY i.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, (page - 1) * limit],
    )
    invoices = [dict(row) for row in c.fetchall()]

    return jsonify(
        {
            "data": invoices,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }
    )


@app.route("/api/invoices/<int:id>", methods=["GET"])
def get_invoice(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """SELECT i.*, p.name as party_name, p.gstin as party_gstin, p.state as party_state
                 FROM invoices i LEFT JOIN parties p ON i.party_id = p.id WHERE i.id = ? AND i.organisation_id = ?""",
        (id, org_id),
    )
    row = c.fetchone()
    invoice = dict(row) if row else None

    if invoice:
        execute_query(
            c,
            conn,
            """SELECT ii.*, i.name as item_name, i.hsn_code, i.unit FROM invoice_items ii
                     LEFT JOIN items i ON ii.item_id = i.id WHERE ii.invoice_id = ?""",
            (id,),
        )
        invoice["items"] = [dict(row) for row in c.fetchall()]
    return jsonify(invoice)


@app.route("/api/invoices", methods=["POST"])
def create_invoice():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT state FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    company_state = org["state"] if org else None

    execute_query(
        c,
        conn,
        "SELECT state FROM parties WHERE id = ? AND organisation_id = ?",
        (data["party_id"], org_id),
    )
    party_row = c.fetchone()
    party_state = party_row["state"] if party_row else None

    invoice_no = generate_invoice_no(data["type"])

    subtotal = 0
    cgst = sgst = igst = 0
    is_inter_state = company_state and party_state and company_state != party_state

    for item in data["items"]:
        amount = item["quantity"] * item["rate"]
        subtotal += amount
        gst_amount = amount * (item["gst_rate"] / 100)

        if is_inter_state:
            igst += gst_amount
        else:
            cgst += gst_amount / 2
            sgst += gst_amount / 2

    total = subtotal + cgst + sgst + igst

    execute_query(
        c,
        conn,
        """INSERT INTO invoices (invoice_no, party_id, type, date, subtotal, cgst, sgst, igst, total, notes, is_inter_state, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            invoice_no,
            data["party_id"],
            data["type"],
            data["date"],
            subtotal,
            cgst,
            sgst,
            igst,
            total,
            data.get("notes"),
            1 if is_inter_state else 0,
            org_id,
        ),
    )
    conn.commit()
    invoice_id = c.lastrowid

    items_data = [
        (
            invoice_id,
            item.get("id") or item.get("item_id"),
            item["quantity"],
            item["rate"],
            item["gst_rate"],
            item["quantity"] * item["rate"],
        )
        for item in data["items"]
    ]
    c.executemany(
        """INSERT INTO invoice_items (invoice_id, item_id, quantity, rate, gst_rate, amount)
                     VALUES (?, ?, ?, ?, ?, ?)""",
        items_data,
    )

    conn.commit()
    log_db_operation("INSERT", "invoices", invoice_id)
    return jsonify({"id": invoice_id, "invoice_no": invoice_no})


@app.route("/api/invoices/<int:id>", methods=["DELETE"])
def delete_invoice(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM invoice_items WHERE invoice_id IN (SELECT id FROM invoices WHERE id = ? AND organisation_id = ?)",
        (id, org_id),
    )
    execute_query(
        c,
        conn,
        "DELETE FROM invoices WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/invoices/<int:id>/status", methods=["PUT"])
def update_invoice_status(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "UPDATE invoices SET status = ? WHERE id = ? AND organisation_id = ?",
        (data.get("status"), id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/invoices/bulk-delete", methods=["POST"])
def bulk_delete_invoices():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"success": True})
    conn = get_db()
    c = make_cursor(conn)
    placeholders = ",".join("?" * len(ids))
    execute_query(
        c,
        conn,
        f"""DELETE FROM invoice_items WHERE invoice_id IN 
                  (SELECT id FROM invoices WHERE id IN ({placeholders}) AND organisation_id = ?)""",
        ids + [org_id],
    )
    execute_query(
        c,
        conn,
        f"DELETE FROM invoices WHERE id IN ({placeholders}) AND organisation_id = ?",
        ids + [org_id],
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/transactions", methods=["GET"])
def get_transactions():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c, c, f"SELECT COUNT(*) FROM transactions WHERE organisation_id = ?", (org_id,)
    )
    total = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT t.*, p.name as party_name FROM transactions t
                 LEFT JOIN parties p ON t.party_id = p.id WHERE t.organisation_id = ? 
                 ORDER BY t.date DESC, t.id DESC LIMIT ? OFFSET ?""",
        (org_id, limit, (page - 1) * limit),
    )
    transactions = [dict(row) for row in c.fetchall()]

    return jsonify(
        {
            "data": transactions,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit,
        }
    )


@app.route("/api/transactions", methods=["POST"])
def create_transaction():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """INSERT INTO transactions (date, type, party_id, amount, mode, reference_no, description, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["date"],
            data["type"],
            data.get("party_id"),
            data["amount"],
            data.get("mode"),
            data.get("reference_no"),
            data.get("description"),
            org_id,
        ),
    )
    conn.commit()
    trans_id = c.lastrowid
    return jsonify({"id": trans_id})


@app.route("/api/transactions/<int:id>", methods=["DELETE"])
def delete_transaction(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM transactions WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/transactions/bulk-delete", methods=["POST"])
def bulk_delete_transactions():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    ids = data.get("ids", [])
    conn = get_db()
    c = make_cursor(conn)
    placeholders = ",".join("?" * len(ids))
    execute_query(
        c,
        conn,
        f"DELETE FROM transactions WHERE id IN ({placeholders}) AND organisation_id = ?",
        ids + [org_id],
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/reports/party-ledger/<int:party_id>", methods=["GET"])
def party_ledger(party_id):
    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT * FROM parties WHERE id = ?", (party_id,))
    party = dict(c.fetchone())

    execute_query(
        c,
        conn,
        """SELECT 'Invoice' as type, invoice_no as ref_no, date, total as amount
                 FROM invoices WHERE party_id = ? AND type = 'sale' """,
        (party_id,),
    )
    invoices = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT 'Receipt' as type, reference_no as ref_no, date, amount
                 FROM transactions WHERE party_id = ? AND type = 'receipt' """,
        (party_id,),
    )
    payments = [dict(row) for row in c.fetchall()]

    all_entries = invoices + payments
    all_entries.sort(key=lambda x: x["date"])

    balance = party.get("opening_balance", 0) or 0
    for item in all_entries:
        balance += item["amount"] if item["type"] == "Invoice" else -item["amount"]
        item["balance"] = balance

    return jsonify({"party": party, "entries": all_entries, "closingBalance": balance})


@app.route("/api/reports/gst-summary", methods=["GET"])
def gst_summary():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT SUM(subtotal) as taxable, SUM(cgst) as cgst, SUM(sgst) as sgst, SUM(igst) as igst
                 FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    sales = dict(c.fetchone())

    execute_query(
        c,
        conn,
        """SELECT SUM(subtotal) as taxable, SUM(cgst) as cgst, SUM(sgst) as sgst, SUM(igst) as igst
                 FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    purchases = dict(c.fetchone())

    return jsonify({"sales": sales, "purchases": purchases})


@app.route("/api/reports/gstr-1", methods=["GET"])
def gstr1_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT i.invoice_no, i.date, i.total, i.subtotal, i.cgst, i.sgst, i.igst, i.is_inter_state,
                 p.name as party_name, p.gstin as party_gstin, p.state as party_state
                 FROM invoices i LEFT JOIN parties p ON i.party_id = p.id
                 WHERE i.type = 'sale' AND i.date BETWEEN ? AND ? ORDER BY i.date""",
        (from_date, to_date),
    )
    invoices = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT DISTINCT ii.gst_rate, SUM(ii.amount) as taxable_amount, SUM(ii.amount * ii.gst_rate / 100) as tax_amount
                 FROM invoices i JOIN invoice_items ii ON i.id = ii.invoice_id
                 WHERE i.type = 'sale' AND i.date BETWEEN ? AND ?
                 GROUP BY ii.gst_rate""",
        (from_date, to_date),
    )
    tax_rates = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT SUM(total) as total, SUM(subtotal) as taxable FROM invoices 
                 WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    totals = dict(c.fetchone())

    b2b = [inv for inv in invoices if inv.get("party_gstin")]
    b2c = [inv for inv in invoices if not inv.get("party_gstin")]

    return jsonify(
        {
            "invoices": invoices,
            "b2b": b2b,
            "b2c": b2c,
            "tax_rates": tax_rates,
            "totals": totals,
        }
    )


@app.route("/api/reports/gstr-2", methods=["GET"])
def gstr2_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT i.invoice_no, i.date, i.total, i.subtotal, i.cgst, i.sgst, i.igst, i.is_inter_state,
                 p.name as party_name, p.gstin as party_gstin, p.state as party_state
                 FROM invoices i LEFT JOIN parties p ON i.party_id = p.id
                 WHERE i.type = 'purchase' AND i.date BETWEEN ? AND ? ORDER BY i.date""",
        (from_date, to_date),
    )
    invoices = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT DISTINCT ii.gst_rate, SUM(ii.amount) as taxable_amount, SUM(ii.amount * ii.gst_rate / 100) as tax_amount
                 FROM invoices i JOIN invoice_items ii ON i.id = ii.invoice_id
                 WHERE i.type = 'purchase' AND i.date BETWEEN ? AND ?
                 GROUP BY ii.gst_rate""",
        (from_date, to_date),
    )
    tax_rates = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT SUM(total) as total, SUM(subtotal) as taxable FROM invoices 
                 WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    totals = dict(c.fetchone())

    return jsonify({"invoices": invoices, "tax_rates": tax_rates, "totals": totals})


@app.route("/api/reports/day-book", methods=["GET"])
def day_book_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT date, type, notes as description, total as amount, '' as mode, invoice_no as reference_no, 'Invoice' as source
                 FROM invoices WHERE date BETWEEN ? AND ?
                 UNION ALL
                 SELECT date, type, description, amount, mode, reference_no, 'Transaction' as source
                 FROM transactions WHERE date BETWEEN ? AND ?
                 UNION ALL
                 SELECT date, category as type, description, amount, mode, reference_no, 'Expense' as source
                 FROM expenses WHERE date BETWEEN ? AND ?
                 ORDER BY date""",
        (from_date, to_date, from_date, to_date, from_date, to_date),
    )
    entries = [dict(row) for row in c.fetchall()]

    sales = get_scalar(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )

    purchases = get_scalar(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )

    receipts = get_scalar(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'receipt' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )

    payments = get_scalar(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'payment' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )

    expenses = get_scalar(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM expenses WHERE date BETWEEN ? AND ?""",
        (from_date, to_date),
    )

    return jsonify(
        {
            "entries": entries,
            "totals": {
                "sales": sales,
                "purchases": purchases,
                "receipts": receipts,
                "payments": payments,
                "expenses": expenses,
            },
        }
    )


@app.route("/api/reports/cash-book", methods=["GET"])
def cash_book_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT date, type, reference_no as ref, 
                 CASE WHEN type = 'receipt' THEN amount ELSE -amount END as amount
                 FROM transactions
                 WHERE mode = 'cash' AND date BETWEEN ? AND ?
                 UNION ALL
                 SELECT date, 'Expense' as type, reference_no as ref, -amount as amount 
                 FROM expenses
                 WHERE mode = 'cash' AND date BETWEEN ? AND ?
                 ORDER BY date""",
        (from_date, to_date, from_date, to_date),
    )
    entries = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions 
                 WHERE type = 'receipt' AND mode = 'cash' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    cash_in = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions 
                 WHERE type = 'payment' AND mode = 'cash' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    cash_out_payments = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM expenses 
                 WHERE mode = 'cash' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    cash_out_expenses = c.fetchone()[0]

    return jsonify(
        {
            "entries": entries,
            "totals": {
                "cash_in": cash_in,
                "cash_out": cash_out_payments + cash_out_expenses,
            },
        }
    )


@app.route("/api/reports/trial-balance", methods=["GET"])
def trial_balance_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_sales = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(total), 0) as total FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_purchases = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(opening_balance), 0) as total FROM parties WHERE type = 'customer' """,
    )
    receivables = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(opening_balance), 0) as total FROM parties WHERE type = 'vendor' """,
    )
    payables = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM expenses WHERE date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_expenses = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'receipt' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_receipts = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'payment' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_payments = c.fetchone()[0]

    return jsonify(
        {
            "sales": total_sales,
            "purchases": total_purchases,
            "receivables": receivables,
            "payables": payables,
            "expenses": total_expenses,
            "receipts": total_receipts,
            "payments": total_payments,
            "profit": total_sales - total_purchases - total_expenses,
        }
    )


@app.route("/api/reports/profit-loss", methods=["GET"])
def profit_loss_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(subtotal), 0) as total FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    gross_sales = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(cgst + sgst + igst), 0) as total FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    sales_tax = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(subtotal), 0) as total FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    gross_purchases = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(cgst + sgst + igst), 0) as total FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    purchase_tax = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT COALESCE(SUM(amount), 0) as total FROM expenses WHERE date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    total_expenses = c.fetchone()[0]

    execute_query(
        c,
        conn,
        """SELECT category, COALESCE(SUM(amount), 0) as total FROM expenses WHERE date BETWEEN ? AND ? GROUP BY category""",
        (from_date, to_date),
    )
    expense_breakdown = [dict(row) for row in c.fetchall()]

    net_sales = gross_sales
    net_purchases = gross_purchases
    gross_profit = net_sales - net_purchases
    net_profit = gross_profit - total_expenses

    return jsonify(
        {
            "gross_sales": gross_sales,
            "sales_tax": sales_tax,
            "gross_purchases": gross_purchases,
            "purchase_tax": purchase_tax,
            "gross_profit": gross_profit,
            "expenses": total_expenses,
            "expense_breakdown": expense_breakdown,
            "net_profit": net_profit,
        }
    )


@app.route("/api/reports/sales-register", methods=["GET"])
def sales_register_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT i.invoice_no, i.date, p.name as party_name, p.gstin as party_gstin, p.state as party_state,
                 i.subtotal, i.cgst, i.sgst, i.igst, i.total, i.is_inter_state
                 FROM invoices i LEFT JOIN parties p ON i.party_id = p.id
                 WHERE i.type = 'sale' AND i.date BETWEEN ? AND ? ORDER BY i.date, i.invoice_no""",
        (from_date, to_date),
    )
    invoices = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT COUNT(*) as count, SUM(total) as total, SUM(subtotal) as taxable FROM invoices WHERE type = 'sale' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    summary = dict(c.fetchone())

    return jsonify({"invoices": invoices, "summary": summary})


@app.route("/api/reports/purchase-register", methods=["GET"])
def purchase_register_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT i.invoice_no, i.date, p.name as party_name, p.gstin as party_gstin, p.state as party_state,
                 i.subtotal, i.cgst, i.sgst, i.igst, i.total, i.is_inter_state
                 FROM invoices i LEFT JOIN parties p ON i.party_id = p.id
                 WHERE i.type = 'purchase' AND i.date BETWEEN ? AND ? ORDER BY i.date, i.invoice_no""",
        (from_date, to_date),
    )
    invoices = [dict(row) for row in c.fetchall()]

    execute_query(
        c,
        conn,
        """SELECT COUNT(*) as count, SUM(total) as total, SUM(subtotal) as taxable FROM invoices WHERE type = 'purchase' AND date BETWEEN ? AND ?""",
        (from_date, to_date),
    )
    summary = dict(c.fetchone())

    return jsonify({"invoices": invoices, "summary": summary})


@app.route("/api/reports/stock-summary", methods=["GET"])
def stock_summary_report():
    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT i.name, i.sku, i.hsn_code, i.unit, i.rate as price, i.gst_rate, i.opening_stock,
                 COALESCE(sold.qty, 0) as sold,
                 COALESCE(purchased.qty, 0) as purchased
                 FROM items i
                 LEFT JOIN (SELECT item_id, SUM(quantity) as qty FROM invoice_items ii 
                           JOIN invoices inv ON ii.invoice_id = inv.id AND inv.type = 'sale' GROUP BY item_id) sold ON i.id = sold.item_id
                 LEFT JOIN (SELECT item_id, SUM(quantity) as qty FROM invoice_items ii 
                           JOIN invoices inv ON ii.invoice_id = inv.id AND inv.type = 'purchase' GROUP BY item_id) purchased ON i.id = purchased.item_id""",
    )
    items = [dict(row) for row in c.fetchall()]

    for item in items:
        item["current_stock"] = (
            (item["opening_stock"] or 0)
            + (item["purchased"] or 0)
            - (item["sold"] or 0)
        )
        item["stock_value"] = item["current_stock"] * (item["price"] or 0)

    return jsonify({"items": items})


@app.route("/api/reports/party-wise-sales", methods=["GET"])
def party_wise_sales_report():
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """SELECT p.name, p.gstin, p.state, COUNT(i.id) as invoice_count, 
                 SUM(i.subtotal) as taxable, SUM(i.cgst + i.sgst + i.igst) as tax, SUM(i.total) as total
                 FROM invoices i JOIN parties p ON i.party_id = p.id
                 WHERE i.type = 'sale' AND i.date BETWEEN ? AND ?
                 GROUP BY p.id ORDER BY total DESC""",
        (from_date, to_date),
    )
    parties = [dict(row) for row in c.fetchall()]

    return jsonify({"parties": parties})


@app.route("/api/settings", methods=["GET"])
def get_settings():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    if org:
        settings = dict(org)
        settings["company_name"] = org["name"]
        settings["company_gstin"] = org["gstin"]
        settings["company_pan"] = org["pan"]
        settings["company_phone"] = org["phone"]
        settings["company_email"] = org["email"]
        settings["company_address"] = org["address"]
        settings["state"] = org["state"]
        settings["gst_type"] = org["gst_type"]
        settings["bank_name"] = org["bank_name"]
        settings["bank_account"] = org["bank_account"]
        settings["bank_ifsc"] = org["bank_ifsc"]
        settings["bank_branch"] = org["bank_branch"]
        settings["default_gst_rate"] = org["default_gst_rate"]
        settings["payment_terms"] = org["payment_terms"]
        settings["footer_note"] = org["footer_note"]
        settings["invoice_template"] = org["invoice_template"]
        settings["quotation_template"] = org["quotation_template"]
        settings["po_template"] = org["po_template"]
    else:
        settings = {}
    return jsonify(settings)


@app.route("/api/settings", methods=["PUT"])
def update_settings():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """UPDATE organisations SET name=?, gstin=?, pan=?, phone=?, email=?, address=?, 
                 state=?, gst_type=?, bank_name=?, bank_account=?, bank_ifsc=?, bank_branch=?, 
                 default_gst_rate=?, payment_terms=?, footer_note=?, invoice_template=?, 
                 quotation_template=?, po_template=? WHERE id=?""",
        (
            data.get("company_name"),
            data.get("company_gstin"),
            data.get("company_pan"),
            data.get("company_address"),
            data.get("company_phone"),
            data.get("company_email"),
            data.get("state"),
            data.get("gst_type"),
            data.get("bank_name"),
            data.get("bank_account"),
            data.get("bank_ifsc"),
            data.get("bank_branch"),
            data.get("default_gst_rate"),
            data.get("payment_terms"),
            data.get("footer_note"),
            data.get("invoice_template"),
            data.get("quotation_template"),
            data.get("po_template"),
            org_id,
        ),
    )
    conn.commit()
    return jsonify({"success": True})


EXPENSE_CATEGORIES = [
    "Rent",
    "Electricity",
    "Water",
    "Internet",
    "Phone",
    "Salary",
    "Travel",
    "Office Supplies",
    "Maintenance",
    "Insurance",
    "Marketing",
    "Professional Fees",
    "Bank Charges",
    "Miscellaneous",
]


@app.route("/api/expenses", methods=["GET"])
def get_expenses():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT * FROM expenses WHERE organisation_id = ? ORDER BY date DESC, id DESC",
        (org_id,),
    )
    expenses = [dict(row) for row in c.fetchall()]
    return jsonify(expenses)


@app.route("/api/expenses", methods=["POST"])
def create_expense():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    amount = data.get("amount", 0)
    gst_rate = data.get("gst_rate", 0)
    gst_amount = amount * (gst_rate / 100)
    cgst = sgst = igst = 0

    is_inter_state = data.get("is_inter_state", False)

    if is_inter_state:
        igst = gst_amount
    else:
        cgst = gst_amount / 2
        sgst = gst_amount / 2

    total = amount + cgst + sgst + igst

    execute_query(
        c,
        conn,
        """INSERT INTO expenses (date, category, description, amount, gst_rate, cgst, sgst, igst, mode, reference_no, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["date"],
            data["category"],
            data.get("description"),
            amount,
            gst_rate,
            cgst,
            sgst,
            igst,
            data.get("mode", "cash"),
            data.get("reference_no"),
            org_id,
        ),
    )
    conn.commit()
    expense_id = c.lastrowid
    return jsonify({"id": expense_id})


@app.route("/api/expenses/<int:id>", methods=["DELETE"])
def delete_expense(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM expenses WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/expense-categories", methods=["GET"])
def get_expense_categories():
    return jsonify(EXPENSE_CATEGORIES)


def generate_po_no():
    conn = get_db()
    c = make_cursor(conn)
    today = datetime.now().strftime("%Y%m%d")
    execute_query(c, "SELECT COUNT(*) as count FROM purchase_orders")
    count = c.fetchone()["count"] + 1
    return f"PO/{today}/{str(count).zfill(4)}"


@app.route("/api/purchase-orders", methods=["GET"])
def get_purchase_orders():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """SELECT po.*, p.name as party_name FROM purchase_orders po
                 LEFT JOIN parties p ON po.party_id = p.id WHERE po.organisation_id = ? ORDER BY po.created_at DESC""",
        (org_id,),
    )
    orders = [dict(row) for row in c.fetchall()]
    return jsonify(orders)


@app.route("/api/purchase-orders/<int:id>", methods=["GET"])
def get_purchase_order(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """SELECT po.*, p.name as party_name, p.gstin as party_gstin, p.state as party_state
                 FROM purchase_orders po LEFT JOIN parties p ON po.party_id = p.id WHERE po.id = ? AND po.organisation_id = ?""",
        (id, org_id),
    )
    row = c.fetchone()
    order = dict(row) if row else None

    if order:
        execute_query(
            c,
            conn,
            """SELECT poi.*, i.name as item_name, i.hsn_code, i.unit FROM purchase_order_items poi
                     LEFT JOIN items i ON poi.item_id = i.id WHERE poi.po_id = ?""",
            (id,),
        )
        order["items"] = [dict(row) for row in c.fetchall()]
    return jsonify(order)


@app.route("/api/purchase-orders", methods=["POST"])
def create_purchase_order():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT state FROM organisations WHERE id = ?", (org_id,))
    result = c.fetchone()
    company_state = result["state"] if result else None

    execute_query(
        c,
        conn,
        "SELECT state FROM parties WHERE id = ? AND organisation_id = ?",
        (data["party_id"], org_id),
    )
    party_row = c.fetchone()
    party_state = party_row["state"] if party_row else None

    po_no = generate_po_no()

    subtotal = 0
    cgst = sgst = igst = 0
    is_inter_state = company_state and party_state and company_state != party_state

    for item in data["items"]:
        amount = item["quantity"] * item["rate"]
        subtotal += amount
        gst_amount = amount * (item["gst_rate"] / 100)

        if is_inter_state:
            igst += gst_amount
        else:
            cgst += gst_amount / 2
            sgst += gst_amount / 2

    total = subtotal + cgst + sgst + igst

    execute_query(
        c,
        conn,
        """INSERT INTO purchase_orders (po_no, party_id, date, delivery_date, subtotal, cgst, sgst, igst, total, notes, is_inter_state, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            po_no,
            data["party_id"],
            data["date"],
            data.get("delivery_date"),
            subtotal,
            cgst,
            sgst,
            igst,
            total,
            data.get("notes"),
            1 if is_inter_state else 0,
            org_id,
        ),
    )
    conn.commit()
    po_id = c.lastrowid

    for item in data["items"]:
        amount = item["quantity"] * item["rate"]
        item_id = item.get("id") or item.get("item_id")
        execute_query(
            c,
            conn,
            """INSERT INTO purchase_order_items (po_id, item_id, quantity, rate, gst_rate, amount)
                     VALUES (?, ?, ?, ?, ?, ?)""",
            (po_id, item_id, item["quantity"], item["rate"], item["gst_rate"], amount),
        )

    conn.commit()
    return jsonify({"id": po_id, "po_no": po_no})


@app.route("/api/purchase-orders/<int:id>", methods=["PUT"])
def update_purchase_order_status(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "UPDATE purchase_orders SET status = ? WHERE id = ? AND organisation_id = ?",
        (data.get("status"), id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/purchase-orders/<int:id>", methods=["DELETE"])
def delete_purchase_order(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM purchase_order_items WHERE po_id IN (SELECT id FROM purchase_orders WHERE id = ? AND organisation_id = ?)",
        (id, org_id),
    )
    execute_query(
        c,
        conn,
        "DELETE FROM purchase_orders WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


def generate_quote_no():
    conn = get_db()
    c = make_cursor(conn)
    today = datetime.now().strftime("%Y%m%d")
    execute_query(c, "SELECT COUNT(*) as count FROM quotations")
    count = c.fetchone()["count"] + 1
    return f"QT/{today}/{str(count).zfill(4)}"


@app.route("/api/quotations", methods=["GET"])
def get_quotations():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """SELECT q.*, p.name as party_name FROM quotations q
                 LEFT JOIN parties p ON q.party_id = p.id WHERE q.organisation_id = ? ORDER BY q.created_at DESC""",
        (org_id,),
    )
    quotes = [dict(row) for row in c.fetchall()]
    return jsonify(quotes)


@app.route("/api/quotations/<int:id>", methods=["GET"])
def get_quotation(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        """SELECT q.*, p.name as party_name, p.gstin as party_gstin, p.state as party_state
                 FROM quotations q LEFT JOIN parties p ON q.party_id = p.id WHERE q.id = ? AND q.organisation_id = ?""",
        (id, org_id),
    )
    row = c.fetchone()
    quote = dict(row) if row else None

    if quote:
        execute_query(
            c,
            conn,
            """SELECT qi.*, i.name as item_name, i.hsn_code, i.unit FROM quotation_items qi
                     LEFT JOIN items i ON qi.item_id = i.id WHERE qi.quote_id = ?""",
            (id,),
        )
        quote["items"] = [dict(row) for row in c.fetchall()]
    return jsonify(quote)


@app.route("/api/quotations", methods=["POST"])
def create_quotation():
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT state FROM organisations WHERE id = ?", (org_id,))
    result = c.fetchone()
    company_state = result["state"] if result else None

    execute_query(
        c,
        conn,
        "SELECT state FROM parties WHERE id = ? AND organisation_id = ?",
        (data["party_id"], org_id),
    )
    party_row = c.fetchone()
    party_state = party_row["state"] if party_row else None

    quote_no = generate_quote_no()

    subtotal = 0
    cgst = sgst = igst = 0
    is_inter_state = company_state and party_state and company_state != party_state

    for item in data["items"]:
        amount = item["quantity"] * item["rate"]
        subtotal += amount
        gst_amount = amount * (item["gst_rate"] / 100)

        if is_inter_state:
            igst += gst_amount
        else:
            cgst += gst_amount / 2
            sgst += gst_amount / 2

    total = subtotal + cgst + sgst + igst

    execute_query(
        c,
        conn,
        """INSERT INTO quotations (quote_no, party_id, date, valid_until, subtotal, cgst, sgst, igst, total, notes, is_inter_state, organisation_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            quote_no,
            data["party_id"],
            data["date"],
            data.get("valid_until"),
            subtotal,
            cgst,
            sgst,
            igst,
            total,
            data.get("notes"),
            1 if is_inter_state else 0,
            org_id,
        ),
    )
    conn.commit()
    quote_id = c.lastrowid

    for item in data["items"]:
        amount = item["quantity"] * item["rate"]
        item_id = item.get("id") or item.get("item_id")
        execute_query(
            c,
            conn,
            """INSERT INTO quotation_items (quote_id, item_id, quantity, rate, gst_rate, amount)
                     VALUES (?, ?, ?, ?, ?, ?)""",
            (
                quote_id,
                item_id,
                item["quantity"],
                item["rate"],
                item["gst_rate"],
                amount,
            ),
        )

    conn.commit()
    return jsonify({"id": quote_id, "quote_no": quote_no})


@app.route("/api/quotations/<int:id>", methods=["PUT"])
def update_quotation_status(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "UPDATE quotations SET status = ? WHERE id = ? AND organisation_id = ?",
        (data.get("status"), id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/quotations/<int:id>", methods=["DELETE"])
def delete_quotation(id):
    user_id = request.headers.get("X-User-Id")
    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "DELETE FROM quotation_items WHERE quote_id IN (SELECT id FROM quotations WHERE id = ? AND organisation_id = ?)",
        (id, org_id),
    )
    execute_query(
        c,
        conn,
        "DELETE FROM quotations WHERE id = ? AND organisation_id = ?",
        (id, org_id),
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/backup/import", methods=["POST"])
def import_backup():
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    try:
        if "settings" in data:
            s = data["settings"]
            execute_query(
                c,
                conn,
                """UPDATE settings SET company_name=?, company_gstin=?, company_pan=?, company_address=?, 
                         company_phone=?, company_email=?, state=?, gst_type=?, bank_name=?, bank_account=?, 
                         bank_ifsc=?, bank_branch=?, default_gst_rate=?, payment_terms=?, footer_note=? WHERE id=1""",
                (
                    s.get("company_name"),
                    s.get("company_gstin"),
                    s.get("company_pan"),
                    s.get("company_address"),
                    s.get("company_phone"),
                    s.get("company_email"),
                    s.get("state"),
                    s.get("gst_type"),
                    s.get("bank_name"),
                    s.get("bank_account"),
                    s.get("bank_ifsc"),
                    s.get("bank_branch"),
                    s.get("default_gst_rate"),
                    s.get("payment_terms"),
                    s.get("footer_note"),
                ),
            )

        for table in [
            "invoice_items",
            "invoices",
            "transactions",
            "expenses",
            "purchase_order_items",
            "purchase_orders",
            "quotation_items",
            "quotations",
            "parties",
            "items",
        ]:
            execute_query(c, f"DELETE FROM {table}")

        if "parties" in data:
            for p in data["parties"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO parties (id, name, type, gstin, pan, phone, email, address, state, city, place_of_supply, opening_balance, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        p.get("id"),
                        p.get("name"),
                        p.get("type"),
                        p.get("gstin"),
                        p.get("pan"),
                        p.get("phone"),
                        p.get("email"),
                        p.get("address"),
                        p.get("state"),
                        p.get("city"),
                        p.get("place_of_supply"),
                        p.get("opening_balance"),
                        p.get("created_at"),
                    ),
                )

        if "items" in data:
            for i in data["items"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO items (id, name, hsn_code, sku, unit, rate, gst_rate, opening_stock, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        i.get("id"),
                        i.get("name"),
                        i.get("hsn_code"),
                        i.get("sku"),
                        i.get("unit"),
                        i.get("rate"),
                        i.get("gst_rate"),
                        i.get("opening_stock"),
                        i.get("created_at"),
                    ),
                )

        if "invoices" in data:
            for inv in data["invoices"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO invoices (id, invoice_no, party_id, type, date, subtotal, cgst, sgst, igst, total, notes, is_inter_state, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        inv.get("id"),
                        inv.get("invoice_no"),
                        inv.get("party_id"),
                        inv.get("type"),
                        inv.get("date"),
                        inv.get("subtotal"),
                        inv.get("cgst"),
                        inv.get("sgst"),
                        inv.get("igst"),
                        inv.get("total"),
                        inv.get("notes"),
                        inv.get("is_inter_state"),
                        inv.get("created_at"),
                    ),
                )

        if "invoice_items" in data:
            for ii in data["invoice_items"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO invoice_items (id, invoice_id, item_id, quantity, rate, gst_rate, amount)
                             VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ii.get("id"),
                        ii.get("invoice_id"),
                        ii.get("item_id"),
                        ii.get("quantity"),
                        ii.get("rate"),
                        ii.get("gst_rate"),
                        ii.get("amount"),
                    ),
                )

        if "transactions" in data:
            for t in data["transactions"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO transactions (id, date, type, party_id, amount, mode, reference_no, description, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        t.get("id"),
                        t.get("date"),
                        t.get("type"),
                        t.get("party_id"),
                        t.get("amount"),
                        t.get("mode"),
                        t.get("reference_no"),
                        t.get("description"),
                        t.get("created_at"),
                    ),
                )

        if "expenses" in data:
            for e in data["expenses"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO expenses (id, date, category, description, amount, gst_rate, cgst, sgst, igst, mode, reference_no, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        e.get("id"),
                        e.get("date"),
                        e.get("category"),
                        e.get("description"),
                        e.get("amount"),
                        e.get("gst_rate"),
                        e.get("cgst"),
                        e.get("sgst"),
                        e.get("igst"),
                        e.get("mode"),
                        e.get("reference_no"),
                        e.get("created_at"),
                    ),
                )

        if "purchase_orders" in data:
            for po in data["purchase_orders"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO purchase_orders (id, po_no, party_id, date, delivery_date, subtotal, cgst, sgst, igst, total, notes, status, is_inter_state, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        po.get("id"),
                        po.get("po_no"),
                        po.get("party_id"),
                        po.get("date"),
                        po.get("delivery_date"),
                        po.get("subtotal"),
                        po.get("cgst"),
                        po.get("sgst"),
                        po.get("igst"),
                        po.get("total"),
                        po.get("notes"),
                        po.get("status"),
                        po.get("is_inter_state"),
                        po.get("created_at"),
                    ),
                )

        if "purchase_order_items" in data:
            for poi in data["purchase_order_items"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO purchase_order_items (id, po_id, item_id, quantity, rate, gst_rate, amount)
                             VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        poi.get("id"),
                        poi.get("po_id"),
                        poi.get("item_id"),
                        poi.get("quantity"),
                        poi.get("rate"),
                        poi.get("gst_rate"),
                        poi.get("amount"),
                    ),
                )

        if "quotations" in data:
            for q in data["quotations"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO quotations (id, quote_no, party_id, date, valid_until, subtotal, cgst, sgst, igst, total, notes, status, is_inter_state, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        q.get("id"),
                        q.get("quote_no"),
                        q.get("party_id"),
                        q.get("date"),
                        q.get("valid_until"),
                        q.get("subtotal"),
                        q.get("cgst"),
                        q.get("sgst"),
                        q.get("igst"),
                        q.get("total"),
                        q.get("notes"),
                        q.get("status"),
                        q.get("is_inter_state"),
                        q.get("created_at"),
                    ),
                )

        if "quotation_items" in data:
            for qi in data["quotation_items"]:
                execute_query(
                    c,
                    conn,
                    """INSERT INTO quotation_items (id, quote_id, item_id, quantity, rate, gst_rate, amount)
                             VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        qi.get("id"),
                        qi.get("quote_id"),
                        qi.get("item_id"),
                        qi.get("quantity"),
                        qi.get("rate"),
                        qi.get("gst_rate"),
                        qi.get("amount"),
                    ),
                )

        conn.commit()
        log_db_operation("IMPORT", "all")
        return jsonify({"success": True})
    except Exception as e:
        log_error(e, "import_backup")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/backup/list", methods=["GET"])
def list_db_backups():
    backups = list_backups()
    return jsonify(backups)


@app.route("/api/backup/restore/<backup_name>", methods=["POST"])
def restore_db_backup(backup_name):
    result = restore_backup(backup_name, DB_NAME)
    return jsonify(result)


@app.route("/api/backup/<backup_name>", methods=["DELETE"])
def delete_db_backup(backup_name):
    result = delete_backup(backup_name)
    return jsonify(result)


@app.route("/api/backup", methods=["POST"])
def create_db_backup():
    data = request.json or {}
    backup_name = data.get("name")
    result = create_backup(DB_NAME, backup_name)
    return jsonify(result)


@app.route("/api/export", methods=["GET"])
def export_data():
    data = export_json(DB_NAME)
    return jsonify(data)


def seed_dummy_data():
    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT id FROM parties")
    party_ids = [row[0] for row in c.fetchall()]
    if not party_ids:
        return

    execute_query(c, "SELECT id FROM items")
    item_ids = [row[0] for row in c.fetchall()]
    if not item_ids:
        return

    execute_query(
        c,
        conn,
        "SELECT MAX(CAST(SUBSTR(invoice_no, 4) AS INTEGER)) FROM invoices WHERE invoice_no LIKE 'INV-%'",
    )
    inv_num = (c.fetchone()[0] or 10000) + 1

    execute_query(
        c,
        conn,
        "SELECT MAX(CAST(SUBSTR(invoice_no, 4) AS INTEGER)) FROM invoices WHERE invoice_no LIKE 'PINV-%'",
    )
    pinv_num = (c.fetchone()[0] or 10000) + 1

    now = datetime.now()

    for i in range(12):
        month_date = now - timedelta(days=30 * i)
        month_str = month_date.strftime("%Y-%m")

        num_invoices = random.randint(3, 8)
        for j in range(num_invoices):
            day_offset = random.randint(1, 28)
            inv_date = f"{month_str}-{day_offset:02d}"

            party_id = random.choice(party_ids)
            execute_query(c, "SELECT state FROM parties WHERE id = ?", (party_id,))
            party_state = c.fetchone()[0]

            execute_query(c, "SELECT state FROM settings LIMIT 1")
            company_state_row = c.fetchone()
            company_state = company_state_row[0] if company_state_row else "Karnataka"

            is_inter = 1 if party_state and party_state != company_state else 0
            tax_rate = 18
            num_items = random.randint(1, 3)
            subtotal = 0

            for _ in range(num_items):
                item_id = random.choice(item_ids)
                execute_query(
                    c, conn, "SELECT rate, gst_rate FROM items WHERE id = ?", (item_id,)
                )
                row = c.fetchone()
                rate = row[0] if row else 100
                gst = row[1] if row else 18
                qty = random.randint(1, 5)
                amount = rate * qty
                subtotal += amount

                execute_query(
                    c,
                    conn,
                    """INSERT INTO invoice_items (invoice_id, item_id, quantity, rate, gst_rate, amount) 
                             VALUES (?, ?, ?, ?, ?, ?)""",
                    (inv_num, item_id, qty, rate, gst, amount),
                )

            if is_inter:
                igst = subtotal * tax_rate / 100
                cgst = sgst = 0
            else:
                igst = 0
                cgst = sgst = subtotal * tax_rate / 200

            total = subtotal + cgst + sgst + igst
            inv_type = random.choice(["sale", "purchase"])
            inv_no = f"INV-{inv_num}" if inv_type == "sale" else f"PINV-{pinv_num}"

            execute_query(
                c,
                conn,
                """INSERT INTO invoices (invoice_no, party_id, type, date, subtotal, cgst, sgst, igst, total, is_inter_state)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    inv_no,
                    party_id,
                    inv_type,
                    inv_date,
                    subtotal,
                    cgst,
                    sgst,
                    igst,
                    total,
                    is_inter,
                ),
            )

            if inv_type == "sale":
                inv_num += 1
            else:
                pinv_num += 1

            amount = random.randint(5000, 50000)
            execute_query(
                c,
                conn,
                """INSERT INTO transactions (date, type, party_id, amount, mode, reference_no, description)
                         VALUES (?, 'receipt', ?, ?, 'cash', ?, '')""",
                (inv_date, party_id, amount, f"RCP-{random.randint(1000, 9999)}"),
            )

            exp_amount = random.randint(1000, 10000)
            category = random.choice(
                ["Rent", "Salary", "Utilities", "Transport", "Office"]
            )
            execute_query(
                c,
                conn,
                """INSERT INTO expenses (date, category, amount, description) 
                         VALUES (?, ?, ?, '')""",
                (inv_date, category, exp_amount),
            )

    conn.commit()
    conn.close()
    logger.info("Dummy data seeded for 12 months")


def rate_limit_login(f):
    if limiter:
        return limiter.limit("10 per minute")(f)
    return f


@app.route("/api/login", methods=["POST"])
@rate_limit_login
def login():
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify(
            {"success": False, "error": "Username and password required"}
        ), 400

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT id, username, password, name, email, role, avatar, is_active, organisation_id FROM users WHERE username = ?",
        (username,),
    )
    user = c.fetchone()

    if not user:
        return jsonify({"success": False, "error": "Invalid credentials"}), 401

    stored_password = user["password"]
    if stored_password.startswith("pbkdf2:") or stored_password.startswith("scrypt:"):
        if not check_password_hash(stored_password, password):
            return jsonify({"success": False, "error": "Invalid credentials"}), 401
    else:
        if stored_password != password:
            return jsonify({"success": False, "error": "Invalid credentials"}), 401
        hashed = generate_password_hash(password)
        execute_query(
            c, conn, "UPDATE users SET password = ? WHERE id = ?", (hashed, user["id"])
        )
        conn.commit()

    if not user["is_active"]:
        return jsonify({"success": False, "error": "Account is deactivated"}), 403

    org_id = user["organisation_id"]
    organisation = None
    if org_id:
        execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
        org = c.fetchone()
        if org:
            organisation = dict(org)

    logger.info(f"User logged in: {username}")
    return jsonify(
        {
            "success": True,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "name": user["name"],
                "email": user["email"],
                "role": user["role"],
                "avatar": user["avatar"],
                "organisation_id": org_id,
            },
            "organisation": organisation,
        }
    )


@app.route("/api/logout", methods=["POST"])
def logout():
    return jsonify({"success": True})


@app.route("/api/validate-session", methods=["GET"])
def validate_session():
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return jsonify({"valid": False, "error": "No user ID"}), 401

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT id, username, name, email, role, avatar, organisation_id, is_active FROM users WHERE id = ?",
        (user_id,),
    )
    user = c.fetchone()

    if not user or not user["is_active"]:
        conn.close()
        return jsonify({"valid": False, "error": "Invalid session"}), 401

    org_id = user["organisation_id"]
    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()

    conn.close()

    return jsonify(
        {
            "valid": True,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "name": user["name"],
                "email": user["email"],
                "role": user["role"],
                "avatar": user["avatar"],
                "organisation_id": org_id,
            },
            "organisation": dict(org) if org else None,
        }
    )


@app.route("/api/organisations", methods=["GET"])
def get_organisations():
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id:
        return jsonify({"success": False, "error": "Not authenticated"}), 401

    conn = get_db()
    c = make_cursor(conn)

    if user_role == "admin":
        execute_query(
            c, "SELECT * FROM organisations WHERE is_active = 1 ORDER BY name"
        )
    else:
        execute_query(
            c,
            conn,
            "SELECT o.* FROM organisations o JOIN users u ON u.organisation_id = o.id WHERE u.id = ? AND o.is_active = 1",
            (user_id,),
        )

    orgs = [dict(row) for row in c.fetchall()]
    return jsonify(orgs)


@app.route("/api/organisations/<int:org_id>", methods=["GET"])
def get_organisation(org_id):
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return jsonify({"success": False, "error": "Not authenticated"}), 401

    conn = get_db()
    c = make_cursor(conn)
    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    conn.close()

    if not org:
        return jsonify({"success": False, "error": "Organisation not found"}), 404

    return jsonify(dict(org))


@app.route("/api/organisations", methods=["POST"])
def create_organisation():
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id or user_role != "admin":
        return jsonify(
            {"success": False, "error": "Only admins can create organisations"}
        ), 403

    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    execute_query(
        c,
        conn,
        """INSERT INTO organisations (name, gstin, pan, phone, email, address, state, city, gst_type, bank_name, bank_account, bank_ifsc, bank_branch, default_gst_rate, payment_terms, footer_note, invoice_template, quotation_template, po_template)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data.get("name"),
            data.get("gstin"),
            data.get("pan"),
            data.get("phone"),
            data.get("email"),
            data.get("address"),
            data.get("state"),
            data.get("city"),
            data.get("gst_type", "regular"),
            data.get("bank_name"),
            data.get("bank_account"),
            data.get("bank_ifsc"),
            data.get("bank_branch"),
            data.get("default_gst_rate", 18),
            data.get("payment_terms"),
            data.get("footer_note"),
            data.get("invoice_template", "classic"),
            data.get("quotation_template", "classic"),
            data.get("po_template", "classic"),
        ),
    )

    org_id = c.lastrowid
    conn.commit()

    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    conn.close()

    return jsonify({"success": True, "organisation": dict(org)})


@app.route("/api/organisations/<int:org_id>", methods=["PUT"])
def update_organisation(org_id):
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id or user_role != "admin":
        return jsonify(
            {"success": False, "error": "Only admins can update organisations"}
        ), 403

    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    fields = []
    values = []
    for key in [
        "name",
        "gstin",
        "pan",
        "phone",
        "email",
        "address",
        "state",
        "city",
        "gst_type",
        "bank_name",
        "bank_account",
        "bank_ifsc",
        "bank_branch",
        "default_gst_rate",
        "payment_terms",
        "footer_note",
        "invoice_template",
        "quotation_template",
        "po_template",
        "is_active",
    ]:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])

    if fields:
        values.append(org_id)
        execute_query(
            c, c, f"UPDATE organisations SET {', '.join(fields)} WHERE id = ?", values
        )
        conn.commit()

    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    conn.close()

    return jsonify({"success": True, "organisation": dict(org)})


@app.route("/api/switch-organisation/<int:org_id>", methods=["POST"])
def switch_organisation(org_id):
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return jsonify({"success": False, "error": "Not authenticated"}), 401

    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT organisation_id FROM users WHERE id = ?", (user_id,))
    user = c.fetchone()

    if user["organisation_id"] != org_id:
        execute_query(
            c,
            conn,
            "SELECT id FROM organisations WHERE id = ? AND is_active = 1",
            (org_id,),
        )
        if not c.fetchone():
            conn.close()
            return jsonify(
                {"success": False, "error": "Organisation not found or access denied"}
            ), 403

    execute_query(
        c, conn, "UPDATE users SET organisation_id = ? WHERE id = ?", (org_id, user_id)
    )
    conn.commit()

    execute_query(c, "SELECT * FROM organisations WHERE id = ?", (org_id,))
    org = c.fetchone()
    conn.close()

    return jsonify({"success": True, "organisation": dict(org)})


@app.route("/api/current-user", methods=["GET"])
def get_current_user():
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return jsonify({"success": False, "error": "Not authenticated"}), 401

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT id, username, name, email, role, avatar FROM users WHERE id = ?",
        (user_id,),
    )
    user = c.fetchone()
    conn.close()

    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    return jsonify(
        {
            "id": user["id"],
            "username": user["username"],
            "name": user["name"],
            "email": user["email"],
            "role": user["role"],
            "avatar": user["avatar"],
        }
    )


@app.route("/api/users", methods=["GET"])
def get_users():
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id or user_role != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    org_id = get_user_org_id(user_id)
    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT id, username, name, email, role, avatar, is_active, created_at FROM users WHERE organisation_id = ?",
        (org_id,),
    )
    users = [dict(row) for row in c.fetchall()]
    return jsonify(users)


@app.route("/api/users", methods=["POST"])
def create_user():
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id or user_role != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    org_id = get_user_org_id(user_id)
    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    try:
        execute_query(
            c,
            conn,
            """INSERT INTO users (username, password, name, email, role, avatar, organisation_id) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                data["username"],
                data["password"],
                data["name"],
                data.get("email"),
                data["role"],
                data.get("avatar"),
                org_id,
            ),
        )
        user_id = c.lastrowid
        conn.commit()
        conn.close()
        return jsonify({"success": True, "id": user_id})
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"success": False, "error": "Username already exists"}), 400


@app.route("/api/users/<int:id>", methods=["PUT"])
def update_user(id):
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    org_id = get_user_org_id(user_id)

    if not user_id or (int(user_id) != id and user_role != "admin"):
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    if int(user_id) == id and user_role != "admin":
        if data.get("avatar"):
            execute_query(
                c,
                conn,
                "UPDATE users SET avatar = ? WHERE id = ? AND organisation_id = ?",
                (data.get("avatar"), id, org_id),
            )
            conn.commit()
            conn.close()
            return jsonify({"success": True})

    if data.get("password"):
        execute_query(
            c,
            conn,
            """UPDATE users SET name=?, email=?, role=?, avatar=?, is_active=?, password=? WHERE id=? AND organisation_id=?""",
            (
                data["name"],
                data.get("email"),
                data["role"],
                data.get("avatar"),
                data.get("is_active", 1),
                data["password"],
                id,
                org_id,
            ),
        )
    else:
        execute_query(
            c,
            conn,
            """UPDATE users SET name=?, email=?, role=?, avatar=?, is_active=? WHERE id=? AND organisation_id=?""",
            (
                data["name"],
                data.get("email"),
                data["role"],
                data.get("avatar"),
                data.get("is_active", 1),
                id,
                org_id,
            ),
        )

    conn.commit()
    return jsonify({"success": True})


@app.route("/api/users/<int:id>/password", methods=["PUT"])
def change_user_password(id):
    user_id = request.headers.get("X-User-Id")
    if not user_id or int(user_id) != id:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    current_password = data.get("currentPassword", "")
    new_password = data.get("newPassword", "")

    if not current_password or not new_password:
        return jsonify({"success": False, "error": "All fields are required"}), 400

    conn = get_db()
    c = make_cursor(conn)

    execute_query(c, "SELECT password FROM users WHERE id = ?", (id,))
    user = c.fetchone()

    if not user or user["password"] != current_password:
        conn.close()
        return jsonify(
            {"success": False, "error": "Current password is incorrect"}
        ), 400

    execute_query(c, "UPDATE users SET password = ? WHERE id = ?", (new_password, id))
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/users/<int:id>", methods=["DELETE"])
def delete_user(id):
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    org_id = get_user_org_id(user_id)
    if not user_id or user_role != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    if int(id) == int(user_id):
        return jsonify({"success": False, "error": "Cannot delete yourself"}), 400

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c, conn, "DELETE FROM users WHERE id = ? AND organisation_id = ?", (id, org_id)
    )
    conn.commit()
    return jsonify({"success": True})


@app.route("/api/module-access", methods=["GET"])
def get_module_access():
    user_role = request.headers.get("X-User-Role")
    if not user_role:
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT module, can_view, can_add, can_edit, can_delete FROM module_access WHERE role = ?",
        (user_role,),
    )
    access = {
        row["module"]: {
            "view": row["can_view"],
            "add": row["can_add"],
            "edit": row["can_edit"],
            "delete": row["can_delete"],
        }
        for row in c.fetchall()
    }
    return jsonify(access)


@app.route("/api/module-access", methods=["PUT"])
def update_module_access():
    user_id = request.headers.get("X-User-Id")
    user_role = request.headers.get("X-User-Role")
    if not user_id or user_role != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    conn = get_db()
    c = make_cursor(conn)

    for module, perms in data.items():
        execute_query(
            c,
            conn,
            """INSERT OR REPLACE INTO module_access (role, module, can_view, can_add, can_edit, can_delete) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
            (
                data["role"],
                module,
                perms.get("view", 0),
                perms.get("add", 0),
                perms.get("edit", 0),
                perms.get("delete", 0),
            ),
        )

    conn.commit()
    return jsonify({"success": True})


@app.route("/api/module-access/<role>", methods=["GET"])
def get_role_access(role):
    user_role = request.headers.get("X-User-Role")
    if not user_role or user_role != "admin":
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    conn = get_db()
    c = make_cursor(conn)
    execute_query(
        c,
        conn,
        "SELECT module, can_view, can_add, can_edit, can_delete FROM module_access WHERE role = ?",
        (role,),
    )
    access = {
        row["module"]: {
            "view": row["can_view"],
            "add": row["can_add"],
            "edit": row["can_edit"],
            "delete": row["can_delete"],
        }
        for row in c.fetchall()
    }
    return jsonify(access)


@app.route("/logo/<path:filename>")
def serve_logo(filename):
    return send_from_directory("public/logo", filename)


@app.route("/public/<path:filename>")
def serve_public(filename):
    return send_from_directory("public", filename)


if __name__ == "__main__":
    import sys

    if "--init" in sys.argv:
        init_db()
        print("Database initialized.")
    else:
        init_db()

        conn = get_db()
        c = make_cursor(conn)
        execute_query(c, "SELECT COUNT(*) FROM invoices")
        if c.fetchone()[0] < 50:
            seed_dummy_data()
        conn.close()

        import logging

        log = logging.getLogger("werkzeug")
        log.setLevel(logging.WARNING)

        logger.info("ProERP starting at http://localhost:3000")
        app.run(port=3000, debug=False, use_reloader=False)
