from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import logging
import stripe
import uuid
import secrets
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import BytesIO
import re
import qrcode


def _load_local_env():
    """Minimal .env loader so local SMTP settings work without extra packages."""
    candidates = [
        Path(__file__).resolve().parent / '.env',
        Path(__file__).resolve().parent.parent / '.env',
        Path.cwd() / '.env',
    ]
    for env_path in candidates:
        try:
            if not env_path.exists():
                continue
            for raw_line in env_path.read_text(encoding='utf-8').splitlines():
                line = raw_line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
            return
        except Exception:
            continue


_load_local_env()

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, ForeignKey, text, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from app.core.config import (
    STRIPE_WEBHOOK_SECRET, STRIPE_SECRET_KEY, DATABASE_URL
)

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

app = FastAPI(title="QUINGAPP API")

# CORS for Flutter Web / Chrome POS testing
CORS_ALLOW_ORIGINS_RAW = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
if CORS_ALLOW_ORIGINS_RAW == "*":
    _cors_allow_origins = ["*"]
else:
    _cors_allow_origins = [
        origin.strip()
        for origin in CORS_ALLOW_ORIGINS_RAW.split(",")
        if origin.strip()
    ] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger("quingapp")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Stripe
stripe.api_key = STRIPE_SECRET_KEY
STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "").strip()
STRIPE_EPHEMERAL_KEY_API_VERSION = os.getenv("STRIPE_EPHEMERAL_KEY_API_VERSION", "2024-06-20").strip()

# -------------------------
# Admin token (TEMP)
# -------------------------
# Set this in Render ENV as ADMIN_TOKEN=<any strong string>
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()


def _require_admin(request: Request):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not configured")
    token = (request.headers.get("x-admin-token") or request.query_params.get("token") or "").strip()
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _dt(v):
    return v.isoformat() if v else None


# -------------------------
# Database
# -------------------------
Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(32), nullable=False, default="PENDING_PAYMENT")  # PENDING_PAYMENT | PAID | DELIVERED | EXPIRED | CANCELLED

    customer_name = Column(String(200), nullable=False, default="")
    customer_phone = Column(String(50), nullable=False, default="")
    # ✅ NEW: email to send sales note (nota de venta)
    customer_email = Column(String(254), nullable=False, default="")

    total_mxn = Column(Integer, nullable=False, default=0)  # stored in MXN pesos (int)

    created_at = Column(DateTime(timezone=True), nullable=False)
    paid_at = Column(DateTime(timezone=True), nullable=True)

    expires_at = Column(DateTime(timezone=True), nullable=True)     # paid_at + 30d
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    pickup_status = Column(String(32), nullable=False, default="PENDING")  # PENDING | ACTIVE | DELIVERED | EXPIRED
    pickup_code = Column(String(32), nullable=False, default="", unique=True)
    pickup_token = Column(String(64), nullable=False, default="", unique=True)

    # ✅ NEW: sales note send tracking
    note_sent_at = Column(DateTime(timezone=True), nullable=True)
    note_status = Column(String(32), nullable=False, default="PENDING")  # PENDING | SENT | FAILED | SKIPPED
    note_error = Column(String(500), nullable=False, default="")
    inventory_deducted_at = Column(DateTime(timezone=True), nullable=True)

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    payments = relationship("Payment", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)

    name = Column(String(250), nullable=False)
    sku = Column(String(80), nullable=False, default="")
    qty = Column(Integer, nullable=False, default=1)
    unit_amount_mxn = Column(Integer, nullable=False, default=0)  # pesos

    order = relationship("Order", back_populates="items")


class Payment(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)

    stripe_session_id = Column(String(200), nullable=False)
    payment_status = Column(String(50), nullable=True)
    amount_total_cents = Column(Integer, nullable=True)
    currency = Column(String(10), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False)

    order = relationship("Order", back_populates="payments")


class InventoryItem(Base):
    __tablename__ = "inventory_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    school_code = Column(String(20), nullable=False, default="")
    product_type = Column(String(20), nullable=False, default="")
    size = Column(String(20), nullable=False, default="")
    gender = Column(String(10), nullable=True)
    sku = Column(String(80), nullable=False, unique=True)
    barcode = Column(String(80), nullable=False, unique=True)
    stock = Column(Integer, nullable=False, default=0)
    price_mxn = Column(Integer, nullable=False, default=0)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


engine = None
SessionLocal = None

if DATABASE_URL:
    # Render Postgres URLs are often postgres:// ; SQLAlchemy needs postgresql://
    db_url = DATABASE_URL
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}

    engine = create_engine(db_url, pool_pre_ping=True, future=True, connect_args=connect_args)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS customer_email VARCHAR(254) NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS note_sent_at TIMESTAMPTZ NULL"))
            conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS note_status VARCHAR(32) NOT NULL DEFAULT 'PENDING'"))
            conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS note_error VARCHAR(500) NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS inventory_deducted_at TIMESTAMPTZ NULL"))
            conn.execute(text("ALTER TABLE order_items ADD COLUMN IF NOT EXISTS sku VARCHAR(80) NOT NULL DEFAULT ''"))
    except Exception:
        logger.exception("runtime schema sync failed")
else:
    logger.warning("DATABASE_URL not configured — DB endpoints will fail until configured.")


class PosSale(Base):
    __tablename__ = "pos_sales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    folio = Column(String(64), nullable=False, unique=True)
    payment_method = Column(String(32), nullable=False, default="CASH")
    employee_name = Column(String(120), nullable=False, default="")
    items_count = Column(Integer, nullable=False, default=0)
    total_mxn = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False)

    items = relationship("PosSaleItem", back_populates="sale", cascade="all, delete-orphan")


class PosSaleItem(Base):
    __tablename__ = "pos_sale_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sale_id = Column(Integer, ForeignKey("pos_sales.id"), nullable=False)

    sku = Column(String(80), nullable=False, default="")
    barcode = Column(String(80), nullable=False, default="")
    name = Column(String(250), nullable=False, default="")
    qty = Column(Integer, nullable=False, default=1)
    unit_price_mxn = Column(Integer, nullable=False, default=0)
    line_total_mxn = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False)

    sale = relationship("PosSale", back_populates="items")


class PartnerAccount(Base):
    __tablename__ = "partner_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    school_code = Column(String(20), nullable=False, unique=True)
    school_name = Column(String(120), nullable=False, default="")
    partner_name = Column(String(120), nullable=False, default="")
    access_code = Column(String(80), nullable=False, unique=True)
    commission_per_item = Column(Integer, nullable=False, default=40)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    history = relationship("PartnerCommissionHistory", back_populates="partner", cascade="all, delete-orphan")


class PartnerCommissionHistory(Base):
    __tablename__ = "partner_commission_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    partner_id = Column(Integer, ForeignKey("partner_accounts.id"), nullable=False)
    school_code = Column(String(20), nullable=False)
    items_sold = Column(Integer, nullable=False, default=0)
    commission_total_mxn = Column(Integer, nullable=False, default=0)
    paid_from_at = Column(DateTime(timezone=True), nullable=True)
    paid_to_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)

    partner = relationship("PartnerAccount", back_populates="history")


if engine:
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        logger.exception("runtime metadata sync failed for partner tables")


# -------------------------
# Helpers
# -------------------------
def _now_utc():
    return datetime.now(timezone.utc)


def _pickup_qr_payload(pickup_token: str) -> str:
    """What goes INSIDE the QR."""
    pickup_token = (pickup_token or "").strip()
    return f"quingapp://pickup?token={pickup_token}"


def _is_valid_email(email: str) -> bool:
    email = (email or "").strip()
    if not email or len(email) > 254:
        return False
    # Simple validation (good enough for MVP)
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email) is not None


def _normalize_code(value: str, upper: bool = True) -> str:
    value = str(value or "").strip()
    if upper:
        value = value.upper()
    value = re.sub(r"\s+", "", value)
    return value


def _normalize_size(value: str) -> str:
    value = _normalize_code(value, upper=True)
    return value


def _normalize_gender(value: str):
    value = _normalize_code(value, upper=True)
    return value or None




def _normalize_sizes_list(values):
    if not isinstance(values, list) or len(values) == 0:
        raise HTTPException(status_code=400, detail="sizes must be a non-empty list")

    cleaned = []
    seen = set()
    for value in values:
        size = _normalize_size(value)
        if not size:
            raise HTTPException(status_code=400, detail="sizes contains invalid value")
        if size in seen:
            continue
        seen.add(size)
        cleaned.append(size)

    if not cleaned:
        raise HTTPException(status_code=400, detail="sizes must be a non-empty list")

    return cleaned

def _build_inventory_sku(school_code: str, product_type: str, size: str, gender: str = None) -> str:
    school_code = _normalize_code(school_code, upper=True)
    product_type = _normalize_code(product_type, upper=True)
    size = _normalize_size(size)
    gender = _normalize_gender(gender)

    if not school_code:
        raise HTTPException(status_code=400, detail="school_code is required")
    if not product_type:
        raise HTTPException(status_code=400, detail="product_type is required")
    if not size:
        raise HTTPException(status_code=400, detail="size is required")

    if gender:
        return f"{school_code}-{product_type}-{gender}-{size}"
    return f"{school_code}-{product_type}-{size}"


def _inventory_title_for_sale(item: InventoryItem):
    parts = []
    if str(item.school_code or "").strip():
        parts.append(str(item.school_code).strip())
    if str(item.product_type or "").strip():
        parts.append(str(item.product_type).strip())
    if str(item.size or "").strip():
        parts.append(f"Talla {str(item.size).strip()}")
    return " • ".join(parts) if parts else str(item.sku or "").strip()


def _inventory_to_dict(item: InventoryItem):
    return {
        "id": item.id,
        "school_code": item.school_code,
        "product_type": item.product_type,
        "size": item.size,
        "gender": item.gender,
        "sku": item.sku,
        "barcode": item.barcode,
        "stock": item.stock,
        "price_mxn": item.price_mxn,
        "active": bool(item.active),
        "created_at": _dt(item.created_at),
        "updated_at": _dt(item.updated_at),
    }


def _validate_inventory_payload(payload: dict, partial: bool = False):
    payload = payload or {}

    school_code = payload.get("school_code")
    product_type = payload.get("product_type")
    size = payload.get("size")
    gender = payload.get("gender")
    stock = payload.get("stock")
    price_mxn = payload.get("price_mxn")
    active = payload.get("active")
    barcode = payload.get("barcode")

    data = {}

    if not partial or school_code is not None:
        school_code = _normalize_code(school_code, upper=True)
        if not school_code:
            raise HTTPException(status_code=400, detail="school_code is required")
        data["school_code"] = school_code

    if not partial or product_type is not None:
        product_type = _normalize_code(product_type, upper=True)
        if not product_type:
            raise HTTPException(status_code=400, detail="product_type is required")
        data["product_type"] = product_type

    if not partial or size is not None:
        size = _normalize_size(size)
        if not size:
            raise HTTPException(status_code=400, detail="size is required")
        data["size"] = size

    if gender is not None or not partial:
        data["gender"] = _normalize_gender(gender)

    if not partial or stock is not None:
        try:
            stock = int(stock if stock is not None else 0)
        except Exception:
            raise HTTPException(status_code=400, detail="stock must be integer")
        if stock < 0:
            raise HTTPException(status_code=400, detail="stock must be >= 0")
        data["stock"] = stock

    if not partial or price_mxn is not None:
        try:
            price_mxn = int(round(float(price_mxn if price_mxn is not None else 0)))
        except Exception:
            raise HTTPException(status_code=400, detail="price_mxn must be numeric")
        if price_mxn < 0:
            raise HTTPException(status_code=400, detail="price_mxn must be >= 0")
        data["price_mxn"] = price_mxn

    if active is not None or not partial:
        if isinstance(active, bool):
            data["active"] = active
        else:
            active_text = str(active if active is not None else "true").strip().lower()
            data["active"] = active_text in ("1", "true", "yes", "y", "on")

    if barcode is not None:
        barcode = _normalize_code(barcode, upper=True)
        data["barcode"] = barcode or None

    return data


def _apply_inventory_fields(item: InventoryItem, data: dict):
    school_code = data.get("school_code", item.school_code)
    product_type = data.get("product_type", item.product_type)
    size = data.get("size", item.size)
    gender = data.get("gender", item.gender)

    sku = _build_inventory_sku(school_code, product_type, size, gender)

    item.school_code = school_code
    item.product_type = product_type
    item.size = size
    item.gender = gender
    item.sku = sku
    item.barcode = data.get("barcode") or sku

    if "stock" in data:
        item.stock = int(data["stock"])
    if "price_mxn" in data:
        item.price_mxn = int(data["price_mxn"])
    if "active" in data:
        item.active = bool(data["active"])

    item.updated_at = _now_utc()
    if not getattr(item, "created_at", None):
        item.created_at = _now_utc()

    return item


def _gen_pos_folio():
    return "POS-" + _now_utc().strftime("%y%m%d-%H%M%S") + "-" + secrets.token_hex(2).upper()


def _pos_sale_item_to_dict(it: "PosSaleItem"):
    return {
        "id": it.id,
        "sku": it.sku,
        "barcode": it.barcode,
        "name": it.name,
        "qty": int(it.qty or 0),
        "unit_price_mxn": int(it.unit_price_mxn or 0),
        "line_total_mxn": int(it.line_total_mxn or 0),
        "created_at": _dt(it.created_at),
    }


def _pos_sale_to_dict(sale: "PosSale"):
    return {
        "id": sale.id,
        "folio": sale.folio,
        "payment_method": sale.payment_method,
        "employee_name": sale.employee_name,
        "items_count": int(sale.items_count or 0),
        "total_mxn": int(sale.total_mxn or 0),
        "created_at": _dt(sale.created_at),
        "items": [_pos_sale_item_to_dict(it) for it in (sale.items or [])],
    }


def _start_of_today_utc():
    now = _now_utc()
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)


def _start_of_month_utc():
    now = _now_utc()
    return datetime(now.year, now.month, 1, tzinfo=timezone.utc)


def _partner_account_to_dict(partner: "PartnerAccount"):
    return {
        "id": partner.id,
        "school_code": partner.school_code,
        "school_name": partner.school_name,
        "partner_name": partner.partner_name,
        "access_code": partner.access_code,
        "commission_per_item": int(partner.commission_per_item or 0),
        "active": bool(partner.active),
        "created_at": _dt(partner.created_at),
        "updated_at": _dt(partner.updated_at),
    }


def _partner_history_to_dict(row: "PartnerCommissionHistory"):
    return {
        "id": row.id,
        "partner_id": row.partner_id,
        "school_code": row.school_code,
        "items_sold": int(row.items_sold or 0),
        "commission_total_mxn": int(row.commission_total_mxn or 0),
        "paid_from_at": _dt(row.paid_from_at),
        "paid_to_at": _dt(row.paid_to_at),
        "created_at": _dt(row.created_at),
    }


def _extract_school_code_from_sku(sku: str) -> str:
    sku = str(sku or "").strip().upper()
    if not sku:
        return ""
    return sku.split("-", 1)[0].strip()


def _partner_last_paid_to(db, partner: "PartnerAccount"):
    row = (
        db.query(PartnerCommissionHistory)
        .filter(PartnerCommissionHistory.partner_id == int(partner.id))
        .order_by(PartnerCommissionHistory.paid_to_at.desc(), PartnerCommissionHistory.id.desc())
        .first()
    )
    return row.paid_to_at if row else None


def _partner_collect_sales_rows(db, partner: "PartnerAccount"):
    school_code = str(partner.school_code or "").strip().upper()
    if not school_code:
        return []

    rows = []

    paid_orders = db.query(Order).filter(Order.status == "PAID").all()
    for order in paid_orders:
        event_at = order.paid_at or order.created_at
        for item in (order.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            if _extract_school_code_from_sku(sku) != school_code:
                continue
            rows.append({
                "source": "ONLINE",
                "event_at": event_at,
                "order_id": order.id,
                "folio": None,
                "sku": sku,
                "name": getattr(item, "name", "") or sku,
                "qty": int(getattr(item, "qty", 0) or 0),
            })

    pos_sales = db.query(PosSale).order_by(PosSale.created_at.asc()).all()
    for sale in pos_sales:
        event_at = sale.created_at
        for item in (sale.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            if _extract_school_code_from_sku(sku) != school_code:
                continue
            rows.append({
                "source": "POS",
                "event_at": event_at,
                "order_id": None,
                "folio": sale.folio,
                "sku": sku,
                "name": getattr(item, "name", "") or sku,
                "qty": int(getattr(item, "qty", 0) or 0),
            })

    rows.sort(key=lambda x: (x["event_at"] or datetime(1970, 1, 1, tzinfo=timezone.utc), x["source"]))
    return rows


def _partner_build_dashboard(db, partner: "PartnerAccount"):
    rows = _partner_collect_sales_rows(db, partner)
    now = _now_utc()
    start_today = _start_of_today_utc()
    start_month = _start_of_month_utc()
    last_paid_to = _partner_last_paid_to(db, partner)
    commission = int(partner.commission_per_item or 0)

    items_today = 0
    items_month = 0
    pending_items = 0
    pending_rows = []
    product_rows = {}
    latest_pending_at = None

    for row in rows:
        qty = int(row["qty"] or 0)
        event_at = row["event_at"]
        if event_at and event_at >= start_today:
            items_today += qty
        if event_at and event_at >= start_month:
            items_month += qty
        is_pending = True
        if last_paid_to and event_at:
            is_pending = event_at > last_paid_to
        elif last_paid_to and event_at is None:
            is_pending = False

        if is_pending:
            pending_items += qty
            pending_rows.append(row)
            product_key = row["name"] or row["sku"]
            bucket = product_rows.setdefault(product_key, {"name": product_key, "qty": 0})
            bucket["qty"] += qty
            if event_at and (latest_pending_at is None or event_at > latest_pending_at):
                latest_pending_at = event_at

    history_rows = (
        db.query(PartnerCommissionHistory)
        .filter(PartnerCommissionHistory.partner_id == int(partner.id))
        .order_by(PartnerCommissionHistory.created_at.desc(), PartnerCommissionHistory.id.desc())
        .limit(30)
        .all()
    )

    pending_rows_out = []
    for row in pending_rows[-50:][::-1]:
        pending_rows_out.append({
            "source": row["source"],
            "event_at": _dt(row["event_at"]),
            "order_id": row["order_id"],
            "folio": row["folio"],
            "sku": row["sku"],
            "name": row["name"],
            "qty": int(row["qty"] or 0),
            "commission_mxn": int(row["qty"] or 0) * commission,
        })

    product_rows_out = sorted(
        (
            {
                "name": value["name"],
                "qty": int(value["qty"] or 0),
                "commission_mxn": int(value["qty"] or 0) * commission,
            }
            for value in product_rows.values()
        ),
        key=lambda x: (-x["qty"], x["name"]),
    )

    return {
        "partner": _partner_account_to_dict(partner),
        "summary": {
            "school_code": partner.school_code,
            "school_name": partner.school_name,
            "partner_name": partner.partner_name,
            "commission_per_item": commission,
            "items_today": items_today,
            "items_month": items_month,
            "pending_items": pending_items,
            "pending_commission_mxn": pending_items * commission,
            "last_paid_to_at": _dt(last_paid_to),
            "latest_pending_at": _dt(latest_pending_at),
        },
        "pending_rows": pending_rows_out,
        "product_rows": product_rows_out,
        "history": [_partner_history_to_dict(row) for row in history_rows],
    }


def _partner_find_by_code(db, code: str):
    code = _normalize_code(code, upper=True)
    if not code:
        return None
    return (
        db.query(PartnerAccount)
        .filter(PartnerAccount.access_code == code, PartnerAccount.active == True)
        .first()
    )


def _order_to_dict(o: Order):
    return {
        "id": o.id,
        "status": o.status,
        "customer_name": o.customer_name,
        "customer_phone": o.customer_phone,
        "customer_email": o.customer_email,
        "total_mxn": o.total_mxn,
        "created_at": _dt(o.created_at),
        "paid_at": _dt(o.paid_at),
        "expires_at": _dt(o.expires_at),
        "delivered_at": _dt(o.delivered_at),
        "pickup_status": o.pickup_status,
        "pickup_code": o.pickup_code,
        "pickup_token": o.pickup_token,
        "note_sent_at": _dt(o.note_sent_at),
        "note_status": o.note_status,
        "note_error": o.note_error,
        "inventory_deducted_at": _dt(o.inventory_deducted_at),
        "items": [
            {"id": it.id, "name": it.name, "sku": it.sku, "qty": it.qty, "unit_amount_mxn": it.unit_amount_mxn}
            for it in (o.items or [])
        ],
        "payments": [
            {
                "id": p.id,
                "stripe_session_id": p.stripe_session_id,
                "payment_status": p.payment_status,
                "amount_total_cents": p.amount_total_cents,
                "currency": p.currency,
                "created_at": _dt(p.created_at),
            }
            for p in (o.payments or [])
        ],
    }


def _order_public_to_dict(o: Order):
    """Subset for customer-facing calls."""
    return {
        "id": o.id,
        "status": o.status,
        "customer_name": o.customer_name,
        "customer_email": o.customer_email,
        "total_mxn": o.total_mxn,
        "created_at": _dt(o.created_at),
        "paid_at": _dt(o.paid_at),
        "expires_at": _dt(o.expires_at),
        "delivered_at": _dt(o.delivered_at),
        "pickup_status": o.pickup_status,
        "pickup_code": o.pickup_code,
        "pickup_token": o.pickup_token,
        "note_sent_at": _dt(o.note_sent_at),
        "note_status": o.note_status,
        "inventory_deducted_at": _dt(o.inventory_deducted_at),
        "items": [
            {"name": it.name, "sku": it.sku, "qty": it.qty, "unit_amount_mxn": it.unit_amount_mxn}
            for it in (o.items or [])
        ],
    }


def _maybe_mark_expired(db, o: Order) -> bool:
    if o.status in ("DELIVERED", "CANCELLED", "EXPIRED"):
        return False
    if o.pickup_status == "DELIVERED":
        return False

    if o.expires_at and _now_utc() > o.expires_at:
        o.status = "EXPIRED"
        o.pickup_status = "EXPIRED"
        db.add(o)
        return True
    return False



def _deduct_inventory_for_paid_order(db, order: Order):
    if not order:
        return False

    if getattr(order, "inventory_deducted_at", None):
        return False

    order_items = list(order.items or [])
    order_items_with_sku = [it for it in order_items if str(getattr(it, "sku", "") or "").strip()]
    if not order_items_with_sku:
        return False

    inventory_rows = []
    for it in order_items_with_sku:
        sku = str(it.sku or "").strip().upper()
        inv = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
        if not inv:
            raise HTTPException(status_code=400, detail=f"Inventory SKU not found for paid order: {sku}")
        if not bool(inv.active):
            raise HTTPException(status_code=400, detail=f"Inventory SKU inactive for paid order: {sku}")
        if int(inv.stock or 0) < int(it.qty or 0):
            raise HTTPException(status_code=400, detail=f"Insufficient stock for paid order SKU: {sku}")
        inventory_rows.append((inv, int(it.qty or 0)))

    for inv, qty in inventory_rows:
        inv.stock = int(inv.stock or 0) - qty
        inv.updated_at = _now_utc()
        db.add(inv)

    order.inventory_deducted_at = _now_utc()
    db.add(order)
    return True


def _get_or_create_payment_record(db, order_id: int, stripe_session_id: str):
    payment = (
        db.query(Payment)
        .filter(Payment.order_id == order_id, Payment.stripe_session_id == str(stripe_session_id or ""))
        .first()
    )
    if not payment:
        payment = Payment(
            order_id=order_id,
            stripe_session_id=str(stripe_session_id or ""),
            created_at=_now_utc(),
        )
        db.add(payment)
    return payment


def _record_payment_snapshot(db, order: Order, stripe_session_id: str):
    """
    Idempotent snapshot sync from Stripe Checkout Session into local payment/order fields.
    Does NOT send emails. Does NOT create duplicate payments.
    """
    if not stripe_session_id:
        raise ValueError("stripe_session_id is required")

    session = stripe.checkout.Session.retrieve(stripe_session_id)
    payment = _get_or_create_payment_record(db, order.id, stripe_session_id)

    payment.payment_status = getattr(session, "payment_status", None)
    payment.amount_total_cents = getattr(session, "amount_total", None)
    payment.currency = getattr(session, "currency", None)

    if getattr(session, "payment_status", None) == "paid":
        if order.status != "PAID":
            order.status = "PAID"
        if not order.paid_at:
            order.paid_at = _now_utc()
        if not order.expires_at:
            order.expires_at = order.paid_at + timedelta(days=60)
        if order.pickup_status != "DELIVERED":
            order.pickup_status = "ACTIVE"
        _deduct_inventory_for_paid_order(db, order)

    db.add(order)
    db.add(payment)
    return session, payment


def _mark_order_paid_from_session(db, stripe_session_id: str):
    """
    Old helper kept for webhook path compatibility.
    Marks PAID using Stripe session snapshot and returns (order, payment).
    """
    payment = db.query(Payment).filter(Payment.stripe_session_id == str(stripe_session_id or "")).first()
    if not payment:
        return None, None

    order = db.query(Order).filter(Order.id == payment.order_id).first()
    if not order:
        return None, None

    _, payment = _record_payment_snapshot(db, order, stripe_session_id)
    return order, payment


def _gen_pickup_code():
    return "QNG-" + secrets.token_hex(3).upper()


def _gen_pickup_token():
    return uuid.uuid4().hex


def _ensure_unique_pickup_fields(db):
    """
    Returns (pickup_code, pickup_token) unique in DB.
    """
    while True:
        code = _gen_pickup_code()
        token = _gen_pickup_token()

        exists = (
            db.query(Order)
            .filter((Order.pickup_code == code) | (Order.pickup_token == token))
            .first()
        )
        if not exists:
            return code, token


def _validate_checkout_payload(payload: dict):
    items = payload.get("items") or []
    if not isinstance(items, list) or len(items) == 0:
        raise HTTPException(status_code=400, detail="items is required")

    customer_name = str(payload.get("customer_name", "")).strip()
    customer_phone = str(payload.get("customer_phone", "")).strip()
    customer_email = str(payload.get("customer_email", "")).strip()

    if not customer_name:
        raise HTTPException(status_code=400, detail="customer_name is required")

    if customer_email and not _is_valid_email(customer_email):
        raise HTTPException(status_code=400, detail="customer_email invalid")

    total_mxn = 0
    cleaned_items = []
    line_items = []

    for it in items:
        name = str(it.get("name", "")).strip()
        sku = _normalize_code(it.get("sku"), upper=True)
        qty = int(it.get("qty", 0) or 0)
        unit_amount_mxn = float(it.get("unit_amount_mxn", it.get("unit_price_mxn", 0)) or 0)

        if not name or qty <= 0 or unit_amount_mxn <= 0:
            raise HTTPException(status_code=400, detail="Invalid item in items")

        unit_amount_pesos = int(round(unit_amount_mxn))
        total_mxn += unit_amount_pesos * qty

        cleaned_items.append({"name": name, "sku": sku, "qty": qty, "unit_amount_mxn": unit_amount_pesos})

        line_items.append({
            "price_data": {
                "currency": "mxn",
                "product_data": {"name": name},
                "unit_amount": unit_amount_pesos * 100,
            },
            "quantity": qty,
        })

    return {
        "customer_name": customer_name,
        "customer_phone": customer_phone,
        "customer_email": customer_email,
        "total_mxn": total_mxn,
        "cleaned_items": cleaned_items,
        "line_items": line_items,
    }


def _create_pending_order(db, checkout_data: dict):
    pickup_code, pickup_token = _ensure_unique_pickup_fields(db)

    order = Order(
        status="PENDING_PAYMENT",
        customer_name=checkout_data["customer_name"],
        customer_phone=checkout_data["customer_phone"],
        customer_email=checkout_data["customer_email"],
        total_mxn=checkout_data["total_mxn"],
        created_at=_now_utc(),
        pickup_status="PENDING",
        pickup_code=pickup_code,
        pickup_token=pickup_token,
        note_status="PENDING" if checkout_data["customer_email"] else "SKIPPED",
    )
    db.add(order)
    db.flush()

    for it in checkout_data["cleaned_items"]:
        db.add(OrderItem(
            order_id=order.id,
            name=it["name"],
            sku=it.get("sku", ""),
            qty=it["qty"],
            unit_amount_mxn=it["unit_amount_mxn"],
        ))

    return order, pickup_code, pickup_token


def _mark_order_paid_from_payment_intent(db, payment_intent_id: str):
    payment = db.query(Payment).filter(Payment.stripe_session_id == str(payment_intent_id or "")).first()
    if payment:
        order = db.query(Order).filter(Order.id == payment.order_id).first()
    else:
        order = None

    if not order:
        pi = stripe.PaymentIntent.retrieve(payment_intent_id)
        order_id = None
        metadata = getattr(pi, "metadata", None) or {}
        try:
            order_id = int(str(metadata.get("order_id", "")).strip() or "0")
        except Exception:
            order_id = None

        if not order_id:
            return None, None

        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            return None, None

        payment = _get_or_create_payment_record(db, order.id, payment_intent_id)

    pi = stripe.PaymentIntent.retrieve(payment_intent_id)
    payment.payment_status = getattr(pi, "status", None)
    payment.amount_total_cents = getattr(pi, "amount", None)
    payment.currency = getattr(pi, "currency", None)

    if getattr(pi, "status", None) == "succeeded":
        if order.status != "PAID":
            order.status = "PAID"
        if not order.paid_at:
            order.paid_at = _now_utc()
        if not order.expires_at:
            order.expires_at = order.paid_at + timedelta(days=60)
        if order.pickup_status != "DELIVERED":
            order.pickup_status = "ACTIVE"
        _deduct_inventory_for_paid_order(db, order)

    db.add(order)
    db.add(payment)
    return order, payment


# -------------------------
# Root
# -------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "QUINGAPP backend",
        "docs": "/docs",
    }


# -------------------------
# Inventory admin endpoints
# -------------------------


@app.get("/admin/setup-pos")
def admin_setup_pos(request: Request):
    _require_admin(request)
    if not engine:
        raise HTTPException(status_code=500, detail="Database not configured")

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pos_sales (
                    id SERIAL PRIMARY KEY,
                    folio VARCHAR(64) NOT NULL UNIQUE,
                    payment_method VARCHAR(32) NOT NULL DEFAULT 'CASH',
                    employee_name VARCHAR(120) NOT NULL DEFAULT '',
                    items_count INTEGER NOT NULL DEFAULT 0,
                    total_mxn INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pos_sale_items (
                    id SERIAL PRIMARY KEY,
                    sale_id INTEGER NOT NULL REFERENCES pos_sales(id) ON DELETE CASCADE,
                    sku VARCHAR(80) NOT NULL DEFAULT '',
                    barcode VARCHAR(80) NOT NULL DEFAULT '',
                    name VARCHAR(250) NOT NULL DEFAULT '',
                    qty INTEGER NOT NULL DEFAULT 1,
                    unit_price_mxn INTEGER NOT NULL DEFAULT 0,
                    line_total_mxn INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL
                )
            """))

        return {
            "ok": True,
            "message": "POS tables ready",
            "tables": ["pos_sales", "pos_sale_items"],
        }
    except Exception as e:
        logger.exception("admin_setup_pos failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/inventory")
def admin_list_inventory(request: Request, limit: int = 100, active_only: bool = False, school_code: str = "", product_type: str = ""):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    limit = max(1, min(int(limit or 100), 500))
    school_code = _normalize_code(school_code, upper=True)
    product_type = _normalize_code(product_type, upper=True)

    db = SessionLocal()
    try:
        q = db.query(InventoryItem)

        if active_only:
            q = q.filter(InventoryItem.active == True)
        if school_code:
            q = q.filter(InventoryItem.school_code == school_code)
        if product_type:
            q = q.filter(InventoryItem.product_type == product_type)

        items = q.order_by(InventoryItem.id.desc()).limit(limit).all()
        return {"ok": True, "count": len(items), "items": [_inventory_to_dict(x) for x in items]}
    finally:
        db.close()


@app.post("/admin/inventory/bulk-create")
def admin_bulk_create_inventory(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}

    school_code = _normalize_code(payload.get("school_code"), upper=True)
    product_type = _normalize_code(payload.get("product_type"), upper=True)
    gender = _normalize_gender(payload.get("gender"))
    sizes = _normalize_sizes_list(payload.get("sizes"))

    if not school_code:
        raise HTTPException(status_code=400, detail="school_code is required")
    if not product_type:
        raise HTTPException(status_code=400, detail="product_type is required")

    try:
        stock = int(payload.get("stock", 0))
    except Exception:
        raise HTTPException(status_code=400, detail="stock must be integer")
    if stock < 0:
        raise HTTPException(status_code=400, detail="stock must be >= 0")

    try:
        price_mxn = int(round(float(payload.get("price_mxn", payload.get("price", 0)))))
    except Exception:
        raise HTTPException(status_code=400, detail="price_mxn must be numeric")
    if price_mxn < 0:
        raise HTTPException(status_code=400, detail="price_mxn must be >= 0")

    active = payload.get("active", True)
    if isinstance(active, bool):
        active_value = active
    else:
        active_text = str(active).strip().lower()
        active_value = active_text in ("1", "true", "yes", "y", "on")

    created_items = []
    created_skus = set()

    db = SessionLocal()
    try:
        existing_skus = {row[0] for row in db.query(InventoryItem.sku).all()}
        existing_barcodes = {row[0] for row in db.query(InventoryItem.barcode).all()}

        for size in sizes:
            item = InventoryItem(
                school_code="",
                product_type="",
                size="",
                gender=None,
                sku="",
                barcode="",
                stock=0,
                price_mxn=0,
                active=True,
                created_at=_now_utc(),
                updated_at=_now_utc(),
            )
            data = {
                "school_code": school_code,
                "product_type": product_type,
                "size": size,
                "gender": gender,
                "stock": stock,
                "price_mxn": price_mxn,
                "active": active_value,
            }
            _apply_inventory_fields(item, data)

            if item.sku in existing_skus or item.sku in created_skus:
                raise HTTPException(status_code=400, detail=f"SKU already exists: {item.sku}")
            if item.barcode in existing_barcodes:
                raise HTTPException(status_code=400, detail=f"Barcode already exists: {item.barcode}")

            created_skus.add(item.sku)
            created_items.append(item)

        for item in created_items:
            db.add(item)

        db.commit()

        for item in created_items:
            db.refresh(item)

        return {
            "ok": True,
            "count": len(created_items),
            "items": [_inventory_to_dict(item) for item in created_items],
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("admin_bulk_create_inventory failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()



@app.get("/admin/inventory/{item_id}")
def admin_get_inventory_item(item_id: int, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    db = SessionLocal()
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == int(item_id)).first()
        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")
        return {"ok": True, "item": _inventory_to_dict(item)}
    finally:
        db.close()


@app.post("/admin/inventory")
def admin_create_inventory_item(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    data = _validate_inventory_payload(payload, partial=False)
    db = SessionLocal()
    try:
        item = InventoryItem(
            school_code="",
            product_type="",
            size="",
            gender=None,
            sku="",
            barcode="",
            stock=0,
            price_mxn=0,
            active=True,
            created_at=_now_utc(),
            updated_at=_now_utc(),
        )
        _apply_inventory_fields(item, data)

        existing_sku = db.query(InventoryItem).filter(InventoryItem.sku == item.sku).first()
        if existing_sku:
            raise HTTPException(status_code=400, detail="SKU already exists")

        existing_barcode = db.query(InventoryItem).filter(InventoryItem.barcode == item.barcode).first()
        if existing_barcode:
            raise HTTPException(status_code=400, detail="Barcode already exists")

        db.add(item)
        db.commit()
        db.refresh(item)
        return {"ok": True, "item": _inventory_to_dict(item)}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("admin_create_inventory_item failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/admin/inventory/{item_id}")
def admin_update_inventory_item(item_id: int, payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    data = _validate_inventory_payload(payload, partial=True)
    db = SessionLocal()
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == int(item_id)).first()
        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        _apply_inventory_fields(item, data)

        existing_sku = (
            db.query(InventoryItem)
            .filter(InventoryItem.sku == item.sku, InventoryItem.id != item.id)
            .first()
        )
        if existing_sku:
            raise HTTPException(status_code=400, detail="SKU already exists")

        existing_barcode = (
            db.query(InventoryItem)
            .filter(InventoryItem.barcode == item.barcode, InventoryItem.id != item.id)
            .first()
        )
        if existing_barcode:
            raise HTTPException(status_code=400, detail="Barcode already exists")

        db.add(item)
        db.commit()
        db.refresh(item)
        return {"ok": True, "item": _inventory_to_dict(item)}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("admin_update_inventory_item failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()




@app.post("/admin/inventory/lookup")
def admin_inventory_lookup(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    barcode = _normalize_code(payload.get("barcode"), upper=True)
    sku = _normalize_code(payload.get("sku"), upper=True)

    if not barcode and not sku:
        raise HTTPException(status_code=400, detail="barcode or sku is required")

    db = SessionLocal()
    try:
        q = db.query(InventoryItem)
        item = q.filter(InventoryItem.barcode == barcode).first() if barcode else q.filter(InventoryItem.sku == sku).first()
        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")
        return {"ok": True, "item": _inventory_to_dict(item)}
    finally:
        db.close()


@app.post("/admin/inventory/adjust-stock")
def admin_inventory_adjust_stock(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    item_id = payload.get("item_id")
    barcode = _normalize_code(payload.get("barcode"), upper=True)
    sku = _normalize_code(payload.get("sku"), upper=True)

    try:
        delta = int(payload.get("delta", 0))
    except Exception:
        raise HTTPException(status_code=400, detail="delta must be integer")

    if delta == 0:
        raise HTTPException(status_code=400, detail="delta must not be 0")

    db = SessionLocal()
    try:
        item = None

        if item_id is not None:
            try:
                item = db.query(InventoryItem).filter(InventoryItem.id == int(item_id)).first()
            except Exception:
                raise HTTPException(status_code=400, detail="item_id invalid")
        elif barcode:
            item = db.query(InventoryItem).filter(InventoryItem.barcode == barcode).first()
        elif sku:
            item = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
        else:
            raise HTTPException(status_code=400, detail="item_id or barcode or sku is required")

        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        new_stock = item.stock + delta
        if new_stock < 0:
            raise HTTPException(status_code=400, detail="Insufficient stock")

        item.stock = new_stock
        item.updated_at = _now_utc()

        db.add(item)
        db.commit()
        db.refresh(item)

        return {
            "ok": True,
            "delta": delta,
            "item": _inventory_to_dict(item),
        }
    finally:
        db.close()


@app.post("/admin/inventory/sell")
def admin_inventory_sell(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    sku = _normalize_code(payload.get("sku"), upper=True)
    barcode = _normalize_code(payload.get("barcode"), upper=True)

    try:
        qty = int(payload.get("qty", 1))
    except Exception:
        raise HTTPException(status_code=400, detail="qty must be integer")

    if qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be >= 1")

    db = SessionLocal()
    try:
        item = None
        if sku:
            item = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
        elif barcode:
            item = db.query(InventoryItem).filter(InventoryItem.barcode == barcode).first()
        else:
            raise HTTPException(status_code=400, detail="sku or barcode is required")

        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        if not bool(item.active):
            raise HTTPException(status_code=400, detail="Inventory item inactive")

        before_stock = int(item.stock or 0)
        if before_stock < qty:
            raise HTTPException(status_code=400, detail="Insufficient stock")

        item.stock = before_stock - qty
        item.updated_at = _now_utc()

        db.add(item)
        db.commit()
        db.refresh(item)

        return {
            "ok": True,
            "message": "Sale recorded",
            "qty": qty,
            "before_stock": before_stock,
            "after_stock": int(item.stock or 0),
            "item": _inventory_to_dict(item),
        }
    finally:
        db.close()







@app.post("/admin/pos/checkout")
def admin_pos_checkout(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    items = payload.get("items") or []
    payment_method = _normalize_code(payload.get("payment_method") or "CASH", upper=True) or "CASH"
    employee_name = str(payload.get("employee_name", "") or "").strip()

    if not isinstance(items, list) or len(items) == 0:
        raise HTTPException(status_code=400, detail="items is required")

    db = SessionLocal()
    try:
        prepared_items = []
        total_mxn = 0
        total_units = 0

        for raw in items:
            if not isinstance(raw, dict):
                raise HTTPException(status_code=400, detail="items contains invalid row")

            sku = _normalize_code(raw.get("sku"), upper=True)
            barcode = _normalize_code(raw.get("barcode"), upper=True)
            try:
                qty = int(raw.get("qty", 1))
            except Exception:
                raise HTTPException(status_code=400, detail="qty must be integer")

            if qty <= 0:
                raise HTTPException(status_code=400, detail="qty must be >= 1")

            item = None
            if sku:
                item = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
            elif barcode:
                item = db.query(InventoryItem).filter(InventoryItem.barcode == barcode).first()
            else:
                raise HTTPException(status_code=400, detail="each item requires sku or barcode")

            if not item:
                raise HTTPException(status_code=404, detail=f"Inventory item not found: {sku or barcode}")

            if not bool(item.active):
                raise HTTPException(status_code=400, detail=f"Inventory item inactive: {item.sku}")

            before_stock = int(item.stock or 0)
            if before_stock < qty:
                raise HTTPException(status_code=400, detail=f"Insufficient stock for {item.sku}")

            unit_price_mxn = int(item.price_mxn or 0)
            line_total_mxn = unit_price_mxn * qty

            prepared_items.append({
                "item": item,
                "qty": qty,
                "before_stock": before_stock,
                "unit_price_mxn": unit_price_mxn,
                "line_total_mxn": line_total_mxn,
            })
            total_mxn += line_total_mxn
            total_units += qty

        sale = PosSale(
            folio=_gen_pos_folio(),
            payment_method=payment_method,
            employee_name=employee_name,
            items_count=total_units,
            total_mxn=total_mxn,
            created_at=_now_utc(),
        )
        db.add(sale)
        db.flush()

        for row in prepared_items:
            item = row["item"]
            qty = row["qty"]
            item.stock = int(item.stock or 0) - qty
            item.updated_at = _now_utc()
            db.add(item)

            db.add(PosSaleItem(
                sale_id=sale.id,
                sku=item.sku,
                barcode=item.barcode,
                name=_inventory_title_for_sale(item),
                qty=qty,
                unit_price_mxn=int(row["unit_price_mxn"]),
                line_total_mxn=int(row["line_total_mxn"]),
                created_at=_now_utc(),
            ))

        db.commit()
        db.refresh(sale)
        return {"ok": True, "sale": _pos_sale_to_dict(sale)}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("admin_pos_checkout failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/admin/pos/sales")
def admin_list_pos_sales(request: Request, limit: int = 50):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")
    limit = max(1, min(int(limit or 50), 200))
    db = SessionLocal()
    try:
        rows = db.query(PosSale).order_by(PosSale.id.desc()).limit(limit).all()
        return {"ok": True, "count": len(rows), "sales": [_pos_sale_to_dict(x) for x in rows]}
    finally:
        db.close()


@app.get("/admin/dashboard/pos-today")
def admin_dashboard_pos_today(request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")
    db = SessionLocal()
    try:
        return {"ok": True, "summary": _pos_sales_total_between(db, _start_of_today_utc(), None)}
    finally:
        db.close()


@app.get("/admin/dashboard/pos-month")
def admin_dashboard_pos_month(request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")
    db = SessionLocal()
    try:
        return {"ok": True, "summary": _pos_sales_total_between(db, _start_of_month_utc(), None)}
    finally:
        db.close()


@app.get("/admin/dashboard/pos-top-products")
def admin_dashboard_pos_top_products(request: Request, limit: int = 20):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")
    limit = max(1, min(int(limit or 20), 100))
    db = SessionLocal()
    try:
        rows = _pos_top_products(db, limit=limit)
        return {"ok": True, "count": len(rows), "rows": rows}
    finally:
        db.close()


@app.get("/admin/dashboard/pos-top-sizes")
def admin_dashboard_pos_top_sizes(request: Request, limit: int = 20):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")
    limit = max(1, min(int(limit or 20), 100))
    db = SessionLocal()
    try:
        rows = _pos_top_sizes(db, limit=limit)
        return {"ok": True, "count": len(rows), "rows": rows}
    finally:
        db.close()
def _dashboard_summary(db):
    total_inventory_items = db.query(InventoryItem).count()
    active_inventory_items = db.query(InventoryItem).filter(InventoryItem.active == True).count()
    low_stock_items = db.query(InventoryItem).filter(InventoryItem.active == True, InventoryItem.stock > 0, InventoryItem.stock <= 5).count()
    out_of_stock_items = db.query(InventoryItem).filter(InventoryItem.active == True, InventoryItem.stock <= 0).count()
    total_stock_units = 0
    total_inventory_value = 0
    for item in db.query(InventoryItem).filter(InventoryItem.active == True).all():
        stock = int(item.stock or 0)
        price = int(item.price_mxn or 0)
        total_stock_units += stock
        total_inventory_value += stock * price

    total_orders = db.query(Order).count()
    paid_orders = db.query(Order).filter(Order.status == "PAID").count()
    delivered_orders = db.query(Order).filter(Order.status == "DELIVERED").count()

    online_sales_mxn = 0
    for order in db.query(Order).filter(Order.status.in_(["PAID", "DELIVERED"])).all():
        online_sales_mxn += int(order.total_mxn or 0)

    pos_sales_count = db.query(PosSale).count()
    pos_sales_mxn = 0
    for sale in db.query(PosSale).all():
        pos_sales_mxn += int(sale.total_mxn or 0)

    today_start = _start_of_today_utc()
    month_start = _start_of_month_utc()
    pos_today_sales_mxn = 0
    pos_month_sales_mxn = 0
    for sale in db.query(PosSale).filter(PosSale.created_at >= month_start).all():
        total = int(sale.total_mxn or 0)
        pos_month_sales_mxn += total
        if sale.created_at and sale.created_at >= today_start:
            pos_today_sales_mxn += total

    return {
        "total_inventory_items": total_inventory_items,
        "active_inventory_items": active_inventory_items,
        "low_stock_items": low_stock_items,
        "out_of_stock_items": out_of_stock_items,
        "total_stock_units": total_stock_units,
        "total_inventory_value_mxn": total_inventory_value,
        "total_orders": total_orders,
        "paid_orders": paid_orders,
        "delivered_orders": delivered_orders,
        "online_sales_mxn": online_sales_mxn,
        "pos_sales_count": pos_sales_count,
        "pos_sales_mxn": pos_sales_mxn,
        "pos_today_sales_mxn": pos_today_sales_mxn,
        "pos_month_sales_mxn": pos_month_sales_mxn,
        "grand_total_sales_mxn": online_sales_mxn + pos_sales_mxn,
    }


def _top_sizes(db, limit: int = 20):
    sales = {}
    for order in db.query(Order).filter(Order.status.in_(["PAID", "DELIVERED"])).all():
        for item in (order.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            qty = int(getattr(item, "qty", 0) or 0)
            if not sku or qty <= 0:
                continue
            parts = sku.split("-")
            size = parts[-1] if parts else ""
            if not size:
                continue
            row = sales.setdefault(size, {"size": size, "qty_sold": 0, "skus": set()})
            row["qty_sold"] += qty
            row["skus"].add(sku)

    rows = sorted(sales.values(), key=lambda x: (-x["qty_sold"], x["size"]))[:limit]
    out = []
    for row in rows:
        out.append({"size": row["size"], "qty_sold": row["qty_sold"], "sku_count": len(row["skus"])})
    return out


def _sales_by_sku(db, limit: int = 20):
    sales = {}
    for order in db.query(Order).filter(Order.status.in_(["PAID", "DELIVERED"])).all():
        for item in (order.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            qty = int(getattr(item, "qty", 0) or 0)
            if not sku or qty <= 0:
                continue
            row = sales.setdefault(sku, {"sku": sku, "name": getattr(item, "name", ""), "qty_sold": 0})
            row["qty_sold"] += qty

    return sorted(sales.values(), key=lambda x: (-x["qty_sold"], x["sku"]))[:limit]





def _pos_top_products(db, limit: int = 20):
    sales = {}
    for sale in db.query(PosSale).all():
        for item in (sale.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            qty = int(getattr(item, "qty", 0) or 0)
            name = str(getattr(item, "name", "") or "").strip()
            if not sku or qty <= 0:
                continue
            row = sales.setdefault(sku, {"sku": sku, "name": name, "qty_sold": 0, "sales_mxn": 0})
            row["qty_sold"] += qty
            row["sales_mxn"] += int(getattr(item, "line_total_mxn", 0) or 0)
    return sorted(sales.values(), key=lambda x: (-x["qty_sold"], -x["sales_mxn"], x["sku"]))[:limit]


def _pos_top_sizes(db, limit: int = 20):
    sales = {}
    for sale in db.query(PosSale).all():
        for item in (sale.items or []):
            sku = str(getattr(item, "sku", "") or "").strip().upper()
            qty = int(getattr(item, "qty", 0) or 0)
            if not sku or qty <= 0:
                continue
            parts = sku.split("-")
            size = parts[-1] if parts else ""
            if not size:
                continue
            row = sales.setdefault(size, {"size": size, "qty_sold": 0, "sku_count": set()})
            row["qty_sold"] += qty
            row["sku_count"].add(sku)
    rows = sorted(sales.values(), key=lambda x: (-x["qty_sold"], x["size"]))[:limit]
    return [{"size": r["size"], "qty_sold": r["qty_sold"], "sku_count": len(r["sku_count"])} for r in rows]


def _pos_sales_total_between(db, start_dt, end_dt=None):
    q = db.query(PosSale)
    if start_dt is not None:
        q = q.filter(PosSale.created_at >= start_dt)
    if end_dt is not None:
        q = q.filter(PosSale.created_at < end_dt)
    rows = q.all()
    total_mxn = sum(int(r.total_mxn or 0) for r in rows)
    total_tickets = len(rows)
    total_items = sum(int(r.items_count or 0) for r in rows)
    return {"total_mxn": total_mxn, "tickets": total_tickets, "items_count": total_items}
@app.get("/admin/dashboard/summary")
def admin_dashboard_summary(request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    db = SessionLocal()
    try:
        return {"ok": True, "summary": _dashboard_summary(db)}
    finally:
        db.close()


@app.get("/admin/dashboard/top-sizes")
def admin_dashboard_top_sizes(request: Request, limit: int = 20):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    limit = max(1, min(int(limit or 20), 100))
    db = SessionLocal()
    try:
        rows = _top_sizes(db, limit=limit)
        return {"ok": True, "count": len(rows), "rows": rows}
    finally:
        db.close()


@app.get("/admin/dashboard/top-skus")
def admin_dashboard_top_skus(request: Request, limit: int = 20):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    limit = max(1, min(int(limit or 20), 100))
    db = SessionLocal()
    try:
        rows = _sales_by_sku(db, limit=limit)
        return {"ok": True, "count": len(rows), "rows": rows}
    finally:
        db.close()


@app.post("/admin/inventory/set-stock")
def admin_inventory_set_stock(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    sku = _normalize_code(payload.get("sku"), upper=True)
    barcode = _normalize_code(payload.get("barcode"), upper=True)

    try:
        stock = int(payload.get("stock", 0))
    except Exception:
        raise HTTPException(status_code=400, detail="stock must be integer")

    if stock < 0:
        raise HTTPException(status_code=400, detail="stock must be >= 0")

    db = SessionLocal()
    try:
        item = None
        if sku:
            item = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
        elif barcode:
            item = db.query(InventoryItem).filter(InventoryItem.barcode == barcode).first()
        else:
            raise HTTPException(status_code=400, detail="sku or barcode is required")

        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        before_stock = int(item.stock or 0)
        item.stock = stock
        item.updated_at = _now_utc()
        db.add(item)
        db.commit()
        db.refresh(item)
        return {"ok": True, "before_stock": before_stock, "after_stock": int(item.stock or 0), "item": _inventory_to_dict(item)}
    finally:
        db.close()


@app.post("/admin/inventory/set-price")
def admin_inventory_set_price(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    sku = _normalize_code(payload.get("sku"), upper=True)
    barcode = _normalize_code(payload.get("barcode"), upper=True)

    try:
        price_mxn = int(round(float(payload.get("price_mxn", payload.get("price", 0)))))
    except Exception:
        raise HTTPException(status_code=400, detail="price_mxn must be numeric")

    if price_mxn < 0:
        raise HTTPException(status_code=400, detail="price_mxn must be >= 0")

    db = SessionLocal()
    try:
        item = None
        if sku:
            item = db.query(InventoryItem).filter(InventoryItem.sku == sku).first()
        elif barcode:
            item = db.query(InventoryItem).filter(InventoryItem.barcode == barcode).first()
        else:
            raise HTTPException(status_code=400, detail="sku or barcode is required")

        if not item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        before_price = int(item.price_mxn or 0)
        item.price_mxn = price_mxn
        item.updated_at = _now_utc()
        db.add(item)
        db.commit()
        db.refresh(item)
        return {"ok": True, "before_price_mxn": before_price, "after_price_mxn": int(item.price_mxn or 0), "item": _inventory_to_dict(item)}
    finally:
        db.close()

# -------------------------
# Admin endpoints (TEMP)
# -------------------------
@app.get("/admin/orders")
def admin_list_orders(request: Request, limit: int = 20):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    limit = max(1, min(int(limit or 20), 200))

    db = SessionLocal()
    try:
        orders = db.query(Order).order_by(Order.id.desc()).limit(limit).all()
        out = []
        for o in orders:
            out.append(_order_to_dict(o))
        return {"ok": True, "count": len(out), "orders": out}
    finally:
        db.close()


@app.get("/admin/orders/{order_id}")
def admin_get_order(order_id: int, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    db = SessionLocal()
    try:
        o = db.query(Order).filter(Order.id == int(order_id)).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")
        return {"ok": True, "order": _order_to_dict(o)}
    finally:
        db.close()


# -------------------------
# Partner / socios
# -------------------------
@app.get("/admin/partners")
def admin_list_partners(request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    db = SessionLocal()
    try:
        partners = db.query(PartnerAccount).order_by(PartnerAccount.school_name.asc(), PartnerAccount.id.asc()).all()
        rows = []
        for partner in partners:
            dashboard = _partner_build_dashboard(db, partner)
            rows.append({
                **_partner_account_to_dict(partner),
                "pending_items": dashboard["summary"]["pending_items"],
                "pending_commission_mxn": dashboard["summary"]["pending_commission_mxn"],
                "items_today": dashboard["summary"]["items_today"],
                "items_month": dashboard["summary"]["items_month"],
                "last_paid_to_at": dashboard["summary"]["last_paid_to_at"],
            })
        return {"ok": True, "count": len(rows), "partners": rows}
    finally:
        db.close()


@app.post("/admin/partners")
def admin_create_partner(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    school_code = _normalize_code(payload.get("school_code"), upper=True)
    school_name = str(payload.get("school_name", "")).strip()
    partner_name = str(payload.get("partner_name", "")).strip()
    access_code = _normalize_code(payload.get("access_code"), upper=True)
    try:
        commission_per_item = int(round(float(payload.get("commission_per_item", 40))))
    except Exception:
        raise HTTPException(status_code=400, detail="commission_per_item inválido")

    if not school_code:
        raise HTTPException(status_code=400, detail="school_code es requerido")
    if not school_name:
        raise HTTPException(status_code=400, detail="school_name es requerido")
    if not partner_name:
        raise HTTPException(status_code=400, detail="partner_name es requerido")
    if not access_code:
        raise HTTPException(status_code=400, detail="access_code es requerido")
    if commission_per_item < 0:
        raise HTTPException(status_code=400, detail="commission_per_item inválido")

    db = SessionLocal()
    try:
        existing_school = db.query(PartnerAccount).filter(PartnerAccount.school_code == school_code).first()
        if existing_school:
            raise HTTPException(status_code=400, detail="Ya existe un socio para esa escuela")
        existing_code = db.query(PartnerAccount).filter(PartnerAccount.access_code == access_code).first()
        if existing_code:
            raise HTTPException(status_code=400, detail="Ese código ya existe")

        now = _now_utc()
        partner = PartnerAccount(
            school_code=school_code,
            school_name=school_name,
            partner_name=partner_name,
            access_code=access_code,
            commission_per_item=commission_per_item,
            active=True,
            created_at=now,
            updated_at=now,
        )
        db.add(partner)
        db.commit()
        db.refresh(partner)
        return {"ok": True, "partner": _partner_account_to_dict(partner)}
    finally:
        db.close()


@app.get("/admin/partner/dashboard")
def admin_partner_dashboard(request: Request, school_code: str = ""):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    school_code = _normalize_code(school_code, upper=True)
    if not school_code:
        raise HTTPException(status_code=400, detail="school_code es requerido")

    db = SessionLocal()
    try:
        partner = db.query(PartnerAccount).filter(PartnerAccount.school_code == school_code).first()
        if not partner:
            raise HTTPException(status_code=404, detail="Socio no encontrado")
        return {"ok": True, **_partner_build_dashboard(db, partner)}
    finally:
        db.close()


@app.post("/admin/partner/pay")
def admin_partner_pay(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    school_code = _normalize_code(payload.get("school_code"), upper=True)
    if not school_code:
        raise HTTPException(status_code=400, detail="school_code es requerido")

    db = SessionLocal()
    try:
        partner = db.query(PartnerAccount).filter(PartnerAccount.school_code == school_code).first()
        if not partner:
            raise HTTPException(status_code=404, detail="Socio no encontrado")

        dashboard = _partner_build_dashboard(db, partner)
        pending_items = int(dashboard["summary"]["pending_items"] or 0)
        pending_total = int(dashboard["summary"]["pending_commission_mxn"] or 0)
        if pending_items <= 0 or pending_total <= 0:
            raise HTTPException(status_code=400, detail="No hay comisión pendiente")

        paid_from = None
        if dashboard["summary"]["last_paid_to_at"]:
            paid_from = datetime.fromisoformat(str(dashboard["summary"]["last_paid_to_at"]).replace("Z", "+00:00"))
        paid_to = None
        if dashboard["summary"]["latest_pending_at"]:
            paid_to = datetime.fromisoformat(str(dashboard["summary"]["latest_pending_at"]).replace("Z", "+00:00"))

        row = PartnerCommissionHistory(
            partner_id=partner.id,
            school_code=partner.school_code,
            items_sold=pending_items,
            commission_total_mxn=pending_total,
            paid_from_at=paid_from,
            paid_to_at=paid_to,
            created_at=_now_utc(),
        )
        db.add(row)
        partner.updated_at = _now_utc()
        db.add(partner)
        db.commit()
        db.refresh(row)
        return {"ok": True, "history": _partner_history_to_dict(row)}
    finally:
        db.close()


@app.post("/partner/login")
def partner_login(payload: dict):
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    code = _normalize_code(payload.get("code"), upper=True)
    if not code:
        raise HTTPException(status_code=400, detail="Código requerido")

    db = SessionLocal()
    try:
        partner = _partner_find_by_code(db, code)
        if not partner:
            raise HTTPException(status_code=401, detail="Código inválido")
        return {"ok": True, "partner": _partner_account_to_dict(partner)}
    finally:
        db.close()


@app.get("/partner/dashboard")
def partner_dashboard(code: str = ""):
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    code = _normalize_code(code, upper=True)
    if not code:
        raise HTTPException(status_code=400, detail="Código requerido")

    db = SessionLocal()
    try:
        partner = _partner_find_by_code(db, code)
        if not partner:
            raise HTTPException(status_code=401, detail="Código inválido")
        return {"ok": True, **_partner_build_dashboard(db, partner)}
    finally:
        db.close()


# -------------------------
# Pickup (VERIFY / CONFIRM)
# -------------------------
@app.post("/pickup/verify")
def pickup_verify(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    code = str(payload.get("code", "")).strip()
    token = str(payload.get("token", "")).strip()
    if not code and not token:
        raise HTTPException(status_code=400, detail="code or token is required")

    db = SessionLocal()
    try:
        q = db.query(Order)
        o = q.filter(Order.pickup_token == token).first() if token else q.filter(Order.pickup_code == code).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")

        changed = _maybe_mark_expired(db, o)
        if changed:
            db.commit()
            db.refresh(o)

        if o.status != "PAID" or o.pickup_status != "ACTIVE":
            return {"ok": False, "message": "Not eligible for pickup", "order": _order_to_dict(o)}

        return {"ok": True, "message": "Eligible for pickup", "order": _order_to_dict(o)}
    finally:
        db.close()


@app.post("/pickup/confirm")
def pickup_confirm(payload: dict, request: Request):
    _require_admin(request)
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    code = str(payload.get("code", "")).strip()
    token = str(payload.get("token", "")).strip()
    if not code and not token:
        raise HTTPException(status_code=400, detail="code or token is required")

    db = SessionLocal()
    try:
        q = db.query(Order)
        o = q.filter(Order.pickup_token == token).first() if token else q.filter(Order.pickup_code == code).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")

        changed = _maybe_mark_expired(db, o)
        if changed:
            db.commit()
            db.refresh(o)

        if o.status != "PAID" or o.pickup_status != "ACTIVE":
            return {"ok": False, "message": "Cannot deliver", "order": _order_to_dict(o)}

        o.status = "DELIVERED"
        o.pickup_status = "DELIVERED"
        o.delivered_at = _now_utc()
        db.add(o)
        db.commit()
        db.refresh(o)

        return {"ok": True, "message": "Delivered", "order": _order_to_dict(o)}
    finally:
        db.close()


@app.get("/pickup/qr_payload/{pickup_token}")
def pickup_qr_payload(pickup_token: str):
    pickup_token = (pickup_token or "").strip()
    if not pickup_token:
        raise HTTPException(status_code=400, detail="pickup_token is required")

    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    db = SessionLocal()
    try:
        o = db.query(Order).filter(Order.pickup_token == pickup_token).first()
        if not o:
            raise HTTPException(status_code=404, detail="Order not found")

        changed = _maybe_mark_expired(db, o)
        if changed:
            db.commit()
            db.refresh(o)

        return {"ok": True, "qr_payload": _pickup_qr_payload(pickup_token), "order": _order_public_to_dict(o)}
    finally:
        db.close()


# -------------------------
# Customer orders refresh
# -------------------------
@app.post("/customer/orders/refresh")
def customer_orders_refresh(payload: dict):
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = payload or {}
    tokens = payload.get("tokens") or []
    if not isinstance(tokens, list) or len(tokens) == 0:
        raise HTTPException(status_code=400, detail="tokens is required")

    cleaned_tokens = []
    seen = set()
    for t in tokens:
        tok = str(t or "").strip()
        if not tok:
            continue
        if tok in seen:
            continue
        seen.add(tok)
        cleaned_tokens.append(tok)

    if not cleaned_tokens:
        raise HTTPException(status_code=400, detail="tokens is required")

    db = SessionLocal()
    try:
        orders = (
            db.query(Order)
            .filter(Order.pickup_token.in_(cleaned_tokens))
            .all()
        )

        changed_any = False
        by_token = {}
        for o in orders:
            changed = _maybe_mark_expired(db, o)
            if changed:
                changed_any = True
            by_token[o.pickup_token] = o

        if changed_any:
            db.commit()
            for o in orders:
                db.refresh(o)

        out = []
        for tok in cleaned_tokens:
            o = by_token.get(tok)
            if not o:
                out.append({"pickup_token": tok, "found": False})
            else:
                out.append({"pickup_token": tok, "found": True, "order": _order_public_to_dict(o)})

        return {"ok": True, "count": len(out), "orders": out}
    finally:
        db.close()


@app.post("/pickup/redeem")
def pickup_redeem(payload: dict, request: Request):
    # Store-facing: requires ADMIN_TOKEN
    payload = payload or {}
    token = str(payload.get("token", "")).strip()
    if not token:
        raise HTTPException(status_code=400, detail="token is required")
    return pickup_confirm({"token": token}, request)


# -------------------------
# Nota de venta (email) — MVP
# -------------------------
def _mark_note_status(db, order: Order, status: str, err: str = ""):
    order.note_sent_at = _now_utc()
    order.note_status = status
    order.note_error = (err or "")[:500]
    db.add(order)


def _build_sales_note_text(order: Order) -> str:
    lines = [
        "Gracias por tu compra en Quing Textil.",
        f"Pedido: #{order.id}",
        f"Nombre: {order.customer_name}",
        f"Correo: {order.customer_email}",
        f"Total: ${order.total_mxn} MXN",
        "",
        "Detalle:",
    ]

    for it in (order.items or []):
        qty = int(getattr(it, "qty", 0) or 0)
        unit_amount_mxn = int(getattr(it, "unit_amount_mxn", 0) or 0)
        line_total = qty * unit_amount_mxn
        lines.append(
            f"- {it.name} | SKU: {getattr(it, 'sku', '') or '-'} | Cantidad: {qty} | Unitario: ${unit_amount_mxn} MXN | Subtotal: ${line_total} MXN"
        )

    lines += [
        "",
        f"Código de entrega: {order.pickup_code}",
        f"Fecha límite para recoger: {_dt(order.expires_at)}" if order.expires_at else "",
        "",
        "Muestra tu QR y código de entrega en tienda.",
    ]
    return "\n".join([x for x in lines if x != ""])


def _money_mxn(value: int) -> str:
    try:
        return f"${int(value or 0):,} MXN"
    except Exception:
        return f"${value} MXN"


def _fmt_local_dt(value) -> str:
    if not value:
        return "-"
    try:
        dt = value
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(timezone(timedelta(hours=-6)))
        return local_dt.strftime("%d/%m/%Y %I:%M %p")
    except Exception:
        return str(value)


def _pdf_logo_candidates() -> list[Path]:
    here = Path(__file__).resolve().parent
    return [
        here / "assets" / "quing_logo.png",
        here / "assets" / "logo quing textil sin fondo.png",
        here / "assets" / "LOGO QUING TEXTIL.png",
        here.parent / "assets" / "quing_logo.png",
        here.parent / "assets" / "logo quing textil sin fondo.png",
        here.parent / "assets" / "LOGO QUING TEXTIL.png",
        Path.cwd() / "app" / "assets" / "quing_logo.png",
        Path.cwd() / "app" / "assets" / "logo quing textil sin fondo.png",
        Path.cwd() / "app" / "assets" / "LOGO QUING TEXTIL.png",
        Path.cwd() / "backend" / "app" / "assets" / "quing_logo.png",
        Path.cwd() / "backend" / "app" / "assets" / "logo quing textil sin fondo.png",
        Path.cwd() / "backend" / "app" / "assets" / "LOGO QUING TEXTIL.png",
        Path.cwd() / "assets" / "quing_logo.png",
        Path.cwd() / "assets" / "logo quing textil sin fondo.png",
        Path.cwd() / "assets" / "LOGO QUING TEXTIL.png",
    ]


def _find_pdf_logo_path() -> Path | None:
    for path in _pdf_logo_candidates():
        try:
            if path.exists() and path.is_file():
                logger.info("PDF logo found at %s", path)
                return path
        except Exception:
            continue
    logger.warning("PDF logo not found in expected paths")
    return None


def _wrap_pdf_text(pdf: canvas.Canvas, text_value: str, max_width: float, font_name: str = "Helvetica", font_size: int = 9) -> list[str]:
    text_value = str(text_value or "-").strip() or "-"
    words = text_value.split()
    if not words:
        return ["-"]
    lines = []
    current = words[0]
    for word in words[1:]:
        trial = f"{current} {word}"
        if pdf.stringWidth(trial, font_name, font_size) <= max_width:
            current = trial
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _make_pickup_qr_image(order: Order):
    try:
        payload = _pickup_qr_payload(order.pickup_token)
        qr = qrcode.QRCode(version=None, box_size=8, border=2)
        qr.add_data(payload)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        bio = BytesIO()
        img.save(bio, format="PNG")
        bio.seek(0)
        return ImageReader(bio)
    except Exception:
        logger.exception("PDF QR generation failed")
        return None


def _draw_pdf_header(pdf: canvas.Canvas, width: float, height: float, order: Order):
    dark = colors.HexColor("#111827")
    muted = colors.HexColor("#6B7280")
    soft_gray = colors.HexColor("#E5E7EB")
    logo_path = _find_pdf_logo_path()

    pdf.setFillColor(colors.white)
    pdf.rect(0, height - 38 * mm, width, 38 * mm, stroke=0, fill=1)
    pdf.setStrokeColor(soft_gray)
    pdf.line(18 * mm, height - 34 * mm, width - 18 * mm, height - 34 * mm)

    logo_drawn = False
    if logo_path:
        try:
            logo = ImageReader(str(logo_path))
            pdf.drawImage(
                logo,
                18 * mm,
                height - 30 * mm,
                width=82 * mm,
                height=28 * mm,
                preserveAspectRatio=True,
                mask='auto',
            )
            logo_drawn = True
        except Exception:
            logger.exception("PDF logo draw failed")

    if not logo_drawn:
        pdf.setFillColor(dark)
        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawString(18 * mm, height - 20 * mm, "QUING")

    pdf.setFillColor(dark)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawRightString(width - 18 * mm, height - 16 * mm, f"Pedido #{order.id}")
    pdf.setFillColor(muted)
    pdf.setFont("Helvetica", 10)
    pdf.drawRightString(width - 18 * mm, height - 22 * mm, _fmt_local_dt(order.paid_at or order.created_at))


def _draw_items_header(pdf: canvas.Canvas, left: float, right: float, y: float):
    blue = colors.HexColor("#1D4ED8")
    pdf.setFillColor(blue)
    pdf.setStrokeColor(blue)
    pdf.roundRect(left, y - 4 * mm, right - left, 8 * mm, 1.5 * mm, stroke=0, fill=1)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(left + 2 * mm, y - 1 * mm, "Producto")
    pdf.drawString(116 * mm, y - 1 * mm, "Cant.")
    pdf.drawString(132 * mm, y - 1 * mm, "Unit.")
    pdf.drawRightString(right - 2 * mm, y - 1 * mm, "Subtotal")


def _generate_sales_note_pdf_bytes(order: Order) -> bytes:
    blue = colors.HexColor("#1D4ED8")
    light_blue = colors.HexColor("#EFF6FF")
    soft_gray = colors.HexColor("#E5E7EB")
    dark = colors.HexColor("#111827")
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    _draw_pdf_header(pdf, width, height, order)

    left = 18 * mm
    right = width - 18 * mm
    y = height - 52 * mm

    info_top = y
    info_height = 34 * mm
    qr_size = 28 * mm
    qr_x = right - qr_size

    pdf.setFillColor(light_blue)
    pdf.setStrokeColor(soft_gray)
    pdf.roundRect(left, info_top - info_height, (qr_x - 6 * mm) - left, info_height, 2 * mm, stroke=1, fill=1)

    pdf.setFillColor(blue)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(left + 3 * mm, info_top - 6 * mm, "Datos del cliente")

    pdf.setFillColor(dark)
    pdf.setFont("Helvetica", 10)
    details = [
        f"Nombre: {order.customer_name or '-'}",
        f"Correo: {order.customer_email or '-'}",
        f"Código de entrega: {order.pickup_code or '-'}",
        f"Vence: {_fmt_local_dt(order.expires_at) if order.expires_at else '-'}",
    ]
    detail_y = info_top - 13 * mm
    for line in details:
        pdf.drawString(left + 3 * mm, detail_y, line)
        detail_y -= 5.2 * mm

    qr_img = _make_pickup_qr_image(order)
    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(soft_gray)
    pdf.roundRect(qr_x - 4 * mm, info_top - info_height, qr_size + 4 * mm, info_height, 2 * mm, stroke=1, fill=1)
    if qr_img:
        pdf.drawImage(qr_img, qr_x - 2 * mm, info_top - 30 * mm, width=qr_size, height=qr_size, preserveAspectRatio=True, mask='auto')
    pdf.setFillColor(blue)
    pdf.setFont("Helvetica-Bold", 8)
    pdf.drawCentredString(qr_x + 12 * mm, info_top - 32 * mm, "QR de entrega")

    y = info_top - info_height - 10 * mm
    pdf.setFillColor(dark)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(left, y, "Detalle de compra")
    y -= 8 * mm

    col_name = left + 2 * mm
    col_qty = 116 * mm
    col_unit = 132 * mm
    col_total = right - 2 * mm
    product_width = col_qty - col_name - 6 * mm
    _draw_items_header(pdf, left, right, y)
    y -= 10 * mm

    items = list(order.items or [])
    for idx, it in enumerate(items, start=1):
        qty = int(getattr(it, "qty", 0) or 0)
        unit_amount_mxn = int(getattr(it, "unit_amount_mxn", 0) or 0)
        line_total = qty * unit_amount_mxn
        product_name = str(getattr(it, "name", "") or "-")
        wrapped = _wrap_pdf_text(pdf, product_name, product_width, "Helvetica", 9)
        max_lines = 3
        if len(wrapped) > max_lines:
            wrapped = wrapped[:max_lines]
            wrapped[-1] = wrapped[-1][:max(0, len(wrapped[-1]) - 3)] + "..."
        row_height = max(9 * mm, (len(wrapped) * 4.8 + 3) * mm)

        if y - row_height < 28 * mm:
            pdf.showPage()
            width, height = A4
            _draw_pdf_header(pdf, width, height, order)
            y = height - 30 * mm
            _draw_items_header(pdf, left, right, y)
            y -= 10 * mm

        if idx % 2 == 1:
            pdf.setFillColor(colors.white)
        else:
            pdf.setFillColor(colors.HexColor("#F8FAFC"))
        pdf.setStrokeColor(soft_gray)
        pdf.roundRect(left, y - row_height + 2 * mm, right - left, row_height, 1.5 * mm, stroke=1, fill=1)

        text_y = y - 4 * mm
        pdf.setFillColor(dark)
        pdf.setFont("Helvetica", 9)
        for line in wrapped:
            pdf.drawString(col_name, text_y, line)
            text_y -= 4.8 * mm
        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(col_qty, y - 4 * mm, str(qty))
        pdf.drawString(col_unit, y - 4 * mm, _money_mxn(unit_amount_mxn).replace(" MXN", ""))
        pdf.drawRightString(col_total, y - 4 * mm, _money_mxn(line_total).replace(" MXN", ""))
        y -= row_height + 2 * mm

    y -= 1.5 * mm
    pdf.setStrokeColor(colors.HexColor("#D1D5DB"))
    pdf.setLineWidth(0.8)
    pdf.line(left, y, right, y)

    y -= 7 * mm
    pdf.setFillColor(colors.HexColor("#374151"))
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(right - 50 * mm, y, "Total")

    pdf.setFillColor(dark)
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawRightString(right, y, _money_mxn(order.total_mxn))
    y -= 12 * mm

    pdf.setFillColor(dark)
    pdf.setFont("Helvetica", 10)
    pdf.drawString(left, y, "Gracias por tu compra en Quing Textil.")
    y -= 5.5 * mm
    pdf.drawString(left, y, "Presenta tu QR y código de entrega en tienda para recoger tu pedido.")
    y -= 8 * mm
    pdf.setFillColor(colors.HexColor("#6B7280"))
    pdf.setFont("Helvetica", 8)
    pdf.drawString(left, y, f"Pedido #{order.id} · Generado {_fmt_local_dt(order.paid_at or order.created_at)}")

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def _send_note_email_if_configured(order: Order):
    """Returns (ok: bool, err: str). Sends only if SMTP is configured."""
    host = os.getenv("SMTP_HOST", "").strip()
    port_raw = str(os.getenv("SMTP_PORT", "0") or "0").strip()
    user = os.getenv("SMTP_USER", "").strip()
    pwd = os.getenv("SMTP_PASS", "").strip()
    from_email = (os.getenv("SMTP_FROM", "").strip() or user)
    from_name = (os.getenv("SMTP_FROM_NAME", "").strip() or "QUING TEXTIL")

    try:
        port = int(port_raw or "0")
    except Exception:
        return False, "SMTP_PORT invalid"

    if not host or not port or not from_email:
        return False, "SMTP not configured"

    try:
        import smtplib
        from email.message import EmailMessage
        from email.utils import formataddr

        msg = EmailMessage()
        msg["Subject"] = f"Tu compra en Quing Textil — Pedido #{order.id}"
        msg["From"] = formataddr((from_name, from_email))
        msg["To"] = order.customer_email

        if not order.customer_email:
            return False, "Order has no customer_email"

        msg.set_content(
            "Gracias por tu compra en Quing Textil.\n\n"
            f"Adjuntamos tu ticket en PDF del pedido #{order.id}.\n"
            "Muestra tu QR y código de entrega en tienda para recoger tu pedido."
        )

        pdf_bytes = _generate_sales_note_pdf_bytes(order)
        filename = f"quing-textil-pedido-{order.id}.pdf"
        msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=filename)

        with smtplib.SMTP(host, port, timeout=20) as server:
            server.ehlo()
            try:
                server.starttls()
                server.ehlo()
            except Exception:
                pass

            if user and pwd:
                server.login(user, pwd)

            server.send_message(msg)

        return True, ""
    except Exception as e:
        return False, str(e)


def _send_note_email_for_order_if_needed(db, order: Order) -> bool:
    if not order:
        return False
    if not str(getattr(order, "customer_email", "") or "").strip():
        return False
    if getattr(order, "status", "") != "PAID":
        return False
    if getattr(order, "note_status", "") not in ("PENDING", "FAILED"):
        return False

    ok, err = _send_note_email_if_configured(order)
    if ok:
        _mark_note_status(db, order, "SENT", "")
    else:
        _mark_note_status(db, order, "FAILED", err)
    return ok


# -------------------------
# Checkout
# -------------------------
# Checkout
# -------------------------
@app.post("/checkout")
async def create_checkout(payload: dict):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe secret key not configured")

    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    checkout_data = _validate_checkout_payload(payload)

    db = SessionLocal()
    try:
        order, pickup_code, pickup_token = _create_pending_order(db, checkout_data)

        success_url = f"{os.getenv('BASE_URL', '').rstrip('/')}/checkout/success?session_id={{CHECKOUT_SESSION_ID}}"
        cancel_url = f"{os.getenv('BASE_URL', '').rstrip('/')}/checkout/cancel"

        if not os.getenv("BASE_URL", "").strip():
            raise HTTPException(status_code=500, detail="BASE_URL not configured")

        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=checkout_data["line_items"],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "order_id": str(order.id),
                "pickup_token": pickup_token,
                "pickup_code": pickup_code,
            },
        )

        db.add(Payment(
            order_id=order.id,
            stripe_session_id=session.id,
            created_at=_now_utc(),
        ))

        db.commit()
        db.refresh(order)

        return {
            "ok": True,
            "order_id": order.id,
            "pickup_code": pickup_code,
            "pickup_token": pickup_token,
            "checkout_url": session.url,
            "session_id": session.id,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("create_checkout failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/checkout/mobile")
async def create_mobile_checkout(payload: dict):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe secret key not configured")

    if not STRIPE_PUBLISHABLE_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_PUBLISHABLE_KEY not configured")

    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    checkout_data = _validate_checkout_payload(payload)

    db = SessionLocal()
    try:
        order, pickup_code, pickup_token = _create_pending_order(db, checkout_data)

        customer = stripe.Customer.create(
            name=checkout_data["customer_name"],
            phone=checkout_data["customer_phone"] or None,
            email=checkout_data["customer_email"] or None,
            metadata={
                "order_id": str(order.id),
                "pickup_token": pickup_token,
                "pickup_code": pickup_code,
            },
        )

        ephemeral_key = stripe.EphemeralKey.create(
            customer=customer.id,
            stripe_version=STRIPE_EPHEMERAL_KEY_API_VERSION,
        )

        payment_intent = stripe.PaymentIntent.create(
            amount=checkout_data["total_mxn"] * 100,
            currency="mxn",
            customer=customer.id,
            automatic_payment_methods={"enabled": True},
            metadata={
                "order_id": str(order.id),
                "pickup_token": pickup_token,
                "pickup_code": pickup_code,
            },
        )

        db.add(Payment(
            order_id=order.id,
            stripe_session_id=payment_intent.id,
            created_at=_now_utc(),
        ))

        db.commit()
        db.refresh(order)

        return {
            "ok": True,
            "mode": "mobile_payment_sheet",
            "order_id": order.id,
            "pickup_code": pickup_code,
            "pickup_token": pickup_token,
            "customer_id": customer.id,
            "ephemeral_key": ephemeral_key.secret,
            "payment_intent": payment_intent.client_secret,
            "payment_intent_id": payment_intent.id,
            "publishable_key": STRIPE_PUBLISHABLE_KEY,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("create_mobile_checkout failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# -------------------------
# Success / Cancel
# -------------------------
@app.get("/checkout/success", response_class=HTMLResponse)
def checkout_success(session_id: str = ""):
    if not session_id:
        return HTMLResponse("<h3>Missing session_id</h3>", status_code=400)

    if not SessionLocal:
        return HTMLResponse("<h3>Database not configured</h3>", status_code=500)

    db = SessionLocal()
    try:
        payment = db.query(Payment).filter(Payment.stripe_session_id == session_id).first()
        if not payment:
            return HTMLResponse("<h3>Payment/session not found</h3>", status_code=404)

        order = db.query(Order).filter(Order.id == payment.order_id).first()
        if not order:
            return HTMLResponse("<h3>Order not found</h3>", status_code=404)

        # Sync payment snapshot from Stripe
        try:
            _record_payment_snapshot(db, order, session_id)
            db.commit()
            db.refresh(order)
        except Exception as e:
            logger.exception("Success sync failed")
            return HTMLResponse(f"<h3>Payment sync failed</h3><pre>{e}</pre>", status_code=500)

        # Send sales note once (if email configured and order is paid)
        if _send_note_email_for_order_if_needed(db, order):
            db.commit()
            db.refresh(order)

        # Deep link into the app
        deep_link = _pickup_qr_payload(order.pickup_token)

        html = f"""
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
          <title>Abriendo QUING...</title>
          <style>
            html, body {{
              margin: 0;
              padding: 0;
              width: 100%;
              height: 100%;
              background: #ffffff;
              overflow: hidden;
            }}
          </style>
          <script>
            (function() {{
              var deepLink = {deep_link!r};

              function openApp() {{
                window.location.replace(deepLink);
              }}

              window.addEventListener("load", function() {{
                openApp();
              }});

              setTimeout(function() {{
                openApp();
              }}, 50);

              setTimeout(function() {{
                openApp();
              }}, 300);
            }})();
          </script>
        </head>
        <body></body>
        </html>
        """
        return HTMLResponse(html)
    finally:
        db.close()


@app.get("/checkout/cancel", response_class=HTMLResponse)
def checkout_cancel():
    return HTMLResponse("""
    <html><body style="font-family:Arial;padding:24px">
      <h2>Pago cancelado</h2>
      <p>Puedes regresar a la app y volver a intentar.</p>
    </body></html>
    """)


# -------------------------
# Stripe Webhook
# -------------------------
@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET not configured")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")

    event_type = event.get("type", "")

    if event_type == "checkout.session.completed":
        obj = event["data"]["object"]
        session_id = obj.get("id", "")

        if SessionLocal and session_id:
            db = SessionLocal()
            try:
                order, payment = _mark_order_paid_from_session(db, session_id)
                if order:
                    _send_note_email_for_order_if_needed(db, order)
                    db.commit()
                else:
                    db.rollback()
            except Exception:
                db.rollback()
                logger.exception("webhook processing failed")
            finally:
                db.close()

    if event_type == "payment_intent.succeeded":
        obj = event["data"]["object"]
        payment_intent_id = obj.get("id", "")

        if SessionLocal and payment_intent_id:
            db = SessionLocal()
            try:
                order, payment = _mark_order_paid_from_payment_intent(db, payment_intent_id)
                if order:
                    _send_note_email_for_order_if_needed(db, order)
                    db.commit()
                else:
                    db.rollback()
            except Exception:
                db.rollback()
                logger.exception("payment_intent webhook processing failed")
            finally:
                db.close()

    return {"received": True}
