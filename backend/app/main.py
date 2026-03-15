from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
import logging
import stripe
import uuid
import secrets
import os
from datetime import datetime, timezone, timedelta
import re

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, ForeignKey, text, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from app.core.config import (
    STRIPE_WEBHOOK_SECRET, STRIPE_SECRET_KEY, DATABASE_URL
)

app = FastAPI(title="QUINGAPP API")

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

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    payments = relationship("Payment", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)

    name = Column(String(250), nullable=False)
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
else:
    logger.warning("DATABASE_URL not configured — DB endpoints will fail until configured.")


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
        "items": [
            {"id": it.id, "name": it.name, "qty": it.qty, "unit_amount_mxn": it.unit_amount_mxn}
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
        "items": [
            {"name": it.name, "qty": it.qty, "unit_amount_mxn": it.unit_amount_mxn}
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
        qty = int(it.get("qty", 0) or 0)
        unit_amount_mxn = float(it.get("unit_amount_mxn", it.get("unit_price_mxn", 0)) or 0)

        if not name or qty <= 0 or unit_amount_mxn <= 0:
            raise HTTPException(status_code=400, detail="Invalid item in items")

        unit_amount_pesos = int(round(unit_amount_mxn))
        total_mxn += unit_amount_pesos * qty

        cleaned_items.append({"name": name, "qty": qty, "unit_amount_mxn": unit_amount_pesos})

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


def _send_note_email_if_configured(order: Order):
    """Returns (ok: bool, err: str). Sends only if SMTP is configured."""
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "0") or "0")
    user = os.getenv("SMTP_USER", "").strip()
    pwd = os.getenv("SMTP_PASS", "").strip()
    from_email = os.getenv("SMTP_FROM", "").strip()

    if not host or not port or not from_email:
        return False, "SMTP not configured"

    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = f"Nota de venta QUING — Pedido #{order.id}"
        msg["From"] = from_email
        msg["To"] = order.customer_email

        lines = [
            "Gracias por tu compra en QUING.",
            f"Pedido: #{order.id}",
            f"Nombre: {order.customer_name}",
            f"Total: ${order.total_mxn} MXN",
            "",
            "Detalle:",
        ]
        for it in (order.items or []):
            lines.append(f"- {it.name} x{it.qty} — ${it.unit_amount_mxn} MXN")
        lines += [
            "",
            f"Código de entrega: {order.pickup_code}",
            f"Fecha límite para recoger: {_dt(order.expires_at)}" if order.expires_at else "",
            "",
            "Muestra tu QR y código de entrega en tienda.",
        ]
        msg.set_content("\n".join([x for x in lines if x != ""]))

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

        # Send sales note once (if email configured and order has email)
        if order.customer_email and order.note_status in ("PENDING", "FAILED"):
            ok, err = _send_note_email_if_configured(order)
            if ok:
                _mark_note_status(db, order, "SENT", "")
            else:
                _mark_note_status(db, order, "FAILED", err)
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
                    if order.customer_email and order.note_status in ("PENDING", "FAILED"):
                        ok, err = _send_note_email_if_configured(order)
                        if ok:
                            _mark_note_status(db, order, "SENT", "")
                        else:
                            _mark_note_status(db, order, "FAILED", err)

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
                    if order.customer_email and order.note_status in ("PENDING", "FAILED"):
                        ok, err = _send_note_email_if_configured(order)
                        if ok:
                            _mark_note_status(db, order, "SENT", "")
                        else:
                            _mark_note_status(db, order, "FAILED", err)
                    db.commit()
                else:
                    db.rollback()
            except Exception:
                db.rollback()
                logger.exception("payment_intent webhook processing failed")
            finally:
                db.close()

    return {"received": True}
