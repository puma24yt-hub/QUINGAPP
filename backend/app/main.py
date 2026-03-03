from fastapi import FastAPI, Request, HTTPException
import logging
import stripe
import uuid
import secrets
import os
from datetime import datetime, timezone, timedelta

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, ForeignKey
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

    total_mxn = Column(Integer, nullable=False, default=0)  # stored in MXN pesos (int)

    created_at = Column(DateTime(timezone=True), nullable=False)
    paid_at = Column(DateTime(timezone=True), nullable=True)

    expires_at = Column(DateTime(timezone=True), nullable=True)     # paid_at + 30d
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    pickup_status = Column(String(32), nullable=True)               # ACTIVE | DELIVERED | EXPIRED
    pickup_code = Column(String(32), nullable=True)                 # visible code
    pickup_token = Column(String(64), nullable=True)                # uuid/token for QR payload

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


def _normalize_db_url(url: str) -> str:
    # Render gives postgresql://... which works with psycopg2 in SQLAlchemy
    return (url or "").strip()

if not DATABASE_URL:
    logger.warning("DATABASE_URL is empty. DB features will fail until configured in Render.")

_engine = create_engine(_normalize_db_url(DATABASE_URL), pool_pre_ping=True) if DATABASE_URL else None
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False) if _engine else None


@app.on_event("startup")
def _startup_create_tables():
    if not _engine:
        logger.warning("Startup: DB not configured, skipping create_all.")
        return
    Base.metadata.create_all(bind=_engine)
    logger.info("DB tables ensured (create_all).")


def _now_utc():
    return datetime.now(timezone.utc)


def _generate_pickup_code() -> str:
    # Example: QNG-8F4K29
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    code = "".join(secrets.choice(alphabet) for _ in range(6))
    return f"QNG-{code}"


@app.get("/")
def root():
    return {"message": "QUINGAPP backend is running"}


# -------------------------
# Admin (VIEW ORDERS) - TEMP
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
            out.append({
                "id": o.id,
                "status": o.status,
                "customer_name": o.customer_name,
                "customer_phone": o.customer_phone,
                "total_mxn": o.total_mxn,
                "created_at": _dt(o.created_at),
                "paid_at": _dt(o.paid_at),
                "expires_at": _dt(o.expires_at),
                "delivered_at": _dt(o.delivered_at),
                "pickup_status": o.pickup_status,
                "pickup_code": o.pickup_code,
                "pickup_token": o.pickup_token,
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
                ]
            })

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

        return {
            "ok": True,
            "order": {
                "id": o.id,
                "status": o.status,
                "customer_name": o.customer_name,
                "customer_phone": o.customer_phone,
                "total_mxn": o.total_mxn,
                "created_at": _dt(o.created_at),
                "paid_at": _dt(o.paid_at),
                "expires_at": _dt(o.expires_at),
                "delivered_at": _dt(o.delivered_at),
                "pickup_status": o.pickup_status,
                "pickup_code": o.pickup_code,
                "pickup_token": o.pickup_token,
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
                ]
            }
        }
    finally:
        db.close()


# -------------------------
# Checkout (Card + OXXO)
# -------------------------
@app.post("/checkout")
async def create_checkout(payload: dict):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe secret key not configured")

    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    items = payload.get("items") or []
    if not isinstance(items, list) or len(items) == 0:
        raise HTTPException(status_code=400, detail="items is required")

    customer_name = str(payload.get("customer_name", "")).strip()
    customer_phone = str(payload.get("customer_phone", "")).strip()

    if not customer_name:
        raise HTTPException(status_code=400, detail="customer_name is required")

    # Validate items + build totals
    total_mxn = 0
    cleaned_items = []
    line_items = []

    for it in items:
        name = str(it.get("name", "")).strip()
        qty = int(it.get("qty", 0) or 0)
        unit_amount_mxn = float(it.get("unit_amount_mxn", 0) or 0)

        if not name or qty <= 0 or unit_amount_mxn <= 0:
            raise HTTPException(status_code=400, detail="Invalid item in items")

        unit_amount_pesos = int(round(unit_amount_mxn))
        total_mxn += unit_amount_pesos * qty

        cleaned_items.append(
            {"name": name, "qty": qty, "unit_amount_mxn": unit_amount_pesos}
        )

        unit_amount_cents = int(round(unit_amount_mxn * 100))
        line_items.append(
            {
                "quantity": qty,
                "price_data": {
                    "currency": "mxn",
                    "product_data": {"name": name},
                    "unit_amount": unit_amount_cents,
                },
            }
        )

    # Create Order in DB (PENDING_PAYMENT)
    db = SessionLocal()
    try:
        order = Order(
            status="PENDING_PAYMENT",
            customer_name=customer_name,
            customer_phone=customer_phone,
            total_mxn=total_mxn,
            created_at=_now_utc(),
        )
        db.add(order)
        db.flush()  # get order.id

        for it in cleaned_items:
            db.add(
                OrderItem(
                    order_id=order.id,
                    name=it["name"],
                    qty=it["qty"],
                    unit_amount_mxn=it["unit_amount_mxn"],
                )
            )

        db.commit()
        db.refresh(order)
    except Exception:
        db.rollback()
        logger.exception("DB create order failed")
        raise HTTPException(status_code=500, detail="DB error creating order")
    finally:
        db.close()

    # Stripe redirect placeholders (later we replace with your Flutter flow)
    success_url = "https://quingapp-backend.onrender.com/checkout/success?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = "https://quingapp-backend.onrender.com/checkout/cancel"

    # Create Stripe Checkout Session
    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card", "oxxo"],
            line_items=line_items,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "order_id": str(order.id),
                "customer_name": customer_name,
                "customer_phone": customer_phone,
                "app": "QUINGAPP",
            },
        )
    except Exception as e:
        logger.exception("Stripe Checkout create failed")
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")

    return {"checkout_url": session.url, "session_id": session.id, "order_id": order.id}


@app.get("/checkout/success")
def checkout_success(session_id: str = ""):
    return {
        "ok": True,
        "message": "Pago completado. (placeholder) Luego aquí mostraremos el QR/código en la app.",
        "session_id": session_id,
    }


@app.get("/checkout/cancel")
def checkout_cancel():
    return {"ok": False, "message": "Pago cancelado. (placeholder)"}


# -------------------------
# Stripe Webhook
# -------------------------
@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]

        stripe_session_id = session.get("id")
        payment_status = session.get("payment_status")
        amount_total = session.get("amount_total")
        currency = session.get("currency")

        meta = session.get("metadata") or {}
        order_id_raw = (meta.get("order_id") or "").strip()

        logger.info(
            "checkout.session.completed | order_id=%s | session=%s | payment_status=%s | amount_total=%s %s",
            order_id_raw, stripe_session_id, payment_status, amount_total, currency
        )

        if SessionLocal and order_id_raw.isdigit():
            order_id = int(order_id_raw)
            db = SessionLocal()
            try:
                order = db.query(Order).filter(Order.id == order_id).first()
                if order and order.status != "PAID":
                    paid_at = _now_utc()
                    order.status = "PAID"
                    order.paid_at = paid_at
                    order.expires_at = paid_at + timedelta(days=30)

                    order.pickup_status = "ACTIVE"
                    order.pickup_code = _generate_pickup_code()
                    order.pickup_token = str(uuid.uuid4())

                    db.add(
                        Payment(
                            order_id=order.id,
                            stripe_session_id=str(stripe_session_id or ""),
                            payment_status=str(payment_status or ""),
                            amount_total_cents=int(amount_total or 0),
                            currency=str(currency or ""),
                            created_at=_now_utc(),
                        )
                    )

                    db.commit()
            except Exception:
                db.rollback()
                logger.exception("DB update on webhook failed")
            finally:
                db.close()

    return {"received": True}