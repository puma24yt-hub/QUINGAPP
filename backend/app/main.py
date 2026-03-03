from fastapi import FastAPI, Request, HTTPException
import logging
import stripe

from app.core.config import STRIPE_WEBHOOK_SECRET, STRIPE_SECRET_KEY

app = FastAPI(title="QUINGAPP API")

logger = logging.getLogger("quingapp")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Set Stripe secret key
stripe.api_key = STRIPE_SECRET_KEY


@app.get("/")
def root():
    return {"message": "QUINGAPP backend is running"}


# -------------------------
# Checkout (Card + OXXO)
# -------------------------
@app.post("/checkout")
async def create_checkout(payload: dict):
    """
    Creates a Stripe Checkout Session.
    Supports Card + OXXO (Mexico).

    Expected payload example:
    {
      "items": [
        {"name": "Uniforme Secundaria - Talla 10", "unit_amount_mxn": 900, "qty": 1}
      ],
      "customer_name": "Juan Perez",
      "customer_phone": "7441234567"
    }
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe secret key not configured")

    items = payload.get("items") or []
    if not isinstance(items, list) or len(items) == 0:
        raise HTTPException(status_code=400, detail="items is required")

    line_items = []
    for it in items:
        name = str(it.get("name", "")).strip()
        qty = int(it.get("qty", 0) or 0)
        unit_amount_mxn = float(it.get("unit_amount_mxn", 0) or 0)

        if not name or qty <= 0 or unit_amount_mxn <= 0:
            raise HTTPException(status_code=400, detail="Invalid item in items")

        # Stripe expects amount in cents
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

    customer_name = str(payload.get("customer_name", "")).strip()
    customer_phone = str(payload.get("customer_phone", "")).strip()

    # For now we send user to backend placeholders.
    # Later we'll replace with your Flutter deep-link / app page.
    success_url = "https://quingapp-backend.onrender.com/checkout/success?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = "https://quingapp-backend.onrender.com/checkout/cancel"

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card", "oxxo"],
            line_items=line_items,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "customer_name": customer_name,
                "customer_phone": customer_phone,
                "app": "QUINGAPP",
            },
        )
    except Exception as e:
        logger.exception("Stripe Checkout create failed")
        raise HTTPException(status_code=500, detail=f"Stripe error: {str(e)}")

    # session.url is the hosted checkout link
    return {"checkout_url": session.url, "session_id": session.id}


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

        logger.info(
            "Payment completed | session=%s | amount=%s | currency=%s",
            session.get("id"),
            session.get("amount_total"),
            session.get("currency"),
        )

    return {"received": True}