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

    # Evento principal
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]

        logger.info(
            "Payment completed | session=%s | amount=%s | currency=%s",
            session.get("id"),
            session.get("amount_total"),
            session.get("currency"),
        )

    return {"received": True}