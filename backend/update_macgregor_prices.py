import requests

BASE_URL = "https://quingapp-backend.onrender.com"
ADMIN_TOKEN = "PON_AQUI_TU_TOKEN"

HEADERS = {
    "Content-Type": "application/json",
}

PRICE_UPDATES = [

    # =========================
    # POLO
    # =========================
    ("MCG-POL-4", 360),
    ("MCG-POL-6", 360),
    ("MCG-POL-8", 360),
    ("MCG-POL-10", 360),
    ("MCG-POL-12", 360),

    ("MCG-POL-14", 380),
    ("MCG-POL-16", 380),
    ("MCG-POL-CH", 380),

    ("MCG-POL-M", 420),
    ("MCG-POL-G", 420),
    ("MCG-POL-XG", 420),
    ("MCG-POL-XXG", 420),

    # =========================
    # BERMUDA
    # =========================
    ("MCG-BER-4", 360),
    ("MCG-BER-6", 360),
    ("MCG-BER-8", 360),
    ("MCG-BER-10", 360),
    ("MCG-BER-12", 360),

    ("MCG-BER-14", 390),
    ("MCG-BER-16", 390),
    ("MCG-BER-CH", 390),

    ("MCG-BER-M", 450),
    ("MCG-BER-G", 450),
    ("MCG-BER-XG", 450),
    ("MCG-BER-XXG", 450),

    # =========================
    # SHORFALDA
    # =========================
    ("MCG-SHF-4", 390),
    ("MCG-SHF-6", 390),
    ("MCG-SHF-8", 390),
    ("MCG-SHF-10", 390),
    ("MCG-SHF-12", 390),

    ("MCG-SHF-14", 420),
    ("MCG-SHF-16", 420),
    ("MCG-SHF-CH", 420),

    ("MCG-SHF-M", 490),
    ("MCG-SHF-G", 490),
    ("MCG-SHF-XG", 490),
    ("MCG-SHF-XXG", 490),

    # =========================
    # SHORT EDUCACIÓN FÍSICA (SHU)
    # =========================
    ("MCG-SHU-4", 270),
    ("MCG-SHU-6", 270),
    ("MCG-SHU-8", 270),
    ("MCG-SHU-10", 270),
    ("MCG-SHU-12", 270),

    ("MCG-SHU-14", 270),
    ("MCG-SHU-16", 270),
    ("MCG-SHU-CH", 270),

    ("MCG-SHU-M", 290),
    ("MCG-SHU-G", 290),
    ("MCG-SHU-XG", 290),
    ("MCG-SHU-XXG", 290),

    # =========================
    # CALCETAS (SOLO 3 TALLAS)
    # =========================
    ("MCG-CAL-CH", 290),
    ("MCG-CAL-M", 290),
    ("MCG-CAL-G", 290),
]

def update_price(sku, price_mxn):
    url = f"{BASE_URL}/admin/inventory/set-price?token={ADMIN_TOKEN}"
    payload = {
        "sku": sku,
        "price_mxn": price_mxn,
    }

    response = requests.post(url, json=payload, headers=HEADERS)

    if response.status_code == 200:
        print(f"✅ {sku} → ${price_mxn}")
    else:
        print(f"❌ ERROR {sku}: {response.text}")

def main():
    print("🚀 Actualizando precios Macgregor...\n")

    for sku, price in PRICE_UPDATES:
        update_price(sku, price)

    print("\n✅ Terminado")

if __name__ == "__main__":
    main()