import requests

BASE_URL = "https://quingapp-backend.onrender.com"
ADMIN_TOKEN = "PON_AQUI_TU_TOKEN"

HEADERS = {
    "Content-Type": "application/json",
}

CREATE_ROWS = [
    # POLO
    {"school_code": "MCG", "product_type": "POL", "sizes": ["M", "G", "XG", "XXG"], "stock": 0, "price_mxn": 420},

    # BERMUDA
    {"school_code": "MCG", "product_type": "BER", "sizes": ["M", "G", "XG", "XXG"], "stock": 0, "price_mxn": 450},

    # SHORFALDA
    {"school_code": "MCG", "product_type": "SHF", "sizes": ["M", "G", "XG", "XXG"], "stock": 0, "price_mxn": 490},

    # SHORT EDUCACIÓN FÍSICA
    {"school_code": "MCG", "product_type": "SHU", "sizes": ["M", "G", "XG", "XXG"], "stock": 0, "price_mxn": 290},
]

def create_sizes(row):
    url = f"{BASE_URL}/admin/inventory/bulk-create?token={ADMIN_TOKEN}"
    response = requests.post(url, json=row, headers=HEADERS)

    if response.status_code == 200:
        print(f"✅ {row['product_type']} -> {row['sizes']}")
    else:
        print(f"❌ ERROR {row['product_type']}: {response.text}")

def main():
    print("🚀 Creando tallas faltantes Macgregor...\n")

    for row in CREATE_ROWS:
        create_sizes(row)

    print("\n✅ Terminado")

if __name__ == "__main__":
    main()