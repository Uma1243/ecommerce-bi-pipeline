import pandas as pd
import random
import uuid
from datetime import datetime, timedelta, date

random.seed(42)

# ── CONFIG ────────────────────────────────────────────────────────────────────
NUM_ROWS = 10000
START_DATE = date(2022, 1, 1)
END_DATE   = date(2025, 4, 10)

STORES = [
    "Garden Grove Store",
    "garden grove store",   # dirty variant
    "Pacific Coast",
    "Pacific Coast Store",  # dirty variant
    "Mountain View",
    "Prairie Home",
    "Lakeside Outlet",
    "Lakeside",             # dirty variant
]

PRODUCTS_CLEAN = [
    ("Patio Chair",          "Outdoor Furniture", 89.99),
    ("Garden Hose 50ft",     "Garden",            34.99),
    ("Fire Pit",             "Outdoor Heating",  199.99),
    ("BBQ Grill Pro",        "Cooking",          349.99),
    ("Deck Umbrella",        "Outdoor Furniture", 129.99),
    ("Outdoor Side Table",   "Outdoor Furniture",  59.99),
    ("Solar Garden Light",   "Garden Lighting",   24.99),
    ("Lawn Mower Electric",  "Garden",           299.99),
    ("Hammock Stand",        "Outdoor Furniture", 149.99),
    ("Portable Fire Pit",    "Outdoor Heating",  159.99),
    ("Planter Box Large",    "Garden",            44.99),
    ("Outdoor Rug 8x10",     "Outdoor Furniture",  79.99),
    ("Pressure Washer",      "Garden",           189.99),
    ("Bird Feeder",          "Garden",            19.99),
    ("Compost Bin",          "Garden",            54.99),
]

# Dirty product name variants (simulate real-world messiness)
DIRTY_PRODUCT_MAP = {
    "Patio Chair":         ["patio chair", "PATIO CHAIR", "Patio Chairs", "PatioChair"],
    "BBQ Grill Pro":       ["BBQ Grill", "bbq grill pro", "BBQ grill Pro"],
    "Deck Umbrella":       ["deck umbrella", "Deck Umbrellas"],
    "Fire Pit":            ["fire pit", "FirePit", "FIRE PIT"],
    "Outdoor Side Table":  ["outdoor side table", "Side Table Outdoor"],
    "Hammock Stand":       ["hammock stand", "Hammock Stands"],
}

STATES = ["CA","TX","FL","NY","WA","OR","CO","AZ","IL","GA",
          "NC","VA","OH","PA","MI","NV","UT","MN","MA","TN"]

CUSTOMER_PREFIXES = ["John","Jane","Mike","Sara","Chris","Emma",
                     "Liam","Olivia","Noah","Ava","William","Isabella"]
CUSTOMER_LAST    = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
                    "Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez"]

def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def customer_name():
    return f"{random.choice(CUSTOMER_PREFIXES)} {random.choice(CUSTOMER_LAST)}"

def customer_email(name):
    parts = name.lower().split()
    return f"{parts[0]}.{parts[1]}{random.randint(1,999)}@{'gmail' if random.random()<.6 else 'yahoo'}.com"

rows = []
order_ids_pool = []  # to create duplicates from

for i in range(NUM_ROWS):
    # ── Product ────────────────────────────────────────────────────────────
    prod_name_clean, category, base_price = random.choice(PRODUCTS_CLEAN)

    # 8% chance of dirty product name
    if prod_name_clean in DIRTY_PRODUCT_MAP and random.random() < 0.08:
        prod_name = random.choice(DIRTY_PRODUCT_MAP[prod_name_clean])
    else:
        prod_name = prod_name_clean

    # ── Store ──────────────────────────────────────────────────────────────
    store = random.choice(STORES)

    # ── Dates ─────────────────────────────────────────────────────────────
    order_date = rand_date(START_DATE, END_DATE)

    # 3% ship date BEFORE order date (bug)
    if random.random() < 0.03:
        ship_date = order_date - timedelta(days=random.randint(1, 5))
    # 1% future order date (bug)
    elif random.random() < 0.01:
        order_date = date.today() + timedelta(days=random.randint(1, 30))
        ship_date  = order_date + timedelta(days=random.randint(1, 5))
    else:
        ship_date = order_date + timedelta(days=random.randint(1, 10))

    # ── Quantity / Price / Revenue ─────────────────────────────────────────
    quantity  = random.randint(1, 12)
    discount  = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
    unit_price = round(base_price * random.uniform(0.85, 1.15), 2)

    # 2% zero revenue despite qty > 0 (bug)
    if random.random() < 0.02:
        revenue = 0.00
    else:
        revenue = round(quantity * unit_price * (1 - discount), 2)

    # ── Order ID ───────────────────────────────────────────────────────────
    order_id = str(uuid.uuid4())
    order_ids_pool.append(order_id)

    # 3% duplicate order_id (bug)
    if i > 50 and random.random() < 0.03:
        order_id = random.choice(order_ids_pool[:-1])

    # ── Customer ───────────────────────────────────────────────────────────
    cust_name  = customer_name()
    cust_email = customer_email(cust_name)
    # 1.5% missing email
    if random.random() < 0.015:
        cust_email = None

    # ── Store (1% missing) ─────────────────────────────────────────────────
    if random.random() < 0.01:
        store = None

    rows.append({
        "order_id":       order_id,
        "order_date":     order_date.strftime("%Y-%m-%d"),
        "ship_date":      ship_date.strftime("%Y-%m-%d"),
        "customer_name":  cust_name,
        "customer_email": cust_email,
        "customer_state": random.choice(STATES),
        "product_name":   prod_name,
        "category":       category,
        "quantity":       quantity,
        "unit_price":     unit_price,
        "discount_pct":   discount,
        "revenue":        revenue,
        "store_name":     store,
        "shipping_method":random.choice(["Standard","Express","Overnight","Ground"]),
        "payment_method": random.choice(["Credit Card","PayPal","Debit Card","Gift Card"]),
        "order_status":   random.choice(["Completed","Completed","Completed","Returned","Cancelled"]),
    })

df = pd.DataFrame(rows)
df.to_csv("/home/claude/orders_raw.csv", index=False)

# ── Print quality summary ──────────────────────────────────────────────────────
total = len(df)
dups  = df.duplicated(subset=["order_id"]).sum()
fut   = (pd.to_datetime(df["order_date"]) > pd.Timestamp.today()).sum()
zero_rev = (df["revenue"] == 0).sum()
missing_store = df["store_name"].isna().sum()
missing_email = df["customer_email"].isna().sum()
ship_before = (pd.to_datetime(df["ship_date"]) < pd.to_datetime(df["order_date"])).sum()
dirty_prod = df[df["product_name"].str.lower() != df["product_name"].str.strip().str.lower()].shape[0]

print(f"✅ Generated {total:,} rows → orders_raw.csv")
print(f"\n── Intentional data quality issues ──────────────────")
print(f"  Duplicate order_ids        : {dups:>5}")
print(f"  Future order dates         : {fut:>5}")
print(f"  Zero revenue (qty > 0)     : {zero_rev:>5}")
print(f"  Missing store_name         : {missing_store:>5}")
print(f"  Missing customer_email     : {missing_email:>5}")
print(f"  Ship date before order date: {ship_before:>5}")
print(f"  Dirty product name variants: {(df['product_name'] != df['product_name'].str.strip()).sum():>5}")
print(f"\nShape: {df.shape}")
print(f"\nSample rows:")
print(df.head(3).to_string())
