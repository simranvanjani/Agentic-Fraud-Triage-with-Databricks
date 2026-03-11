#!/usr/bin/env python3
"""
Mock banking dataset for Agentic Fraud Triage.
Generates: transactions (amount, merchant_id, type, timestamp), login_logs (user_id, ip_address, timestamp, mfa_change_flag, lat, lon for impossible travel).
Output: CSV files to upload to Unity Catalog Volume (Financial_Security.raw.mock_source_data).
Run locally or in Databricks; then upload CSVs to the volume.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent / "mock_data"
NUM_USERS = 80
NUM_TRANSACTIONS = 1500
NUM_LOGINS = 2000
# Seed some fraud patterns
FRAUD_USER_IDS = ["USER-FRAUD-001", "USER-FRAUD-002", "USER-FRAUD-003"]
HIGH_RISK_MERCHANTS = ["MERCHANT-WIRE-001", "MERCHANT-WIRE-002"]

def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def generate_transactions():
    rows = []
    base = datetime.utcnow() - timedelta(days=7)
    for i in range(NUM_TRANSACTIONS):
        is_fraud = random.random() < 0.02 or (i % 200 == 0)
        user_id = random.choice(FRAUD_USER_IDS) if is_fraud else f"USER-{random.randint(1, NUM_USERS):05d}"
        amount = round(random.uniform(10, 500), 2)
        if is_fraud or random.random() < 0.05:
            amount = round(random.uniform(10000, 25000), 2)
        txn_type = "wire" if amount >= 10000 or (is_fraud and random.random() < 0.7) else random.choice(["ach", "pos", "card"])
        merchant_id = random.choice(HIGH_RISK_MERCHANTS) if txn_type == "wire" else f"MERCHANT-{random.randint(1, 50):03d}"
        t = base + timedelta(seconds=random.randint(0, 7*24*3600))
        rows.append({
            "transaction_id": f"TXN-{i:08d}",
            "user_id": user_id,
            "amount": amount,
            "merchant_id": merchant_id,
            "transaction_type": txn_type,
            "timestamp": ts(t),
        })
    return rows

def generate_login_logs():
    rows = []
    base = datetime.utcnow() - timedelta(days=7)
    # Geographic points for impossible travel (lat, lon, city)
    locations = [
        (40.71, -74.01, "New York"),
        (22.28, 114.16, "Hong Kong"),
        (51.51, -0.13, "London"),
        (34.05, -118.24, "LA"),
        (41.88, -87.63, "Chicago"),
    ]
    for i in range(NUM_LOGINS):
        is_fraud = random.random() < 0.015
        user_id = random.choice(FRAUD_USER_IDS) if is_fraud else f"USER-{random.randint(1, NUM_USERS):05d}"
        ip = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        t = base + timedelta(seconds=random.randint(0, 7*24*3600))
        mfa_change_flag = "True" if (is_fraud and random.random() < 0.5) or random.random() < 0.02 else "False"
        loc = random.choice(locations)
        device_id = f"device-{random.randint(1,20):03d}"
        rows.append({
            "login_id": f"LOGIN-{i:08d}",
            "user_id": user_id,
            "ip_address": ip,
            "timestamp": ts(t),
            "mfa_change_flag": mfa_change_flag,
            "latitude": loc[0],
            "longitude": loc[1],
            "city": loc[2],
            "device_id": device_id,
        })
    return rows

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    transactions = generate_transactions()
    logins = generate_login_logs()

    with open(OUTPUT_DIR / "transactions.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["transaction_id", "user_id", "amount", "merchant_id", "transaction_type", "timestamp"])
        w.writeheader()
        w.writerows(transactions)
    with open(OUTPUT_DIR / "login_logs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["login_id", "user_id", "ip_address", "timestamp", "mfa_change_flag", "latitude", "longitude", "city", "device_id"])
        w.writeheader()
        w.writerows(logins)

    print(f"Wrote {len(transactions)} transactions -> mock_data/transactions.csv")
    print(f"Wrote {len(logins)} login_logs -> mock_data/login_logs.csv")
    print("Upload these to Financial_Security.raw.mock_source_data volume.")

if __name__ == "__main__":
    main()
