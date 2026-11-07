"""Seeded synthetic banking data -> Unity Catalog volume (CSV).
Run as a Lakeflow Jobs spark_python_task or from a notebook.

Example: python generate_banking_data.py --root /Volumes/bank_dev/raw/landing \
             --start 2026-01-01 --days 30
"""
import argparse
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker  # %pip install faker (notebook) or add to job environment

REGIONS = {
    "Northeast": ["NY", "MA", "NJ"],
    "Southeast": ["FL", "GA", "NC"],
    "Midwest": ["IL", "OH", "MI"],
    "West": ["CA", "WA", "AZ"],
}


def _write(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def build_reference(root: str, n_customers=10_000, n_branches=40, seed=42):
    rng = np.random.default_rng(seed)
    fake = Faker()
    Faker.seed(seed)

    states = [(r, s) for r, ss in REGIONS.items() for s in ss]
    picks = [states[i % len(states)] for i in range(n_branches)]
    branches = pd.DataFrame({
        "branch_id": [f"BR{i:03d}" for i in range(n_branches)],
        "branch_name": [f"{fake.city()} Branch" for _ in range(n_branches)],
        "region": [p[0] for p in picks],
        "state": [p[1] for p in picks],
        "opened_date": [fake.date_between("-20y", "-1y") for _ in range(n_branches)],
    })

    customers = pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(n_customers)],
        "first_name": [fake.first_name() for _ in range(n_customers)],
        "last_name": [fake.last_name() for _ in range(n_customers)],
        "email": [fake.email() for _ in range(n_customers)],
        "phone": [fake.phone_number() for _ in range(n_customers)],
        "ssn": [fake.ssn() for _ in range(n_customers)],
        "date_of_birth": [fake.date_of_birth(minimum_age=18, maximum_age=90) for _ in range(n_customers)],
        "state": rng.choice([s for _, s in states], n_customers),
        "segment": rng.choice(["mass", "affluent", "private"], n_customers, p=[0.75, 0.20, 0.05]),
        "credit_score": rng.integers(300, 851, n_customers),
        "kyc_status": rng.choice(["verified", "pending", "expired"], n_customers, p=[0.90, 0.07, 0.03]),
        "created_at": [fake.date_time_between("-8y", "-1y").isoformat() for _ in range(n_customers)],
    })

    n_acc = int(n_customers * 1.5)
    accounts = pd.DataFrame({
        "account_id": [f"A{i:07d}" for i in range(n_acc)],
        "customer_id": rng.choice(customers["customer_id"], n_acc),
        "branch_id": rng.choice(branches["branch_id"], n_acc),
        "account_type": rng.choice(["CHECKING", "SAVINGS", "CREDIT_CARD", "LOAN"], n_acc, p=[0.45, 0.30, 0.15, 0.10]),
        "status": rng.choice(["ACTIVE", "DORMANT", "CLOSED"], n_acc, p=[0.85, 0.10, 0.05]),
        "opening_balance": np.round(rng.lognormal(8, 1.2, n_acc), 2),
        "open_date": [fake.date_between("-8y", "-1y").isoformat() for _ in range(n_acc)],
    })

    _write(branches, f"{root}/branches/branches.csv")
    _write(customers, f"{root}/customers/customers.csv")
    _write(accounts, f"{root}/accounts/accounts.csv")
    return accounts


def build_transactions(root: str, accounts: pd.DataFrame, start: date, days: int,
                       per_day=30_000, seed=7):
    ids = accounts["account_id"].to_numpy()
    for d in range(days):
        day = start + timedelta(days=d)
        rng = np.random.default_rng(seed + d)
        n = per_day
        fraud = rng.random(n) < 0.004
        amount = np.round(rng.lognormal(3.5, 1.1, n), 2)
        amount = np.where(fraud, np.round(amount * rng.uniform(5, 20, n), 2), amount)
        df = pd.DataFrame({
            "transaction_id": [f"T{day:%Y%m%d}{i:07d}" for i in range(n)],
            "account_id": rng.choice(ids, n),
            "txn_ts": pd.to_datetime(day) + pd.to_timedelta(rng.integers(0, 86400, n), unit="s"),
            "txn_type": rng.choice(["DEBIT", "CREDIT", "TRANSFER", "WITHDRAWAL", "PAYMENT"], n,
                                   p=[0.40, 0.20, 0.15, 0.10, 0.15]),
            "channel": rng.choice(["POS", "ONLINE", "MOBILE", "ATM", "BRANCH"], n,
                                  p=[0.35, 0.25, 0.25, 0.10, 0.05]),
            "amount": amount,
            "merchant_category": rng.choice(["grocery", "fuel", "travel", "retail", "dining", "utilities", "crypto"],
                                            n, p=[0.25, 0.12, 0.10, 0.20, 0.18, 0.12, 0.03]),
            "merchant_country": rng.choice(["US", "CA", "MX", "GB", "NG", "RU"], n,
                                           p=[0.93, 0.02, 0.02, 0.015, 0.01, 0.005]),
            "fraud_flag": fraud.astype(int),
        })
        # Plant dirty rows so Silver expectations have work to do
        df = pd.concat([df, df.sample(frac=0.002, random_state=d)], ignore_index=True)       # duplicates
        df.loc[df.sample(frac=0.001, random_state=d + 1).index, "amount"] = np.nan          # null amount
        df.loc[df.sample(frac=0.001, random_state=d + 2).index, "txn_type"] = "UNKNOWN"     # bad code
        _write(df, f"{root}/transactions/dt={day}/transactions_{day:%Y%m%d}.csv")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--days", type=int, default=30)
    a = p.parse_args()
    accts = build_reference(a.root)
    build_transactions(a.root, accts, date.fromisoformat(a.start), a.days)