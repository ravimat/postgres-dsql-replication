"""
CDC Load Test — Amazon-Style Order System
==========================================
Simulates a realistic e-commerce order pipeline (like Amazon.com) with
multiple related tables and realistic DML patterns to stress-test the
CDC replication from PostgreSQL → Aurora DSQL.

Tables:
  - sample_customers        (CRUD, mostly INSERT on signup, occasional UPDATE)
  - sample_products         (CRUD, infrequent changes)
  - sample_orders           (INSERT-heavy, status UPDATEs through lifecycle)
  - sample_order_items      (INSERT with order, rarely updated)
  - sample_payments         (INSERT when order placed, UPDATE on status change)
  - sample_shipments        (INSERT when shipped, UPDATE for tracking)
  - sample_inventory        (UPDATE-heavy, decrement on order, increment on restock)

Realistic patterns:
  - Order lifecycle: pending → confirmed → shipped → delivered
  - Payment lifecycle: pending → authorized → captured → settled
  - Inventory decrements on order, restocks periodically
  - Cascading writes: 1 order = INSERT sample_orders + N INSERT sample_order_items +
    INSERT payment + UPDATE sample_inventory (per item)

Usage:
  # Run with defaults (5 min, 20 sample_orders/sec)
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN"

  # High-throughput burst
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
      --duration 600 --sample_orders-per-sec 100 --threads 8

  # Just create the schema (no load)
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" --setup-only
"""

import argparse
import json
import os
import sys
import time
import random
import string
import uuid
import threading
import statistics
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Schema Definition
# ---------------------------------------------------------------------------

SOURCE_SCHEMA = """
-- Customers
CREATE TABLE IF NOT EXISTS public.sample_customers (
    customer_id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    phone TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT DEFAULT 'US',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Products
CREATE TABLE IF NOT EXISTS public.sample_products (
    product_id BIGSERIAL PRIMARY KEY,
    sku TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    weight_kg NUMERIC(6, 3),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Inventory
CREATE TABLE IF NOT EXISTS public.sample_inventory (
    product_id BIGINT PRIMARY KEY REFERENCES public.sample_products(product_id),
    quantity_available INT NOT NULL DEFAULT 0,
    quantity_reserved INT NOT NULL DEFAULT 0,
    warehouse_code TEXT DEFAULT 'WH-01',
    last_restocked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Orders
CREATE TABLE IF NOT EXISTS public.sample_orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES public.sample_customers(customer_id),
    order_status TEXT NOT NULL DEFAULT 'pending',
    subtotal NUMERIC(12, 2) NOT NULL,
    tax NUMERIC(10, 2) NOT NULL DEFAULT 0,
    shipping_cost NUMERIC(8, 2) NOT NULL DEFAULT 0,
    total NUMERIC(12, 2) NOT NULL,
    shipping_address TEXT,
    order_date TIMESTAMPTZ DEFAULT NOW(),
    confirmed_at TIMESTAMPTZ,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Order Items
CREATE TABLE IF NOT EXISTS public.sample_order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES public.sample_orders(order_id),
    product_id BIGINT NOT NULL REFERENCES public.sample_products(product_id),
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Payments
CREATE TABLE IF NOT EXISTS public.sample_payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES public.sample_orders(order_id),
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL DEFAULT 'pending',
    amount NUMERIC(12, 2) NOT NULL,
    currency TEXT DEFAULT 'USD',
    transaction_ref TEXT,
    authorized_at TIMESTAMPTZ,
    captured_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Shipments
CREATE TABLE IF NOT EXISTS public.sample_shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES public.sample_orders(order_id),
    carrier TEXT NOT NULL,
    tracking_number TEXT,
    shipment_status TEXT NOT NULL DEFAULT 'preparing',
    shipped_at TIMESTAMPTZ,
    estimated_delivery TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sample_orders_customer ON public.sample_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_sample_orders_status ON public.sample_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_sample_order_items_order ON public.sample_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_sample_payments_order ON public.sample_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_sample_shipments_order ON public.sample_shipments(order_id);
CREATE INDEX IF NOT EXISTS idx_sample_inventory_product ON public.sample_inventory(product_id);
"""

# DSQL-compatible schema (no SERIAL, no FK REFERENCES, no DEFAULT NOW())
DSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS public.sample_customers (
    customer_id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    phone TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_products (
    product_id BIGINT PRIMARY KEY,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    weight_kg NUMERIC(6, 3),
    is_active BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_inventory (
    product_id BIGINT PRIMARY KEY,
    quantity_available INT NOT NULL,
    quantity_reserved INT NOT NULL,
    warehouse_code TEXT,
    last_restocked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_status TEXT NOT NULL,
    subtotal NUMERIC(12, 2) NOT NULL,
    tax NUMERIC(10, 2) NOT NULL,
    shipping_cost NUMERIC(8, 2) NOT NULL,
    total NUMERIC(12, 2) NOT NULL,
    shipping_address TEXT,
    order_date TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency TEXT,
    transaction_ref TEXT,
    authorized_at TIMESTAMPTZ,
    captured_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.sample_shipments (
    shipment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    carrier TEXT NOT NULL,
    tracking_number TEXT,
    shipment_status TEXT NOT NULL,
    shipped_at TIMESTAMPTZ,
    estimated_delivery TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
"""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class OrderTestConfig:
    source_dsn: str
    target_dsn: str
    duration_seconds: int = 300
    sample_orders_per_sec: int = 20          # Target sample_orders/second
    threads: int = 4
    report_dir: str = "results"
    seed_sample_customers: int = 500
    seed_sample_products: int = 200
    warmup_seconds: int = 15          # Wait for CDC catch-up before validating
    seed: int = 42

    @classmethod
    def from_args(cls, args) -> "OrderTestConfig":
        return cls(
            source_dsn=args.source_dsn or os.environ.get("SOURCE_DSN", ""),
            target_dsn=args.target_dsn or os.environ.get("TARGET_DSN", ""),
            duration_seconds=args.duration,
            sample_orders_per_sec=args.sample_orders_per_sec,
            threads=args.threads,
            report_dir=args.report_dir,
            seed_sample_customers=args.seed_sample_customers,
            seed_sample_products=args.seed_sample_products,
        )


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class TestMetrics:
    """Tracks all test metrics."""
    start_time: float = 0
    end_time: float = 0
    sample_orders_placed: int = 0
    sample_orders_confirmed: int = 0
    sample_orders_shipped: int = 0
    sample_orders_delivered: int = 0
    sample_payments_created: int = 0
    sample_shipments_created: int = 0
    sample_inventory_updates: int = 0
    total_dml_operations: int = 0
    errors: int = 0
    error_samples: List = field(default_factory=list)
    latencies_ms: List = field(default_factory=list)
    tps_per_second: Dict = field(default_factory=dict)

    def record_latency(self, ms: float):
        self.latencies_ms.append(ms)

    def record_error(self, err: str):
        self.errors += 1
        if len(self.error_samples) < 10:
            self.error_samples.append(err[:200])

    def summary(self) -> Dict:
        duration = self.end_time - self.start_time if self.end_time else 1
        latencies = sorted(self.latencies_ms) if self.latencies_ms else [0]
        return {
            "duration_seconds": round(duration, 1),
            "sample_orders_placed": self.sample_orders_placed,
            "sample_orders_confirmed": self.sample_orders_confirmed,
            "sample_orders_shipped": self.sample_orders_shipped,
            "sample_orders_delivered": self.sample_orders_delivered,
            "total_dml_operations": self.total_dml_operations,
            "effective_tps": round(self.total_dml_operations / duration, 1),
            "sample_orders_per_sec": round(self.sample_orders_placed / duration, 1),
            "errors": self.errors,
            "latency_avg_ms": round(statistics.mean(latencies), 2) if latencies else 0,
            "latency_p50_ms": round(latencies[len(latencies)//2], 2),
            "latency_p95_ms": round(latencies[int(len(latencies)*0.95)], 2) if len(latencies) > 20 else 0,
            "latency_p99_ms": round(latencies[int(len(latencies)*0.99)], 2) if len(latencies) > 100 else 0,
            "error_samples": self.error_samples,
        }


# ---------------------------------------------------------------------------
# Data Generators
# ---------------------------------------------------------------------------

class DataGenerator:
    """Generates realistic e-commerce data."""

    FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                   "Michael", "Linda", "David", "Elizabeth", "Ravi", "Priya",
                   "Wei", "Yuki", "Carlos", "Sofia", "Ahmed", "Fatima"]
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Patel", "Kumar",
                  "Wang", "Chen", "Kim", "Tanaka", "Silva", "Müller"]
    CITIES = ["Seattle", "New York", "San Francisco", "Austin", "Chicago",
              "Denver", "Portland", "Boston", "Miami", "Dallas", "Atlanta"]
    STATES = ["WA", "NY", "CA", "TX", "IL", "CO", "OR", "MA", "FL", "TX", "GA"]
    CATEGORIES = ["Electronics", "Books", "Clothing", "Home & Kitchen",
                  "Sports", "Toys", "Health", "Automotive", "Garden", "Office"]
    CARRIERS = ["UPS", "FedEx", "USPS", "DHL", "Amazon Logistics"]
    PAYMENT_METHODS = ["credit_card", "debit_card", "gift_card", "prime_balance", "paypal"]

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def customer(self, idx: int) -> Dict:
        fn = self._rng.choice(self.FIRST_NAMES)
        ln = self._rng.choice(self.LAST_NAMES)
        city_idx = self._rng.randint(0, len(self.CITIES) - 1)
        return {
            "email": f"{fn.lower()}.{ln.lower()}{idx}@example.com",
            "first_name": fn,
            "last_name": ln,
            "phone": f"+1{self._rng.randint(200,999)}{self._rng.randint(1000000,9999999)}",
            "address_line1": f"{self._rng.randint(100,9999)} {ln} St",
            "city": self.CITIES[city_idx],
            "state": self.STATES[city_idx],
            "zip_code": f"{self._rng.randint(10000,99999)}",
            "country": "US",
        }

    def product(self, idx: int) -> Dict:
        cat = self._rng.choice(self.CATEGORIES)
        adjectives = ["Premium", "Basic", "Pro", "Ultra", "Eco", "Smart", "Classic"]
        nouns = ["Widget", "Gadget", "Device", "Tool", "Kit", "Set", "Pack"]
        name = f"{self._rng.choice(adjectives)} {cat} {self._rng.choice(nouns)} {idx}"
        return {
            "sku": f"SKU-{cat[:3].upper()}-{idx:06d}",
            "name": name,
            "description": f"High-quality {cat.lower()} product. Model #{idx}.",
            "category": cat,
            "price": round(self._rng.uniform(4.99, 999.99), 2),
            "weight_kg": round(self._rng.uniform(0.1, 25.0), 3),
            "is_active": True,
        }

    def tracking_number(self) -> str:
        return f"1Z{''.join(self._rng.choices(string.ascii_uppercase + string.digits, k=16))}"


# ---------------------------------------------------------------------------
# Order System Load Generator
# ---------------------------------------------------------------------------

class OrderSystemLoadTest:
    """
    Simulates realistic order lifecycle:
    
    1. Place Order:
       - INSERT into sample_orders
       - INSERT into sample_order_items (1-5 items)
       - INSERT into sample_payments
       - UPDATE sample_inventory (decrement per item)
    
    2. Confirm Order (after ~2s):
       - UPDATE sample_orders SET status = 'confirmed'
       - UPDATE sample_payments SET status = 'authorized'
    
    3. Ship Order (after ~5s):
       - UPDATE sample_orders SET status = 'shipped'
       - INSERT into sample_shipments
       - UPDATE sample_payments SET status = 'captured'
    
    4. Deliver Order (after ~10s):
       - UPDATE sample_orders SET status = 'delivered'
       - UPDATE sample_shipments SET status = 'delivered'
       - UPDATE sample_payments SET status = 'settled'
    
    5. Periodic Inventory Restock:
       - UPDATE sample_inventory SET quantity_available += random
    """

    def __init__(self, config: OrderTestConfig, metrics: TestMetrics):
        self.config = config
        self.metrics = metrics
        self.gen = DataGenerator(config.seed)
        self._running = False
        self._lock = threading.Lock()

        # Track pending sample_orders for lifecycle progression
        self._pending_sample_orders: List[Dict] = []     # awaiting confirmation
        self._confirmed_sample_orders: List[Dict] = []   # awaiting shipment
        self._shipped_sample_orders: List[Dict] = []     # awaiting delivery

        # Cached IDs
        self._customer_ids: List[int] = []
        self._product_ids: List[int] = []
        self._product_prices: Dict[int, float] = {}

    def setup_schema(self):
        """Create tables on source and target."""
        print("Creating schema on source (RDS PostgreSQL)...")
        with psycopg2.connect(self.config.source_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Execute each statement separately
                for stmt in SOURCE_SCHEMA.split(";"):
                    # Remove comment-only lines, keep SQL
                    lines = [l for l in stmt.split('\n') 
                             if l.strip() and not l.strip().startswith('--')]
                    clean_stmt = '\n'.join(lines).strip()
                    if clean_stmt:
                        cur.execute(clean_stmt)
        print("✓ Source schema created")

        # Set REPLICA IDENTITY and add to publication for CDC
        print("Setting up REPLICA IDENTITY and publication for sample tables...")
        sample_tables = ['sample_customers', 'sample_products', 'sample_inventory',
                         'sample_orders', 'sample_order_items', 'sample_payments', 'sample_shipments']
        with psycopg2.connect(self.config.source_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for table in sample_tables:
                    cur.execute(f"ALTER TABLE public.{table} REPLICA IDENTITY FULL")
                try:
                    pub_tables = ', '.join(f'public.{t}' for t in sample_tables)
                    cur.execute(f"ALTER PUBLICATION dsql_cdc_pub ADD TABLE {pub_tables}")
                except Exception as e:
                    if 'already member' not in str(e):
                        print(f"  Note: {e}")
        print("✓ REPLICA IDENTITY + publication configured")

        if not self.config.target_dsn:
            print("⚠ No target DSN provided — skipping target schema creation (CDC service will replicate)")
            return
        print("Creating schema on target (Aurora DSQL)...")
        try:
            # DSQL requires each DDL in its own transaction/connection
            for stmt in DSQL_SCHEMA.split(";"):
                lines = [l for l in stmt.split('\n')
                         if l.strip() and not l.strip().startswith('--')]
                clean_stmt = '\n'.join(lines).strip()
                if not clean_stmt:
                    continue
                try:
                    conn = psycopg2.connect(self.config.target_dsn)
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(clean_stmt)
                    conn.close()
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"  ⚠️  {str(e)[:80]}")
            print("✓ Target schema created (DSQL)")
        except Exception as e:
            print(f"⚠️  Target schema: {e}")

    def seed_data(self):
        """Seed sample_customers and sample_products."""
        print(f"Seeding {self.config.seed_sample_customers} sample_customers and "
              f"{self.config.seed_sample_products} sample_products...")

        with psycopg2.connect(self.config.source_dsn) as conn:
            with conn.cursor() as cur:
                # Seed sample_customers
                for i in range(self.config.seed_sample_customers):
                    c = self.gen.customer(i)
                    cur.execute("""
                        INSERT INTO public.sample_customers 
                            (email, first_name, last_name, phone, address_line1,
                             city, state, zip_code, country)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING
                        RETURNING customer_id
                    """, (c["email"], c["first_name"], c["last_name"], c["phone"],
                          c["address_line1"], c["city"], c["state"], c["zip_code"], c["country"]))
                    row = cur.fetchone()
                    if row:
                        self._customer_ids.append(row[0])

                # Seed sample_products + sample_inventory
                for i in range(self.config.seed_sample_products):
                    p = self.gen.product(i)
                    cur.execute("""
                        INSERT INTO public.sample_products
                            (sku, name, description, category, price, weight_kg, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sku) DO NOTHING
                        RETURNING product_id, price
                    """, (p["sku"], p["name"], p["description"], p["category"],
                          p["price"], p["weight_kg"], p["is_active"]))
                    row = cur.fetchone()
                    if row:
                        self._product_ids.append(row[0])
                        self._product_prices[row[0]] = float(row[1])

                        # Create sample_inventory record
                        cur.execute("""
                            INSERT INTO public.sample_inventory (product_id, quantity_available, quantity_reserved)
                            VALUES (%s, %s, 0)
                            ON CONFLICT (product_id) DO NOTHING
                        """, (row[0], self.gen._rng.randint(50, 500)))

            conn.commit()

        # If we didn't get IDs (already existed), fetch them
        if not self._customer_ids:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT customer_id FROM public.sample_customers LIMIT %s",
                                (self.config.seed_sample_customers,))
                    self._customer_ids = [r[0] for r in cur.fetchall()]
                    cur.execute("SELECT product_id, price FROM public.sample_products LIMIT %s",
                                (self.config.seed_sample_products,))
                    for r in cur.fetchall():
                        self._product_ids.append(r[0])
                        self._product_prices[r[0]] = float(r[1])

        print(f"✓ Seeded {len(self._customer_ids)} sample_customers, "
              f"{len(self._product_ids)} sample_products with sample_inventory")

    def run(self):
        """Run the order system load test."""
        self._running = True
        self.metrics.start_time = time.time()
        end_time = self.metrics.start_time + self.config.duration_seconds
        interval = 1.0 / self.config.sample_orders_per_sec if self.config.sample_orders_per_sec > 0 else 0

        print(f"\n{'═' * 60}")
        print(f"Order System Load Test Started")
        print(f"  Duration:      {self.config.duration_seconds}s")
        print(f"  Orders/sec:    {self.config.sample_orders_per_sec}")
        print(f"  Threads:       {self.config.threads}")
        print(f"  Customers:     {len(self._customer_ids)}")
        print(f"  Products:      {len(self._product_ids)}")
        print(f"{'═' * 60}\n")

        with ThreadPoolExecutor(max_workers=self.config.threads) as executor:
            # Start lifecycle processor in background
            lifecycle_thread = threading.Thread(
                target=self._lifecycle_processor, daemon=True, name="lifecycle"
            )
            lifecycle_thread.start()

            # Main order placement loop
            order_count = 0
            last_report = time.time()

            while time.time() < end_time and self._running:
                if interval > 0:
                    time.sleep(interval)

                executor.submit(self._place_order)
                order_count += 1

                # Progress report every 10 seconds
                if time.time() - last_report > 10:
                    elapsed = time.time() - self.metrics.start_time
                    print(f"  [{elapsed:.0f}s] Orders placed: {self.metrics.sample_orders_placed}, "
                          f"Confirmed: {self.metrics.sample_orders_confirmed}, "
                          f"Shipped: {self.metrics.sample_orders_shipped}, "
                          f"Delivered: {self.metrics.sample_orders_delivered}, "
                          f"DML ops: {self.metrics.total_dml_operations}, "
                          f"Errors: {self.metrics.errors}")
                    last_report = time.time()

        self._running = False
        self.metrics.end_time = time.time()

        print(f"\n✓ Load test complete!")
        summary = self.metrics.summary()
        print(f"  Orders placed:    {summary['sample_orders_placed']}")
        print(f"  Total DML ops:    {summary['total_dml_operations']}")
        print(f"  Effective TPS:    {summary['effective_tps']}")
        print(f"  Errors:           {summary['errors']}")

    def _place_order(self):
        """Place a single order (multi-table transaction)."""
        start = time.time()
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    # Pick random customer
                    customer_id = self.gen._rng.choice(self._customer_ids)

                    # Pick 1-5 random sample_products
                    num_items = self.gen._rng.randint(1, 5)
                    items = self.gen._rng.sample(
                        self._product_ids, min(num_items, len(self._product_ids))
                    )

                    # Calculate totals
                    subtotal = 0
                    sample_order_items = []
                    for product_id in items:
                        qty = self.gen._rng.randint(1, 3)
                        price = self._product_prices.get(product_id, 19.99)
                        line_total = round(price * qty, 2)
                        subtotal += line_total
                        sample_order_items.append((product_id, qty, price, line_total))

                    tax = round(subtotal * 0.08, 2)
                    shipping = round(self.gen._rng.uniform(0, 12.99), 2)
                    total = round(subtotal + tax + shipping, 2)

                    # 1. INSERT order
                    cur.execute("""
                        INSERT INTO public.sample_orders
                            (customer_id, order_status, subtotal, tax, shipping_cost, 
                             total, shipping_address, order_date)
                        VALUES (%s, 'pending', %s, %s, %s, %s, %s, NOW())
                        RETURNING order_id
                    """, (customer_id, subtotal, tax, shipping, total,
                          f"{self.gen._rng.randint(100,999)} Main St, Seattle, WA"))
                    order_id = cur.fetchone()[0]

                    # 2. INSERT order items
                    for product_id, qty, price, line_total in sample_order_items:
                        cur.execute("""
                            INSERT INTO public.sample_order_items
                                (order_id, product_id, quantity, unit_price, line_total)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (order_id, product_id, qty, price, line_total))

                    # 3. INSERT payment
                    cur.execute("""
                        INSERT INTO public.sample_payments
                            (order_id, payment_method, payment_status, amount, currency, transaction_ref)
                        VALUES (%s, %s, 'pending', %s, 'USD', %s)
                    """, (order_id, self.gen._rng.choice(DataGenerator.PAYMENT_METHODS),
                          total, f"TXN-{uuid.uuid4().hex[:12].upper()}"))

                    # 4. UPDATE sample_inventory (decrement)
                    for product_id, qty, _, _ in sample_order_items:
                        cur.execute("""
                            UPDATE public.sample_inventory
                            SET quantity_available = quantity_available - %s,
                                quantity_reserved = quantity_reserved + %s,
                                updated_at = NOW()
                            WHERE product_id = %s AND quantity_available >= %s
                        """, (qty, qty, product_id, qty))

                conn.commit()

            # Track metrics
            dml_count = 1 + len(sample_order_items) + 1 + len(sample_order_items)  # order + items + payment + sample_inventory updates
            with self._lock:
                self.metrics.sample_orders_placed += 1
                self.metrics.sample_payments_created += 1
                self.metrics.sample_inventory_updates += len(sample_order_items)
                self.metrics.total_dml_operations += dml_count
                self._pending_sample_orders.append({
                    "order_id": order_id,
                    "placed_at": time.time(),
                })

            self.metrics.record_latency((time.time() - start) * 1000)

        except Exception as e:
            self.metrics.record_error(str(e))

    def _lifecycle_processor(self):
        """
        Background thread that progresses sample_orders through their lifecycle:
        pending → confirmed (after ~2s) → shipped (after ~5s) → delivered (after ~10s)
        """
        while self._running:
            now = time.time()

            # Confirm pending sample_orders (after 2 seconds)
            with self._lock:
                ready_to_confirm = [o for o in self._pending_sample_orders if now - o["placed_at"] > 2]
                for o in ready_to_confirm:
                    self._pending_sample_orders.remove(o)
                    self._confirmed_sample_orders.append({**o, "confirmed_at": now})

            for order in ready_to_confirm:
                self._confirm_order(order["order_id"])

            # Ship confirmed sample_orders (after 5 seconds from confirmation)
            with self._lock:
                ready_to_ship = [o for o in self._confirmed_sample_orders if now - o["confirmed_at"] > 5]
                for o in ready_to_ship:
                    self._confirmed_sample_orders.remove(o)
                    self._shipped_sample_orders.append({**o, "shipped_at": now})

            for order in ready_to_ship:
                self._ship_order(order["order_id"])

            # Deliver shipped sample_orders (after 10 seconds from shipping)
            with self._lock:
                ready_to_deliver = [o for o in self._shipped_sample_orders if now - o["shipped_at"] > 10]
                for o in ready_to_deliver:
                    self._shipped_sample_orders.remove(o)

            for order in ready_to_deliver:
                self._deliver_order(order["order_id"])

            time.sleep(0.5)

    def _confirm_order(self, order_id: int):
        """Confirm an order: UPDATE sample_orders + UPDATE payment."""
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.sample_orders
                        SET order_status = 'confirmed', confirmed_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                    cur.execute("""
                        UPDATE public.sample_payments
                        SET payment_status = 'authorized', authorized_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                conn.commit()
            with self._lock:
                self.metrics.sample_orders_confirmed += 1
                self.metrics.total_dml_operations += 2
        except Exception as e:
            self.metrics.record_error(f"confirm: {e}")

    def _ship_order(self, order_id: int):
        """Ship an order: UPDATE sample_orders + INSERT shipment + UPDATE payment."""
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.sample_orders
                        SET order_status = 'shipped', shipped_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                    cur.execute("""
                        INSERT INTO public.sample_shipments
                            (order_id, carrier, tracking_number, shipment_status, 
                             shipped_at, estimated_delivery)
                        VALUES (%s, %s, %s, 'in_transit', NOW(), NOW() + INTERVAL '3 days')
                    """, (order_id, self.gen._rng.choice(DataGenerator.CARRIERS),
                          self.gen.tracking_number()))
                    cur.execute("""
                        UPDATE public.sample_payments
                        SET payment_status = 'captured', captured_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                conn.commit()
            with self._lock:
                self.metrics.sample_orders_shipped += 1
                self.metrics.sample_shipments_created += 1
                self.metrics.total_dml_operations += 3
        except Exception as e:
            self.metrics.record_error(f"ship: {e}")

    def _deliver_order(self, order_id: int):
        """Deliver an order: UPDATE sample_orders + UPDATE shipment + UPDATE payment + release sample_inventory."""
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.sample_orders
                        SET order_status = 'delivered', delivered_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                    cur.execute("""
                        UPDATE public.sample_shipments
                        SET shipment_status = 'delivered', delivered_at = NOW(), updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                    cur.execute("""
                        UPDATE public.sample_payments
                        SET payment_status = 'settled', updated_at = NOW()
                        WHERE order_id = %s
                    """, (order_id,))
                    # Release reserved sample_inventory
                    cur.execute("""
                        UPDATE public.sample_inventory i
                        SET quantity_reserved = GREATEST(0, quantity_reserved - oi.quantity),
                            updated_at = NOW()
                        FROM public.sample_order_items oi
                        WHERE oi.order_id = %s AND i.product_id = oi.product_id
                    """, (order_id,))
                conn.commit()
            with self._lock:
                self.metrics.sample_orders_delivered += 1
                self.metrics.total_dml_operations += 4
        except Exception as e:
            self.metrics.record_error(f"deliver: {e}")

    def validate(self) -> Dict:
        """Validate data integrity between source and target."""
        print(f"\n⏳ Waiting {self.config.warmup_seconds}s for CDC to catch up...")
        time.sleep(self.config.warmup_seconds)

        print("🔍 Validating data integrity across all tables...")
        tables = ["sample_customers", "sample_products", "sample_inventory", "sample_orders",
                  "sample_order_items", "sample_payments", "sample_shipments"]
        results = {}

        for table in tables:
            source_count = self._count(self.config.source_dsn, table)
            target_count = self._count(self.config.target_dsn, table)
            match = source_count == target_count
            results[table] = {
                "source": source_count,
                "target": target_count,
                "match": match,
                "diff": source_count - target_count,
            }
            status = "✓" if match else "✗"
            print(f"  {status} {table:20s} source={source_count:>8,}  target={target_count:>8,}  "
                  f"{'OK' if match else f'DIFF: {source_count - target_count}'}")

        return results

    def _count(self, dsn: str, table: str) -> int:
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM public.{table}")
                    return cur.fetchone()[0]
        except Exception as e:
            return -1

    def generate_report(self, integrity: Dict) -> str:
        """Save test results as JSON."""
        os.makedirs(self.config.report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        report = {
            "test": "order_system_cdc",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "config": {
                "duration": self.config.duration_seconds,
                "sample_orders_per_sec": self.config.sample_orders_per_sec,
                "threads": self.config.threads,
                "seed_sample_customers": self.config.seed_sample_customers,
                "seed_sample_products": self.config.seed_sample_products,
            },
            "metrics": self.metrics.summary(),
            "integrity": integrity,
        }

        path = os.path.join(self.config.report_dir, f"order_test_{timestamp}.json")
        with open(path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\n✓ Report saved: {path}")

        # Print final summary
        summary = self.metrics.summary()
        print(f"\n{'═' * 60}")
        print(f"FINAL RESULTS — Order System CDC Load Test")
        print(f"{'═' * 60}")
        print(f"  Duration:          {summary['duration_seconds']}s")
        print(f"  Orders placed:     {summary['sample_orders_placed']:,}")
        print(f"  Orders confirmed:  {self.metrics.sample_orders_confirmed:,}")
        print(f"  Orders shipped:    {self.metrics.sample_orders_shipped:,}")
        print(f"  Orders delivered:  {self.metrics.sample_orders_delivered:,}")
        print(f"  Total DML ops:     {summary['total_dml_operations']:,}")
        print(f"  Effective TPS:     {summary['effective_tps']}")
        print(f"  Avg latency:       {summary['latency_avg_ms']}ms")
        print(f"  P95 latency:       {summary['latency_p95_ms']}ms")
        print(f"  Errors:            {summary['errors']}")
        print(f"{'═' * 60}")

        return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CDC Load Test — Amazon-Style Order System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard test (5 min, 20 sample_orders/sec)
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN"

  # High-throughput
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \\
      --sample_orders-per-sec 100 --threads 8 --duration 600

  # Setup schema only
  python load_test_sample_orders.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" --setup-only
        """,
    )
    parser.add_argument("--source-dsn", default="", help="PostgreSQL source DSN")
    parser.add_argument("--target-dsn", default="", help="Aurora DSQL target DSN")
    parser.add_argument("--duration", type=int, default=300, help="Test duration seconds (default: 300)")
    parser.add_argument("--sample_orders-per-sec", type=int, default=20, help="Orders/second target (default: 20)")
    parser.add_argument("--threads", type=int, default=4, help="Parallel threads (default: 4)")
    parser.add_argument("--seed-sample_customers", type=int, default=500, help="Number of sample_customers to seed")
    parser.add_argument("--seed-sample_products", type=int, default=200, help="Number of sample_products to seed")
    parser.add_argument("--report-dir", default="results", help="Output directory")
    parser.add_argument("--setup-only", action="store_true", help="Only create schema and seed data")

    args = parser.parse_args()
    config = OrderTestConfig.from_args(args)

    if not config.source_dsn:
        print("Error: --source-dsn required (or set SOURCE_DSN env var)")
        sys.exit(1)

    metrics = TestMetrics()
    test = OrderSystemLoadTest(config, metrics)

    # Setup
    test.setup_schema()
    test.seed_data()

    # Create dedicated replication slot for load test (separate from production CDC)
    SAMPLE_SLOT = "sample_cdc_slot"
    SAMPLE_PUB = "sample_cdc_pub"
    print(f"\nCreating dedicated load test slot: {SAMPLE_SLOT}...")
    with psycopg2.connect(config.source_dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Create publication for sample tables only
            sample_tables = ['sample_customers', 'sample_products', 'sample_inventory',
                             'sample_orders', 'sample_order_items', 'sample_payments', 'sample_shipments']
            tables_list = ', '.join(f'public.{t}' for t in sample_tables)
            try:
                cur.execute(f"CREATE PUBLICATION {SAMPLE_PUB} FOR TABLE {tables_list}")
            except Exception as e:
                if 'already exists' in str(e):
                    pass
                else:
                    print(f"  Note: {e}")
            try:
                cur.execute(f"SELECT pg_create_logical_replication_slot('{SAMPLE_SLOT}', 'test_decoding')")
            except Exception as e:
                if 'already exists' in str(e):
                    pass
                else:
                    print(f"  Note: {e}")
    print(f"✓ Load test slot '{SAMPLE_SLOT}' ready")

    if args.setup_only:
        print("\n✓ Setup complete (--setup-only). Exiting.")
        sys.exit(0)

    # Run
    try:
        test.run()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
        test._running = False
        metrics.end_time = time.time()

    # Cleanup: Drop the load test slot (no WAL accumulation after test)
    print(f"\nCleaning up load test slot: {SAMPLE_SLOT}...")
    try:
        with psycopg2.connect(config.source_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SELECT pg_drop_replication_slot('{SAMPLE_SLOT}')")
                cur.execute(f"DROP PUBLICATION IF EXISTS {SAMPLE_PUB}")
        print(f"✓ Slot '{SAMPLE_SLOT}' dropped")
    except Exception as e:
        print(f"⚠️  Slot cleanup: {e}")

    # Validate & report
    integrity = test.validate()
    test.generate_report(integrity)


if __name__ == "__main__":
    main()
