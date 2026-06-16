"""
Source Database Setup Script
============================
Run this ONCE on your source PostgreSQL 14+ to configure logical replication.

Supports two built-in plugins (NO external extensions needed):
  - test_decoding (built-in, always available) ← DEFAULT
  - pgoutput (native PG 10+ protocol, requires a PUBLICATION)

Prerequisites:
  - PostgreSQL 14+ 
  - wal_level = logical in postgresql.conf
  - max_replication_slots >= 1 (default is 10)
  - max_wal_senders >= 1

Usage:
  export SOURCE_DSN="host=mydb.example.com dbname=myapp user=repl_user password=xxx"
  python setup_source.py --dsn "$SOURCE_DSN" [--plugin test_decoding|pgoutput] [--tables public.orders,public.customers]
"""


import argparse
import sys

import psycopg2
from typing import List, Optional



def check_prerequisites(dsn: str) -> List[str]:
    """Check that the source database is configured for logical replication."""
    issues = []

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Check wal_level
            cur.execute("SHOW wal_level")
            wal_level = cur.fetchone()[0]
            if wal_level != "logical":
                issues.append(
                    f"wal_level is '{wal_level}', must be 'logical'. "
                    f"Set wal_level = logical in postgresql.conf and restart."
                )

            # Check max_replication_slots
            cur.execute("SHOW max_replication_slots")
            max_slots = int(cur.fetchone()[0])
            if max_slots < 1:
                issues.append(f"max_replication_slots is {max_slots}, must be >= 1.")

            # Check max_wal_senders
            cur.execute("SHOW max_wal_senders")
            max_senders = int(cur.fetchone()[0])
            if max_senders < 1:
                issues.append(f"max_wal_senders is {max_senders}, must be >= 1.")

            # Check PostgreSQL version
            cur.execute("SHOW server_version_num")
            version = int(cur.fetchone()[0])
            if version < 140000:
                issues.append(
                    f"PostgreSQL version {version} is below 14. "
                    f"This tool requires PostgreSQL 14+."
                )

    return issues


def setup_replication(
    dsn: str,
    plugin: str = "test_decoding",
    slot_name: str = "dsql_cdc_slot",
    publication_name: str = "dsql_cdc_pub",
    tables: Optional[List[str]] = None,
):
    """Create replication slot and (if pgoutput) publication."""

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Create replication slot
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name,)
            )
            if cur.fetchone():
                print(f"✓ Replication slot '{slot_name}' already exists")
                # Verify plugin matches
                cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,)
                )
                existing_plugin = cur.fetchone()[0]
                if existing_plugin != plugin:
                    print(f"  ⚠️  WARNING: Existing slot uses '{existing_plugin}', "
                          f"but you requested '{plugin}'.")
                    print(f"  To recreate: SELECT pg_drop_replication_slot('{slot_name}');")
            else:
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, %s)",
                    (slot_name, plugin)
                )
                print(f"✓ Created replication slot: {slot_name} (plugin: {plugin})")

            # Create publication (required for pgoutput, optional for test_decoding)
            if plugin == "pgoutput":
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (publication_name,)
                )
                if cur.fetchone():
                    print(f"✓ Publication '{publication_name}' already exists")
                else:
                    if tables:
                        tables_sql = ", ".join(tables)
                        cur.execute(
                            f"CREATE PUBLICATION {publication_name} FOR TABLE {tables_sql}"
                        )
                        print(f"✓ Created publication: {publication_name} (tables: {tables_sql})")
                    else:
                        cur.execute(
                            f"CREATE PUBLICATION {publication_name} FOR ALL TABLES"
                        )
                        print(f"✓ Created publication: {publication_name} (ALL TABLES)")
            else:
                print(f"ℹ  test_decoding does not require a PUBLICATION (table filtering "
                      f"handled in the CDC consumer)")

            # Set REPLICA IDENTITY for proper UPDATE/DELETE handling
            if tables:
                print("\nSetting REPLICA IDENTITY FULL for specified tables...")
                for table in tables:
                    try:
                        cur.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
                        print(f"  ✓ {table} → REPLICA IDENTITY FULL")
                    except Exception as e:
                        print(f"  ⚠️  {table}: {e}")
            else:
                print("\nℹ  Consider setting REPLICA IDENTITY FULL on tables that have "
                      "UPDATE/DELETE operations:")
                print("   ALTER TABLE mytable REPLICA IDENTITY FULL;")
                print("   (This ensures old column values are included in UPDATE/DELETE events)")

            # Show current slot status
            cur.execute("""
                SELECT slot_name, plugin, restart_lsn, confirmed_flush_lsn
                FROM pg_replication_slots
                WHERE slot_name = %s
            """, (slot_name,))
            row = cur.fetchone()
            if row:
                print(f"\n{'─' * 40}")
                print(f"Slot Status:")
                print(f"  Name:            {row[0]}")
                print(f"  Plugin:          {row[1]}")
                print(f"  Restart LSN:     {row[2]}")
                print(f"  Confirmed Flush: {row[3]}")


def create_replication_user(dsn: str, username: str = "cdc_replicator"):
    """Create a dedicated replication user (optional but recommended)."""
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (username,)
            )
            if cur.fetchone():
                print(f"✓ User '{username}' already exists")
            else:
                cur.execute(f"""
                    CREATE ROLE {username} WITH LOGIN REPLICATION;
                    COMMENT ON ROLE {username} IS 'CDC replication user for DSQL sync';
                """)
                print(f"✓ Created replication user: {username}")
                print(f"  ⚠️  Set a password: ALTER ROLE {username} PASSWORD 'your_password';")
                print(f"  ⚠️  Grant SELECT: GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username};")


def test_slot(dsn: str, slot_name: str = "dsql_cdc_slot", plugin: str = "test_decoding"):
    """Test that the replication slot is working by peeking at changes."""
    print(f"\n{'─' * 40}")
    print("Testing replication slot (peeking at current changes)...")

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lsn, xid, data
                FROM pg_logical_slot_peek_changes(%s, NULL, 5)
            """, (slot_name,))
            rows = cur.fetchall()

            if rows:
                print(f"✓ Found {len(rows)} pending change(s):")
                for lsn, xid, data in rows[:3]:
                    print(f"  LSN={lsn} XID={xid}: {data[:100]}...")
            else:
                print("✓ Slot is active, no pending changes (make a write to test)")
                print(f"  Try: INSERT INTO some_table VALUES (...)  then re-run this script")


def main():
    parser = argparse.ArgumentParser(
        description="Setup PostgreSQL source for CDC to Aurora DSQL"
    )
    parser.add_argument("--dsn", required=True, help="PostgreSQL connection string")
    parser.add_argument(
        "--plugin", default="test_decoding",
        choices=["test_decoding", "pgoutput"],
        help="Logical decoding plugin (default: test_decoding, built-in)"
    )
    parser.add_argument("--slot-name", default="dsql_cdc_slot", help="Replication slot name")
    parser.add_argument("--publication", default="dsql_cdc_pub", help="Publication name (pgoutput only)")
    parser.add_argument("--tables", default="", help="Comma-separated list of tables (default: all)")
    parser.add_argument("--create-user", action="store_true", help="Create a dedicated replication user")
    parser.add_argument("--check-only", action="store_true", help="Only check prerequisites")
    parser.add_argument("--test", action="store_true", help="Test the slot after creation")

    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None

    print("=" * 60)
    print("PostgreSQL → Aurora DSQL CDC Setup")
    print(f"  Plugin: {args.plugin} (built-in, no extensions required)")
    print("=" * 60)
    print()

    # Check prerequisites
    print("Checking prerequisites...")
    issues = check_prerequisites(args.dsn)
    if issues:
        print("\n❌ Prerequisites NOT met:")
        for issue in issues:
            print(f"   • {issue}")
        sys.exit(1)
    else:
        print("✓ All prerequisites met")
        print("  • wal_level = logical")
        print("  • max_replication_slots OK")
        print("  • max_wal_senders OK")
        print("  • PostgreSQL 14+")
        print()

    if args.check_only:
        sys.exit(0)

    # Optional: create replication user
    if args.create_user:
        print("Creating replication user...")
        create_replication_user(args.dsn)
        print()

    # Setup replication
    print("Setting up replication...")
    setup_replication(args.dsn, args.plugin, args.slot_name, args.publication, tables)

    # Test slot
    if args.test:
        test_slot(args.dsn, args.slot_name, args.plugin)

    print()
    print("=" * 60)
    print("✅ Setup complete!")
    print()
    print("Next steps:")
    print(f"  1. Deploy the CDC service:")
    print(f"     export DECODING_PLUGIN={args.plugin}")
    print(f"     export SLOT_NAME={args.slot_name}")
    if args.plugin == "pgoutput":
        print(f"     export PUBLICATION_NAME={args.publication}")
    print(f"     python cdc_service.py")
    print()
    print(f"  2. Or deploy to ECS Fargate:")
    print(f"     ./deploy.sh <account-id> <region> <vpc-id> <subnets> <sg>")
    print("=" * 60)


if __name__ == "__main__":
    main()
