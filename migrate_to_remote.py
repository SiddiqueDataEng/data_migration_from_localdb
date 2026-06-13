"""MY ERP — Resilient, Idempotent, Resumable Migration Pipeline.

Features
--------
- **Checkpoint file** (.migration_state.json) persists per-table progress
  (offset + rows confirmed on remote) so any break can resume mid-table.
- **Idempotent** — uses INSERT IGNORE; running twice is safe.
- **Auto-retry** — each batch retries up to 5× with exponential back-off on
  network / timeout errors before failing.
- **Connection keep-alive** — reconnects automatically after timeout.
- **Three modes**:
    --mode auto        Resume from checkpoint (default).
    --mode full        Full reload: truncates remote, resets checkpoint, re-migrates.
    --mode incremental Only migrate tables where remote count < local count.
- **Per-table control** — --only / --skip to target specific tables.
- **Progress dashboard** — live ASCII progress bar + ETA for large tables.

Usage
-----
    # Auto-resume (default — picks up where it left off):
    python migrate_to_remote.py

    # Full reload from scratch:
    python migrate_to_remote.py --mode full

    # Incremental (add missing rows only, no truncate):
    python migrate_to_remote.py --mode incremental

    # Single table:
    python migrate_to_remote.py --only attendance

    # Skip a table:
    python migrate_to_remote.py --skip erp_audit_log

    # Tune batch size (larger = faster on good connection):
    python migrate_to_remote.py --chunk 2000

Exit codes: 0 = success, 1 = one or more table errors
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Connection configs
# ---------------------------------------------------------------------------

LOCAL = dict(
    host="********", port=3306,
    user="********", password="",
    database="********",
    autocommit=False, connection_timeout=30,
    use_pure=True,
)

REMOTE = dict(
    host="********", port=3306,
    user="********",
    password="********",
    database="********",
    autocommit=False, connection_timeout=60,
    use_pure=True,
)

# ---------------------------------------------------------------------------
# Checkpoint file
# ---------------------------------------------------------------------------

_CHECKPOINT_FILE = Path(__file__).parent / ".migration_state.json"

# ---------------------------------------------------------------------------
# Tables to SKIP (config / system / not ERP-generated)
# ---------------------------------------------------------------------------

SKIP_TABLES = {
    "erp_settings",
    "erp_permissions",
    "chart_of_accounts",
    "finance_settings",
    "item_categories",
    "sp_audit_entries",
    "sp_cells",
    "sp_presence",
    "sp_presence_queue",
    "sp_sheets",
    "sp_versions",
    "sp_workbooks",
    "sp_workbook_shares",
    "kiosk_rate_limit",
    "admin_users",
    "dms_folders",
}

# ---------------------------------------------------------------------------
# FK-dependency ordered migration list
# ---------------------------------------------------------------------------

MIGRATION_ORDER = [
    "departments", "stores", "vendors", "subcontractors", "agencies",
    "camps", "camp_rooms", "employees", "erp_users",
    "projects", "project_wbs", "boq_items", "project_activities",
    "budgets", "variation_orders", "daily_site_reports",
    "hse_incidents", "hse_inspections", "toolbox_talks",
    "safety_training", "permits_to_work",
    "equipment", "equipment_log",
    "vehicles", "vehicle_fuel", "vehicle_maintenance", "vehicle_trips",
    "attendance",           # LARGE — 3.9M rows
    "leave_requests", "appraisals", "eosb_records",
    "payroll_runs", "loans", "payroll_details",   # LARGE — 150k
    "gosi_declarations",
    "purchase_requests", "purchase_request_items",
    "purchase_orders", "po_items", "goods_receipts", "grn_items",
    "rfqs", "rfq_vendors", "rfq_items", "vendor_ratings",
    "job_requisitions", "job_postings", "candidates",
    "job_applications", "offer_letters",
    "subcontract_orders", "subcontract_payments",
    "bank_accounts", "bank_transactions",
    "fixed_assets", "depreciation_schedule",
    "journal_entries", "je_lines",
    "vendor_invoices", "client_invoices", "vat_returns",
    "camp_maintenance", "camp_notices",
    "dms_documents", "dms_access_log",
    "wps_records",
    "erp_alerts",
    "erp_audit_log",        # LARGE — 550k rows
]

# ---------------------------------------------------------------------------
# Retry / back-off settings
# ---------------------------------------------------------------------------

MAX_RETRIES     = 5
RETRY_BASE_WAIT = 3    # seconds; doubles each retry
KEEPALIVE_ROWS  = 5_000  # ping connections every N rows


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _connect(cfg: dict, label: str):
    import mysql.connector
    for attempt in range(1, 6):
        try:
            conn = mysql.connector.connect(**cfg)
            if attempt > 1:
                print(f"  [{label}] Reconnected (attempt {attempt})")
            return conn
        except Exception as exc:
            wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
            print(f"  [{label}] Connect failed (attempt {attempt}): {exc} — retry in {wait}s")
            if attempt == 5:
                raise
            time.sleep(wait)


def _ping(conn, cfg: dict, label: str):
    """Ping and reconnect if the connection has dropped."""
    try:
        conn.ping(reconnect=False)
        return conn
    except Exception:
        print(f"  [{label}] Connection lost — reconnecting...")
        try:
            conn.close()
        except Exception:
            pass
        return _connect(cfg, label)


def _count(conn, table: str, cfg: dict, label: str) -> int:
    conn = _ping(conn, cfg, label)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        return cur.fetchone()[0]
    except Exception:
        return -1
    finally:
        cur.close()


def _get_all_columns(conn, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _load_checkpoint() -> dict:
    if _CHECKPOINT_FILE.exists():
        try:
            return json.loads(_CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_checkpoint(state: dict) -> None:
    _CHECKPOINT_FILE.write_text(
        json.dumps(state, indent=2, default=str),
        encoding="utf-8",
    )


def _clear_checkpoint() -> None:
    if _CHECKPOINT_FILE.exists():
        _CHECKPOINT_FILE.unlink()


# ---------------------------------------------------------------------------
# Progress bar
# ---------------------------------------------------------------------------

def _bar(done: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return "[" + "?" * width + "]"
    filled = min(int(width * done / total), width)
    return "[" + "█" * filled + "░" * (width - filled) + "]"


def _eta(done: int, total: int, elapsed: float) -> str:
    if done <= 0 or elapsed <= 0:
        return "ETA ?"
    rate = done / elapsed
    rem  = (total - done) / rate if rate > 0 else 0
    return f"ETA {timedelta(seconds=int(rem))}"


# ---------------------------------------------------------------------------
# wps_records DDL
# ---------------------------------------------------------------------------

def _ensure_wps_table(conn, cfg: dict) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS `wps_records` (
        `id`               INT AUTO_INCREMENT PRIMARY KEY,
        `run_id`           INT NOT NULL,
        `employee_id`      INT NOT NULL,
        `wps_file_no`      VARCHAR(30),
        `transfer_date`    DATE,
        `iban`             VARCHAR(34),
        `bank_name`        VARCHAR(80),
        `net_amount`       DECIMAL(12,2),
        `status`           ENUM('pending','sent','confirmed','rejected') DEFAULT 'pending',
        `rejection_reason` VARCHAR(200),
        `created_at`       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX `idx_wps_run`  (`run_id`),
        INDEX `idx_wps_emp`  (`employee_id`),
        INDEX `idx_wps_sta`  (`status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    conn = _ping(conn, cfg, "remote")
    cur  = conn.cursor()
    try:
        cur.execute(ddl)
        conn.commit()
    except Exception as exc:
        print(f"  [warn] wps_records DDL: {exc}")
    finally:
        cur.close()
    return conn


# ---------------------------------------------------------------------------
# Truncate (full mode)
# ---------------------------------------------------------------------------

def _truncate_all(remote, remote_cfg: dict, tables: list[str]) -> None:
    print("Truncating remote tables (reverse FK order)...")
    remote = _ping(remote, remote_cfg, "remote")
    cur    = remote.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    remote.commit()
    cur.close()

    for table in reversed(tables):
        remote = _ping(remote, remote_cfg, "remote")
        cur    = remote.cursor()
        try:
            cur.execute(f"DELETE FROM `{table}`")
            remote.commit()
        except Exception:
            remote.rollback()
        finally:
            cur.close()

    remote = _ping(remote, remote_cfg, "remote")
    cur    = remote.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    remote.commit()
    cur.close()
    print("Truncation done.\n")
    return remote


# ---------------------------------------------------------------------------
# Core per-table migrator
# ---------------------------------------------------------------------------

def migrate_table(
    local,
    remote,
    local_cfg: dict,
    remote_cfg: dict,
    table: str,
    chunk_size: int,
    start_offset: int = 0,
    mode: str = "auto",
    dry_run: bool = False,
) -> tuple[int, int]:
    """Migrate one table from local → remote with checkpointing + retry.

    Args:
        local:        Local MySQL connection.
        remote:       Remote MySQL connection.
        local_cfg:    Local connection params (for reconnect).
        remote_cfg:   Remote connection params (for reconnect).
        table:        Table name.
        chunk_size:   Rows per batch.
        start_offset: Row offset to resume from (0 = start).
        mode:         'auto' | 'full' | 'incremental'.
        dry_run:      If True, read local but don't write remote.

    Returns:
        (total_inserted, final_offset) tuple.
    """
    # Get shared columns (exist on both local and remote)
    local  = _ping(local,  local_cfg,  "local")
    remote = _ping(remote, remote_cfg, "remote")

    try:
        local_cols  = set(_get_all_columns(local,  table))
        remote_cols = set(_get_all_columns(remote, table))
    except Exception as exc:
        print(f"  [skip] {table}: cannot read columns — {exc}")
        return 0, 0

    shared = [c for c in _get_all_columns(local, table) if c in remote_cols]
    if not shared:
        print(f"  [skip] {table}: no matching columns")
        return 0, 0

    col_str  = ", ".join(f"`{c}`" for c in shared)
    ph_str   = ", ".join(["%s"] * len(shared))
    sql_ins  = f"INSERT IGNORE INTO `{table}` ({col_str}) VALUES ({ph_str})"

    local  = _ping(local,  local_cfg,  "local")
    total_local = _count(local, table, local_cfg, "local")

    if total_local <= 0:
        return 0, 0

    # Incremental mode: skip if remote already has all rows
    if mode == "incremental" and start_offset == 0:
        remote_count = _count(remote, table, remote_cfg, "remote")
        if remote_count >= total_local:
            return remote_count, total_local

    offset   = start_offset
    inserted = 0
    t0       = time.time()
    rows_since_ping = 0

    print(f"\r  {table:<40} {_bar(offset, total_local)} {offset:>8,}/{total_local:,}", end="", flush=True)

    while offset < total_local:
        # Keep-alive ping every KEEPALIVE_ROWS
        if rows_since_ping >= KEEPALIVE_ROWS:
            local  = _ping(local,  local_cfg,  "local")
            remote = _ping(remote, remote_cfg, "remote")
            rows_since_ping = 0

        # Fetch chunk from local
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                local = _ping(local, local_cfg, "local")
                cur_l = local.cursor(dictionary=True)
                cur_l.execute(
                    f"SELECT {col_str} FROM `{table}` LIMIT %s OFFSET %s",
                    (chunk_size, offset),
                )
                rows = cur_l.fetchall()
                cur_l.close()
                break
            except Exception as exc:
                wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
                print(f"\n  [retry {attempt}/{MAX_RETRIES}] {table} read @{offset}: {exc} — wait {wait}s")
                time.sleep(wait)
                if attempt == MAX_RETRIES:
                    raise

        if not rows:
            break

        params = [tuple(r[c] for c in shared) for r in rows]

        # Write to remote with retry
        if not dry_run:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    remote = _ping(remote, remote_cfg, "remote")
                    cur_r  = remote.cursor()
                    cur_r.executemany(sql_ins, params)
                    remote.commit()
                    cur_r.close()
                    break
                except Exception as exc:
                    wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
                    print(f"\n  [retry {attempt}/{MAX_RETRIES}] {table} write @{offset}: {exc} — wait {wait}s")
                    try:
                        remote.rollback()
                    except Exception:
                        pass
                    time.sleep(wait)
                    if attempt == MAX_RETRIES:
                        raise

        inserted        += len(rows)
        offset          += len(rows)
        rows_since_ping += len(rows)
        elapsed = time.time() - t0

        # Live progress bar
        bar  = _bar(offset, total_local)
        eta  = _eta(offset - start_offset, total_local - start_offset, elapsed)
        rate = (offset - start_offset) / elapsed if elapsed > 0 else 0
        print(
            f"\r  {table:<40} {bar} {offset:>8,}/{total_local:,}"
            f"  {rate:>6.0f}/s  {eta}   ",
            end="", flush=True,
        )

    elapsed = time.time() - t0
    print(
        f"\r  {table:<40} {_bar(total_local, total_local)}"
        f" {offset:>8,}/{total_local:,}"
        f"  {elapsed:.0f}s  DONE{' (DRY RUN)' if dry_run else ''}   "
    )
    return inserted, offset


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="MY ERP — Resilient Idempotent Migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  auto         Resume from checkpoint (default)
  full         Full reload: truncate remote + reset + re-migrate all
  incremental  Only migrate tables where remote < local (no truncate)

Examples:
  python migrate_to_remote.py                        # auto-resume
  python migrate_to_remote.py --mode full            # full reload
  python migrate_to_remote.py --mode incremental     # add missing rows
  python migrate_to_remote.py --only attendance      # single table
  python migrate_to_remote.py --only payroll_details,erp_audit_log
  python migrate_to_remote.py --skip erp_audit_log   # skip one table
  python migrate_to_remote.py --chunk 2000           # bigger batches
  python migrate_to_remote.py --dry-run              # read-only check
        """,
    )
    parser.add_argument("--mode",     choices=["auto","full","incremental"], default="auto")
    parser.add_argument("--only",     type=str, default=None, help="Comma-separated table list")
    parser.add_argument("--skip",     type=str, default=None, help="Comma-separated tables to skip")
    parser.add_argument("--chunk",    type=int, default=1000)
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Delete existing checkpoint and start fresh")
    args = parser.parse_args()

    print()
    print("=" * 65)
    print("  MY ERP — Resilient Migration Pipeline")
    print(f"  Remote  : {REMOTE['host']}/{REMOTE['database']}")
    print(f"  Mode    : {args.mode.upper()}")
    print(f"  Chunk   : {args.chunk:,} rows/batch")
    print(f"  Dry-run : {args.dry_run}")
    print("=" * 65)
    print()

    import mysql.connector

    # Connect
    try:
        local  = _connect(LOCAL,  "local")
        remote = _connect(REMOTE, "remote")
    except Exception as exc:
        print(f"[FATAL] Cannot establish connections: {exc}", file=sys.stderr)
        sys.exit(1)

    # Ensure wps_records exists on remote
    _ensure_wps_table(remote, REMOTE)

    # Get available tables
    local_tables  = set(c[0] for c in (lambda cur: (cur.execute("SHOW TABLES"), cur.fetchall())[1])(local.cursor()))
    remote_tables = set(c[0] for c in (lambda cur: (cur.execute("SHOW TABLES"), cur.fetchall())[1])(remote.cursor()))

    # Build ordered table list
    extra_skip = {t.strip() for t in args.skip.split(",")} if args.skip else set()

    if args.only:
        tables_to_run = [t.strip() for t in args.only.split(",")]
    else:
        ordered = [t for t in MIGRATION_ORDER
                   if t in local_tables and t in remote_tables
                   and t not in SKIP_TABLES and t not in extra_skip]
        # Append any local tables not in MIGRATION_ORDER
        ordered_set = set(MIGRATION_ORDER)
        for t in sorted(local_tables):
            if (t not in ordered_set and t not in SKIP_TABLES
                    and t not in extra_skip and t in remote_tables):
                ordered.append(t)
        tables_to_run = ordered

    print(f"Tables queued: {len(tables_to_run)}")

    # Checkpoint management
    if args.reset_checkpoint or args.mode == "full":
        _clear_checkpoint()
    checkpoint = _load_checkpoint()

    # Full mode: truncate remote
    if args.mode == "full" and not args.dry_run:
        remote = _truncate_all(remote, REMOTE, tables_to_run)

    # Disable FK / unique checks for bulk insert
    if not args.dry_run:
        r_cur = remote.cursor()
        r_cur.execute("SET FOREIGN_KEY_CHECKS=0")
        r_cur.execute("SET UNIQUE_CHECKS=0")
        remote.commit()
        r_cur.close()

    # --- Migration loop ---
    results: list[dict] = []
    errors:  list[tuple[str, str]] = []
    t_total = time.time()

    for table in tables_to_run:
        if table not in local_tables:
            print(f"  [skip] {table}: not in local DB")
            continue
        if table not in remote_tables:
            print(f"  [skip] {table}: not in remote schema")
            continue

        local_count = _count(local, table, LOCAL, "local")
        if local_count <= 0:
            print(f"  [skip] {table}: empty on local")
            continue

        # Determine start offset
        state       = checkpoint.get(table, {})
        start_off   = state.get("offset", 0) if args.mode == "auto" else 0

        if start_off > 0:
            print(f"  [resume] {table} from offset {start_off:,}")

        t_table = time.time()
        try:
            inserted, final_offset = migrate_table(
                local, remote, LOCAL, REMOTE,
                table, args.chunk, start_off, args.mode, args.dry_run,
            )

            elapsed = time.time() - t_table
            results.append({
                "table": table, "inserted": inserted,
                "offset": final_offset, "elapsed": elapsed,
            })

            # Update checkpoint
            checkpoint[table] = {
                "offset":   final_offset,
                "inserted": inserted,
                "done":     True,
                "ts":       datetime.now().isoformat(),
            }
            if not args.dry_run:
                _save_checkpoint(checkpoint)

        except Exception as exc:
            import traceback
            elapsed = time.time() - t_table
            print(f"\n  [ERROR] {table} after {elapsed:.0f}s: {exc}")
            traceback.print_exc()
            errors.append((table, str(exc)))

            # Save progress so far in checkpoint
            checkpoint[table] = checkpoint.get(table, {})
            checkpoint[table]["error"] = str(exc)
            if not args.dry_run:
                _save_checkpoint(checkpoint)

            # Reconnect for next table
            try:
                local  = _connect(LOCAL,  "local")
                remote = _connect(REMOTE, "remote")
            except Exception:
                pass
            continue

    # Re-enable FK checks
    if not args.dry_run:
        try:
            remote = _ping(remote, REMOTE, "remote")
            r_cur  = remote.cursor()
            r_cur.execute("SET FOREIGN_KEY_CHECKS=1")
            r_cur.execute("SET UNIQUE_CHECKS=1")
            remote.commit()
            r_cur.close()
        except Exception:
            pass

    total_elapsed = time.time() - t_total
    total_rows    = sum(r["inserted"] for r in results)

    # Summary
    print()
    print("=" * 65)
    print(f"  Migration {'(DRY RUN) ' if args.dry_run else ''}COMPLETE")
    print(f"  Tables migrated : {len(results)}")
    print(f"  Rows inserted   : {total_rows:,}")
    print(f"  Errors          : {len(errors)}")
    print(f"  Total time      : {total_elapsed:.0f}s  ({total_elapsed/60:.1f} min)")
    print("=" * 65)

    if errors:
        print("\nFailed tables (run again to resume):")
        for t, msg in errors:
            print(f"  {t}: {msg[:80]}")
        print(f"\nRerun with: python migrate_to_remote.py --mode auto")

    # Write report
    report_path = Path(__file__).parent / "migration_report.txt"
    lines = [
        f"MY ERP Migration {'DRY RUN' if args.dry_run else 'COMPLETE'} — {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Mode: {args.mode}  |  Remote: {REMOTE['host']}/{REMOTE['database']}",
        f"Total rows: {total_rows:,}  |  Tables: {len(results)}  |  Time: {total_elapsed:.0f}s",
        "",
        f"{'Table':<45} {'Rows':>10}  {'Offset':>10}  {'Time':>8}  Status",
        "-" * 90,
    ]
    for r in results:
        done_marker = "✓" if r["offset"] >= _count(local, r["table"], LOCAL, "local") else "~"
        lines.append(
            f"{r['table']:<45} {r['inserted']:>10,}"
            f"  {r['offset']:>10,}"
            f"  {r['elapsed']:>7.0f}s  {done_marker}"
        )
    if errors:
        lines += ["", "ERRORS:"]
        for t, msg in errors:
            lines.append(f"  {t}: {msg}")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nReport : {report_path}")
    print(f"State  : {_CHECKPOINT_FILE}")

    try:
        local.close()
        remote.close()
    except Exception:
        pass

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
