# MY ERP — ETL Migration Pipeline

## `migrate_to_remote.py`

A production-grade, resilient ETL pipeline that migrates the full MY ERP
dataset (~4.8 million rows, 98 tables) from a local MySQL database to a remote
MySQL server. Designed for WAN conditions, unstable connections, and long-running
bulk transfers.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [ETL Pipeline Design](#etl-pipeline-design)
3. [Key Features](#key-features)
4. [Prerequisites](#prerequisites)
5. [Configuration](#configuration)
6. [Running Modes](#running-modes)
7. [CLI Reference](#cli-reference)
8. [Checkpoint System](#checkpoint-system)
9. [Resilience Mechanisms](#resilience-mechanisms)
10. [Migration Order](#migration-order)
11. [Skipped Tables](#skipped-tables)
12. [Performance Tuning](#performance-tuning)
13. [Monitoring Progress](#monitoring-progress)
14. [Output Files](#output-files)
15. [Troubleshooting](#troubleshooting)
16. [Data Volumes](#data-volumes)
17. [VERSIONS](VERSIONS_COMPARISON.md)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LOCAL MYSQL (SOURCE)                              │
│   host: localhost:3306                                               │
│   db:   ------                                         │
│   rows: ~4,865,925 across 98 tables                                  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         │  ETL Pipeline
                         │  ┌─────────────────────────────────────┐
                         │  │  1. EXTRACT                          │
                         │  │     SELECT ... LIMIT chunk OFFSET n  │
                         │  │     Paginated read, 5k rows/batch    │
                         │  │                                      │
                         │  │  2. TRANSFORM                        │
                         │  │     Column intersection (shared cols) │
                         │  │     Python type passthrough           │
                         │  │     NULL / date object handling       │
                         │  │                                      │
                         │  │  3. LOAD                             │
                         │  │     INSERT IGNORE ... executemany()  │
                         │  │     Commit per batch                  │
                         │  │     Checkpoint saved per table        │
                         │  └─────────────────────────────────────┘
                         │
                         ▼  WAN (Germany: ********)
┌─────────────────────────────────────────────────────────────────────┐
│                    REMOTE MYSQL (TARGET)                             │
│   host: -------                                          │
│   db:   -------                                         │
│   schema: 112 tables (full MY ERP schema)                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ETL Pipeline Design

### Extract Phase

Data is read from the local MySQL source using **paginated `SELECT`** queries:

```sql
SELECT col1, col2, ..., colN
FROM `table_name`
LIMIT <chunk_size> OFFSET <current_offset>
```

- Each table is read sequentially in `chunk_size` row batches (default: 1,000; recommended: 5,000)
- Offset advances by `chunk_size` after each successful batch
- Column list is built at runtime by intersecting `SHOW COLUMNS` from both source and target — this ensures schema differences between local and remote never cause failures
- Cursor uses `dictionary=True` so rows are Python dicts, making column access by name safe and explicit

### Transform Phase

The transform layer is intentionally lightweight — this is a **schema-homogeneous migration** (same schema on both ends). Transformations applied:

| Concern | Handling |
|---|---|
| Column mismatch | Only columns present on **both** source and target are selected and inserted. Extra local columns are silently dropped; extra remote columns keep their defaults |
| Python `date`/`datetime` objects | Passed directly to `mysql-connector-python` — no `.isoformat()` conversion needed |
| `None` / `NULL` | Passed as `None` — connector maps to SQL `NULL` |
| Auto-increment `id` | Included in `shared` columns and explicitly inserted so FK references remain valid |
| Encoding | `use_pure=True` ensures the pure-Python connector handles charset correctly without C extension issues |

### Load Phase

Data is written to remote using **`INSERT IGNORE`**:

```sql
INSERT IGNORE INTO `table_name` (col1, col2, ..., colN)
VALUES (%s, %s, ..., %s)
```

`INSERT IGNORE` silently skips rows where the primary key already exists — this is what makes the pipeline **idempotent**: running it twice (or resuming after a break) never creates duplicate rows.

Each batch is committed individually with `conn.commit()` so progress is durable even if the process is killed mid-table.

---

## Key Features

| Feature | Detail |
|---|---|
| **Idempotent** | `INSERT IGNORE` — safe to run multiple times |
| **Resumable** | Per-table offset stored in `.migration_state.json` after every batch |
| **Auto-retry** | Up to 5 retries with exponential back-off (3s → 6s → 12s → 24s → 48s) |
| **Keep-alive** | Connections pinged every 5,000 rows; silently reconnected if dropped |
| **Schema-safe** | Only columns present on both source and target are transferred |
| **Three modes** | `auto` (resume), `full` (reload), `incremental` (add-only) |
| **Per-table targeting** | `--only` / `--skip` flags for surgical re-runs |
| **Live progress** | ASCII progress bar, rows/sec, and ETA per table |
| **FK-safe bulk load** | `SET FOREIGN_KEY_CHECKS=0` during migration, restored on completion |
| **DDL auto-create** | `wps_records` table created on remote if missing |
| **Dry-run mode** | Read local, print plan, write nothing to remote |
| **Migration report** | `migration_report.txt` written on completion |

---

## Prerequisites

```bash
pip install mysql-connector-python==8.3.0
```

Python 3.10+ required.

Both databases must be accessible:

```bash
# Test local
mysql -u ********** -h ******* ********** -e "SELECT COUNT(*) FROM employees"

# Test remote
mysql -u ************** -p -h ************ *********** \
      -e "SELECT COUNT(*) FROM employees"
```

---

## Configuration

Connection parameters are defined as constants at the top of the script:

```python
LOCAL = dict(
    host="localhost", port=3306,
    user="",      password="",
    database="",
    autocommit=False, connection_timeout=30,
    use_pure=True,
)

REMOTE = dict(
    host="", port=3306,
    user="",
    password="",
    database="",
    autocommit=False, connection_timeout=60,
    use_pure=True,
)
```

Edit these constants directly to change source/target databases. No config file needed.

---

## Running Modes

### `auto` (default) — Resume from checkpoint

The recommended mode for all runs including the first run. Reads `.migration_state.json`
to find the last saved offset per table and resumes from there. Tables with no checkpoint
entry start from offset 0. Tables marked `done: true` are skipped instantly.

```bash
python migrate_to_remote.py
python migrate_to_remote.py --chunk 5000    # faster on good connection
```

**Use this after any interruption** — power cut, network drop, Ctrl+C. The pipeline
picks up exactly where it left off, never re-inserting rows it already migrated.

### `full` — Complete reload

Truncates all target tables (in reverse FK order to avoid constraint errors), clears the
checkpoint file, then migrates everything from scratch.

```bash
python migrate_to_remote.py --mode full
python migrate_to_remote.py --mode full --chunk 5000
```

> ⚠️ **Destructive** — all existing data on the remote is deleted before migration.
> Use when the remote has corrupt/inconsistent data and a clean reload is needed.

### `incremental` — Add missing rows only

Compares row counts between local and remote per table. Skips tables where
`remote_count >= local_count`. For tables with a shortfall, inserts only the
missing rows without truncating.

```bash
python migrate_to_remote.py --mode incremental
```

Best used for:
- Topping up a partially populated remote after new data is generated locally
- Regular sync runs to keep remote fresh

---

## CLI Reference

```
usage: migrate_to_remote.py [-h]
                             [--mode {auto,full,incremental}]
                             [--only ONLY]
                             [--skip SKIP]
                             [--chunk CHUNK]
                             [--dry-run]
                             [--reset-checkpoint]
```

| Argument | Default | Description |
|---|---|---|
| `--mode` | `auto` | Migration mode: `auto` / `full` / `incremental` |
| `--only` | — | Comma-separated list of tables to migrate (all others skipped) |
| `--skip` | — | Comma-separated list of tables to skip |
| `--chunk` | `1000` | Rows per INSERT batch. Increase to 5000–10000 on fast links |
| `--dry-run` | off | Read local, print plan, **write nothing** to remote |
| `--reset-checkpoint` | off | Delete `.migration_state.json` and start offsets from 0 |

### Examples

```bash
# Default — auto-resume everything
python migrate_to_remote.py

# Recommended for WAN migration (fewer round-trips)
python migrate_to_remote.py --chunk 5000

# Full reload from scratch
python migrate_to_remote.py --mode full --chunk 5000

# Preview only — no writes
python migrate_to_remote.py --dry-run

# Migrate a single table
python migrate_to_remote.py --only attendance

# Migrate several tables
python migrate_to_remote.py --only payroll_details,erp_audit_log,wps_records

# Skip the largest table
python migrate_to_remote.py --skip erp_audit_log

# Incremental top-up
python migrate_to_remote.py --mode incremental --chunk 5000

# Reset checkpoint and restart without truncating remote
python migrate_to_remote.py --reset-checkpoint
```

---

## Checkpoint System

The checkpoint file `.migration_state.json` (in the same directory as the script)
records the migration state for every table:

```json
{
  "employees": {
    "offset":   1673,
    "inserted": 1673,
    "done":     true,
    "ts":       "2026-06-12T14:23:11"
  },
  "attendance": {
    "offset":   2608000,
    "inserted": 2608000,
    "done":     false,
    "ts":       "2026-06-12T16:45:03"
  }
}
```

| Field | Meaning |
|---|---|
| `offset` | Number of rows successfully transferred (= resume point) |
| `inserted` | Rows inserted in this run |
| `done` | `true` = table fully migrated; `false` = partially done |
| `ts` | Timestamp of last checkpoint write |
| `error` | Error message (only present if table failed) |

### Checkpoint lifecycle

```
First run   → no file → all tables start at offset 0
Mid-run     → saved after every batch → offset updated continuously
Interrupted → file contains last good offset per table
Resume      → reads file → skips done tables → resumes partials from offset
Full mode   → file deleted → fresh start
```

### Manual checkpoint sync

If the remote DB was modified outside the pipeline (e.g., direct SQL), sync the
checkpoint to match:

```bash
python -c "
import mysql.connector, json
from pathlib import Path
from datetime import datetime

remote = mysql.connector.connect(host='********', ...)
cur    = remote.cursor()
state  = {}

for table in ['attendance', 'payroll_details', ...]:
    cur.execute('SELECT COUNT(*) FROM ' + table)
    count = cur.fetchone()[0]
    state[table] = {'offset': count, 'inserted': count,
                    'done': True, 'ts': datetime.now().isoformat()}

Path('.migration_state.json').write_text(json.dumps(state, indent=2))
"
```

---

## Resilience Mechanisms

### 1. Exponential back-off retry

Every batch read (local) and write (remote) is wrapped in a retry loop:

```
Attempt 1 — immediate
Attempt 2 — wait  3s
Attempt 3 — wait  6s
Attempt 4 — wait 12s
Attempt 5 — wait 24s  ← final attempt, raises if still failing
```

Catches: connection timeouts, packet loss, transient MySQL errors, server restarts.

### 2. Connection keep-alive

Every 5,000 rows the pipeline calls `conn.ping()` on both connections. If the ping
fails, the connection is silently closed and a fresh connection is opened before
continuing. No manual intervention required for connections that idle-timeout.

### 3. Per-table error isolation

If a table fails after all retries, the error is logged to the checkpoint file and
the pipeline continues to the next table. At the end, a summary shows which tables
need re-running. Simply run the pipeline again — it resumes the failed table from
its last saved offset.

### 4. FK constraint bypass

`SET FOREIGN_KEY_CHECKS=0` is set at the start and restored at the end. This allows
any table to be migrated in any order without FK violation errors, and allows the
pipeline to continue if a parent table hasn't been migrated yet.

### 5. INSERT IGNORE semantics

If the process is killed mid-batch, the partial batch is lost (not committed). On
resume, the offset is the **last committed** position, so those rows are re-read
and re-inserted. `INSERT IGNORE` ensures no duplicate key errors — rows that already
exist are silently skipped.

### Failure scenario matrix

| Failure | What happens | Recovery |
|---|---|---|
| Network drop mid-batch | Batch not committed; retry loop kicks in (×5) | Automatic |
| Network drop between batches | Checkpoint already saved; table resumes from correct offset | Run again |
| Power cut | Checkpoint file has last committed offset | Run again |
| Remote MySQL restart | `_ping()` detects dead connection; reconnects | Automatic |
| Local MySQL restart | `_ping()` detects dead connection; reconnects | Automatic |
| Table schema mismatch | Column intersection used; extra cols silently excluded | Automatic |
| Table missing on remote | Table skipped with `[skip]` message | Automatic |
| All 5 retries fail | Table logged as error; pipeline continues to next table | Run again |

---

## Migration Order

Tables are migrated in strict FK-dependency order — parents always before children:

```
LAYER 1 — Reference data (no FK dependencies)
  departments  →  stores  →  vendors  →  subcontractors  →  agencies

LAYER 2 — Core entities
  camps  →  camp_rooms  →  employees  →  erp_users

LAYER 3 — Projects & project sub-tables
  projects  →  project_wbs  →  boq_items  →  project_activities
           →  budgets  →  variation_orders  →  daily_site_reports

LAYER 4 — HSE
  hse_incidents  →  hse_inspections  →  toolbox_talks
  safety_training  →  permits_to_work

LAYER 5 — Equipment & vehicles
  equipment  →  equipment_log
  vehicles   →  vehicle_fuel  →  vehicle_maintenance  →  vehicle_trips

LAYER 6 — HR & attendance (LARGE)
  attendance [3.9M]  →  leave_requests  →  appraisals  →  eosb_records

LAYER 7 — Payroll
  payroll_runs  →  loans  →  payroll_details [150k]  →  gosi_declarations

LAYER 8 — Procurement
  purchase_requests  →  purchase_request_items  →  purchase_orders
  po_items  →  goods_receipts  →  grn_items
  rfqs  →  rfq_vendors  →  rfq_items  →  vendor_ratings

LAYER 9 — Recruitment
  job_requisitions  →  job_postings  →  candidates
  job_applications  →  offer_letters

LAYER 10 — Subcontract
  subcontract_orders  →  subcontract_payments

LAYER 11 — Finance
  bank_accounts  →  bank_transactions
  fixed_assets   →  depreciation_schedule
  journal_entries  →  je_lines
  vendor_invoices  →  client_invoices  →  vat_returns

LAYER 12 — Camp operations & DMS
  camp_maintenance  →  camp_notices
  dms_documents  →  dms_access_log

LAYER 13 — WPS & audit (LARGE)
  wps_records [150k]  →  erp_alerts  →  erp_audit_log [550k]
```

---

## Skipped Tables

The following tables are **never migrated** — they contain application configuration
or system state that should not be overwritten on the remote:

| Table | Reason |
|---|---|
| `erp_settings` | Application configuration (URLs, thresholds, feature flags) |
| `erp_permissions` | Role-based access control definitions |
| `chart_of_accounts` | COA already seeded from the ERP schema SQL |
| `finance_settings` | Finance module configuration |
| `item_categories` | Reference data already seeded |
| `sp_*` (7 tables) | Spreadsheet application internal state |
| `kiosk_rate_limit` | Runtime rate-limiter state |
| `admin_users` | Website admin credentials |
| `dms_folders` | DMS folder structure (already seeded) |

To migrate a skipped table anyway, use `--only`:

```bash
python migrate_to_remote.py --only erp_permissions
```

---

## Performance Tuning

### Chunk size

The single biggest lever for throughput. Each chunk = one `INSERT ... executemany()`
call = one network round-trip. Fewer round-trips = faster migration over WAN.

| Connection | Recommended `--chunk` |
|---|---|
| Local LAN (< 1ms) | 10,000 – 50,000 |
| Fast VPN / DC link (< 10ms) | 5,000 – 10,000 |
| Typical WAN (20–100ms) | 2,000 – 5,000 |
| Slow / unstable WAN (> 100ms) | 500 – 1,000 |

```bash
# WAN migration to Germany (this project)
python migrate_to_remote.py --chunk 5000
```

### Expected throughput

At `--chunk 5000` over WAN to `********`:

| Table | Rows | Approx time |
|---|---|---|
| `attendance` | 3,905,883 | ~5–6 hours |
| `erp_audit_log` | 550,006 | ~45 min |
| `payroll_details` | 150,667 | ~12 min |
| `wps_records` | 150,667 | ~12 min |
| All other tables | ~110,000 | ~15 min |
| **Total** | **~4,866,000** | **~6–7 hours** |

### MySQL server-side optimisations (optional)

Set these on the **remote** server before a full reload for maximum bulk-load speed:

```sql
SET GLOBAL innodb_flush_log_at_trx_commit = 2;   -- batch fsync (restore to 1 after)
SET GLOBAL innodb_buffer_pool_size = 1073741824;  -- 1GB buffer pool if RAM allows
SET GLOBAL bulk_insert_buffer_size = 268435456;   -- 256MB bulk buffer
```

Restore after migration:

```sql
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
```

---

## Monitoring Progress

### Live output

The pipeline prints a live progress bar for every table:

```
  attendance     [████████████████████░░░░░░░░░░] 2,888,000/3,905,883  179/s  ETA 1:34:57
```

| Element | Meaning |
|---|---|
| `████░░░░` | % of rows migrated for this table |
| `2,888,000/3,905,883` | Rows done / total |
| `179/s` | Current throughput (rows per second) |
| `ETA 1:34:57` | Estimated time to complete this table |

### Check remote counts while running

Open a second terminal and query the remote:

```bash
python -c "
import mysql.connector
conn = mysql.connector.connect(host='********', port=3306,
    user='********', password='********',
    database='********', connection_timeout=10, autocommit=True)
cur = conn.cursor()
for t in ['attendance', 'payroll_details', 'erp_audit_log', 'wps_records']:
    cur.execute('SELECT COUNT(*) FROM ' + t)
    print(t, cur.fetchone()[0])
conn.close()
"
```

### Check checkpoint file

```bash
python -c "
import json
from pathlib import Path
state = json.loads(Path('.migration_state.json').read_text())
for t, v in state.items():
    status = 'DONE' if v.get('done') else 'PARTIAL'
    print('{:40s} {:>10,}  {}'.format(t, v['offset'], status))
"
```

---

## Output Files

| File | Location | Description |
|---|---|---|
| `.migration_state.json` | Same dir as script | Checkpoint: per-table offset + status. **Do not delete unless doing a full reset.** |
| `migration_report.txt` | Same dir as script | Summary written on completion: table rows, offsets, elapsed times, any errors |

### Sample `migration_report.txt`

```
MY ERP Migration COMPLETE — 2026-06-12 22:15:03
Mode: auto  |  Remote: ********/********
Total rows: 4,865,925  |  Tables: 98  |  Time: 22845s

Table                                         Rows      Offset      Time  Status
----------------------------------------------------------------------------------
departments                                     10          10        1s  ✓
employees                                    1,673       1,673        2s  ✓
attendance                               3,905,883   3,905,883    21600s  ✓
payroll_details                            150,667     150,667      720s  ✓
erp_audit_log                              550,006     550,006     2700s  ✓
...
```

---

## Troubleshooting

### `MySQL Connection not available`

The connection timed out. The pipeline auto-reconnects — if you see this, just wait.
If it persists and the script exits, run again:

```bash
python migrate_to_remote.py --chunk 5000
```

### `Access denied for user`

Wrong credentials. Check `REMOTE` dict at the top of the script.

### Table shows 0 rows after migration

1. Check if it's in `SKIP_TABLES` — if so, use `--only <table>`
2. Check local count: `SELECT COUNT(*) FROM <table>` on local
3. Check for errors in `migration_report.txt`

### Migration stuck / very slow

```bash
# Check network latency to remote
ping ********

# Increase chunk size
python migrate_to_remote.py --chunk 10000
```

### Checkpoint corrupted

```bash
# Delete and restart (data already on remote won't be duplicated due to INSERT IGNORE)
del analytical_engineering\data_generator\.migration_state.json
python migrate_to_remote.py --mode incremental
```

### Want to re-run one table cleanly

```bash
# Remove that table from checkpoint, then run only that table
python -c "
import json
from pathlib import Path
f = Path('analytical_engineering/data_generator/.migration_state.json')
s = json.loads(f.read_text())
s.pop('attendance', None)
f.write_text(json.dumps(s, indent=2))
"
python migrate_to_remote.py --only attendance
```

---

## Data Volumes

Full migration dataset (local → remote):

| Domain | Key Tables | Rows |
|---|---|---|
| HR & Workforce | employees, leave_requests, appraisals | 28,994 |
| Attendance | attendance | 3,905,883 |
| Payroll & WPS | payroll_runs, payroll_details, wps_records | 301,526 |
| Projects | projects, boq_items, activities, DSRs | 34,330 |
| HSE | incidents, inspections, toolbox_talks, training | 22,397 |
| Equipment & Fleet | equipment_log, vehicle_trips, fuel | 4,406 |
| Procurement | POs, PRs, GRNs, RFQs | 5,207 |
| Finance | JEs, invoices, fixed_assets | 8,463 |
| Audit Log | erp_audit_log | 550,006 |
| Other | camp, DMS, recruitment, subcontract... | ~5,000 |
| **TOTAL** | **98 tables** | **~4,866,000** |

---

## Related Files

```
analytical_engineering/data_generator/
├── migrate_to_remote.py      ← this script
├── MIGRATE_README.md         ← this document
├── .migration_state.json     ← checkpoint (auto-generated, do not commit)
├── migration_report.txt      ← completion report (auto-generated)
├── generate.py               ← generates data into local MySQL
├── rerun_missing.py          ← patches incomplete local tables
└── config.yaml               ← local DB connection config
```

---

## Quick Start

```bash
# 1. Ensure local DB is populated
python analytical_engineering/data_generator/generate.py

# 2. Check connectivity
python -c "import mysql.connector; mysql.connector.connect(host='********',port=3306,user='********',password='********',database='********').close(); print('OK')"

# 3. Dry-run to verify plan
python analytical_engineering/data_generator/migrate_to_remote.py --dry-run

# 4. Run migration (recommended chunk size for WAN)
python analytical_engineering/data_generator/migrate_to_remote.py --chunk 5000

# 5. If interrupted — just run the same command again
python analytical_engineering/data_generator/migrate_to_remote.py --chunk 5000
```
