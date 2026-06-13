
```markdown
# MY ERP — ETL Migration Pipeline

Production-grade ETL pipelines for migrating the MY ERP dataset (~4.8 million rows, 98 tables) from local MySQL to remote MySQL server. Two versions available: **V1 (Classic)** for slow/unstable connections, and **V2 (Parallel)** for high-speed production environments.

---

## Quick Navigation

- [Version Comparison](#version-comparison) - Choose the right tool
- [V2: Parallel Pipeline](#v2-parallel-pipeline) - High-speed migration
- [V1: Classic Pipeline](#v1-classic-pipeline) - Resilient single-threaded
- [Quick Start](#quick-start) - Get running in 5 minutes
- [Configuration](#configuration) - Connection settings
- [Performance Guide](#performance-guide) - Optimization tips

---

## Version Comparison

| Feature | V1 (Classic) | V2 (Parallel) |
|---------|-------------|---------------|
| **Architecture** | Single-threaded | Multi-threaded parallel |
| **Parallel Tables** | ❌ | ✅ 2-8 tables simultaneously |
| **Parallel Batches** | ❌ | ✅ For tables >5M rows |
| **Connection Pooling** | ❌ | ✅ 10-20 connections |
| **Adaptive Batching** | ❌ Fixed size | ✅ Dynamic optimization |
| **Memory Footprint** | ~50MB | ~200-500MB |
| **Checkpoint System** | ✅ | ✅ Thread-safe |
| **Auto-Retry** | ✅ 5 attempts | ✅ 5 attempts |
| **Idempotent** | ✅ INSERT IGNORE | ✅ INSERT IGNORE |
| **Progress Display** | ASCII bar + ETA | Log-based + metrics |
| **Best For** | Slow/unstable WAN | Fast production networks |

### Performance (4.8M rows over WAN)

| Scenario | V1 Time | V2 Time | Speedup |
|----------|---------|---------|---------|
| Small tables (<50k) | 5-10 min | 2-4 min | 2-3x |
| Medium (50k-500k) | 30-45 min | 12-18 min | 2.5-3x |
| Large (500k-5M) | 2-3 hours | 30-45 min | 3-4x |
| Very large (>5M) | 6-8 hours | 1-2 hours | 4-5x |
| **Total (98 tables)** | **7-9 hours** | **1.5-3 hours** | **3-5x** |

### When to Use Each Version

**Use V1 (Classic) when:**
- Network is slow or unstable (<10 Mbps)
- Remote server has limited resources (low RAM/CPU)
- You need minimal memory footprint (<100MB)
- Running on Raspberry Pi or small VM
- You prefer predictable, single-threaded execution

**Use V2 (Parallel) when:**
- Network is fast (>50 Mbps, low latency)
- You need to complete migration quickly
- Remote has adequate resources (4+ cores, 2GB+ RAM)
- Migrating very large tables (>1M rows)
- You want detailed performance metrics

---

## V2: Parallel Pipeline

### Architecture

```
LOCAL MYSQL (4.8M rows)
    │
    ├──► Thread 1: Table A (Batch 1,2,3...)
    ├──► Thread 2: Table B (Batch 1,2,3...)
    └──► Thread 3: Table C (Batch 1,2,3...)
         │
         ▼
    Connection Pool Manager
    (10 local + 10 remote connections)
         │
         ▼
REMOTE MYSQL (Parallel writes)
```

### Key Features

- **Parallel Table Migration** - Multiple tables simultaneously
- **Parallel Batch Processing** - Large tables split into concurrent chunks
- **Connection Pooling** - Reuse connections across threads
- **Adaptive Batch Sizing** - Dynamic optimization based on table size/row width
- **Streaming Cursors** - Memory-efficient for tables >500k rows
- **Thread-Safe Checkpointing** - Resume any interrupted table

### Installation

```bash
pip install mysql-connector-python==8.3.0
```

### Configuration

Edit the connection settings at the top of `migrate_to_remote_v2.py`:

```python
LOCAL = dict(
    host="localhost", port=3306,
    user="your_user", password="your_password",
    database="your_database",
    autocommit=False, connection_timeout=30,
    use_pure=True,
    pool_name="local_pool",
    pool_size=10,
)

REMOTE = dict(
    host="remote.host.com", port=3306,
    user="remote_user", password="remote_password",
    database="remote_database",
    autocommit=False, connection_timeout=60,
    use_pure=True,
    pool_name="remote_pool",
    pool_size=10,
)
```

### V2 CLI Reference

```bash
python migrate_to_remote_v2.py [OPTIONS]

Options:
  --mode {auto,full,incremental}  Migration mode (default: auto)
  --only TABLES                    Comma-separated tables to migrate
  --skip TABLES                    Comma-separated tables to skip
  --chunk N                        Base batch size (default: 2000)
  --dry-run                        Preview only, no writes
  --reset-checkpoint               Delete checkpoint and restart
  --parallel-tables N              Tables to migrate in parallel (1-8, default: 4)
  --parallel-batches N             Parallel batches for large tables (1-6, default: 2)
  --no-adaptive-batching           Disable dynamic batch sizing
```

### V2 Usage Examples

```bash
# Basic parallel migration (recommended)
python migrate_to_remote_v2.py --mode auto --parallel-tables 4

# Aggressive parallelism for fast network
python migrate_to_remote_v2.py --mode auto --parallel-tables 8 --parallel-batches 4 --chunk 5000

# Conservative for limited resources
python migrate_to_remote_v2.py --mode auto --parallel-tables 2 --parallel-batches 1 --chunk 1000

# Single large table with parallel batches
python migrate_to_remote_v2.py --only attendance --parallel-batches 6 --chunk 10000

# Full reload with maximum parallelism
python migrate_to_remote_v2.py --mode full --parallel-tables 6 --parallel-batches 4

# Dry run to preview
python migrate_to_remote_v2.py --dry-run --parallel-tables 4
```

### V2 Performance Tuning

| Parameter | Default | Range | Effect |
|-----------|---------|-------|--------|
| `--parallel-tables` | 4 | 1-8 | Higher = more concurrency, more CPU |
| `--parallel-batches` | 2 | 1-6 | Higher = faster large tables, more memory |
| `--chunk` | 2000 | 500-10000 | Higher = fewer round trips, more memory per batch |

**Optimization Guide:**
1. Start with `--parallel-tables 2 --parallel-batches 1`
2. Monitor CPU: Increase parallelism if CPU < 70%
3. Monitor memory: Decrease if usage > 80%
4. Test network: Lower latency = more parallelism

### V2 Output Example

```
14:23:11 [Thread-1] INFO: Starting migration of attendance
14:23:15 [Thread-1] INFO: attendance: 3,905,883 rows, batch size=8,000
14:23:45 [Thread-2] INFO: employees: 1,673 rows, batch size=2,000
14:24:20 [Thread-1] INFO: attendance: 15.2% (593k/3.9M) @ 12,450 rows/s

================================================================================
  MIGRATION COMPLETE
  Tables: 98 | Rows: 4,865,925 | Errors: 0
  Time: 8420s (140.3 min) | Rate: 578 rows/s

Performance by Table:
attendance                                  3,905,883   4200.0s      930 rows/s
erp_audit_log                                 550,006    420.0s    1,310 rows/s
payroll_details                               150,667    120.0s    1,256 rows/s
...
```

---

## V1: Classic Pipeline

### Architecture

```
LOCAL MYSQL → EXTRACT (SELECT LIMIT OFFSET) → TRANSFORM (Column filter) → LOAD (INSERT IGNORE) → REMOTE MYSQL
                    ↑                                                    ↓
              Checkpoint saved every batch                    Commit per batch
```

### Key Features

- **Simple & Predictable** - Single-threaded, easy to debug
- **Low Memory** - ~50MB RAM, works on Raspberry Pi
- **Resumable** - Checkpoint after every batch
- **Visual Progress** - ASCII bar with ETA per table
- **Auto-Retry** - Exponential backoff (3s→6s→12s→24s→48s)
- **Keep-Alive** - Pings every 5,000 rows, auto-reconnects

### Installation

```bash
pip install mysql-connector-python==8.3.0
```

### Configuration

Edit the connection settings at the top of `migrate_to_remote.py`:

```python
LOCAL = dict(
    host="localhost", port=3306,
    user="your_user", password="your_password",
    database="your_database",
    autocommit=False, connection_timeout=30,
    use_pure=True,
)

REMOTE = dict(
    host="remote.host.com", port=3306,
    user="remote_user", password="remote_password",
    database="remote_database",
    autocommit=False, connection_timeout=60,
    use_pure=True,
)
```

### V1 CLI Reference

```bash
python migrate_to_remote.py [OPTIONS]

Options:
  --mode {auto,full,incremental}  Migration mode (default: auto)
  --only TABLES                    Comma-separated tables to migrate
  --skip TABLES                    Comma-separated tables to skip
  --chunk N                        Rows per batch (default: 1000)
  --dry-run                        Preview only, no writes
  --reset-checkpoint               Delete checkpoint and restart
```

### V1 Usage Examples

```bash
# Basic migration (auto-resume)
python migrate_to_remote.py

# Full reload from scratch
python migrate_to_remote.py --mode full --chunk 5000

# Migrate single table
python migrate_to_remote.py --only attendance

# Skip large tables
python migrate_to_remote.py --skip erp_audit_log,attendance

# Incremental sync (add missing rows)
python migrate_to_remote.py --mode incremental --chunk 5000

# Dry run (read-only)
python migrate_to_remote.py --dry-run
```

### V1 Output Example

```
  attendance     [████████████████████░░░░░░░░░░] 2,888,000/3,905,883  179/s  ETA 1:34:57
  payroll_details[██████████████████████████████]   150,667/150,667  2,401/s  DONE
```

---

## Running Modes (Both Versions)

### `auto` (default) - Resume from checkpoint
Picks up where it left off using `.migration_state.json`. Safe to interrupt and restart.

```bash
python migrate_to_remote.py                    # V1
python migrate_to_remote_v2.py --mode auto     # V2
```

### `full` - Complete reload (destructive)
Truncates all remote tables, clears checkpoint, migrates everything fresh.

```bash
python migrate_to_remote.py --mode full        # V1
python migrate_to_remote_v2.py --mode full     # V2
```

⚠️ **Warning:** Deletes all existing data on remote before migration.

### `incremental` - Add missing rows only
Compares row counts, only inserts rows where remote count < local count.

```bash
python migrate_to_remote.py --mode incremental      # V1
python migrate_to_remote_v2.py --mode incremental   # V2
```

---

## Performance Guide

### Recommended Settings by Environment

| Environment | Version | Parallel Tables | Parallel Batches | Chunk Size |
|-------------|---------|-----------------|------------------|------------|
| **Local/LAN** | V2 | 6-8 | 4 | 8,000-10,000 |
| **Fast WAN (50+ Mbps)** | V2 | 4-6 | 3 | 5,000-8,000 |
| **Medium WAN (10-50 Mbps)** | V2 | 2-4 | 2 | 3,000-5,000 |
| **Slow WAN (<10 Mbps)** | V1 | N/A | N/A | 2,000-3,000 |
| **Unstable connection** | V1 | N/A | N/A | 500-1,000 |
| **Low memory (<1GB)** | V1 | N/A | N/A | 500-1,000 |

### Chunk Size Guidelines

| Connection Type | Recommended Chunk | Why |
|----------------|-------------------|-----|
| Local LAN (<1ms) | 10,000-50,000 | Minimal latency, maximize throughput |
| Fast VPN (<10ms) | 5,000-10,000 | Balance round trips and batch size |
| Typical WAN (20-100ms) | 2,000-5,000 | Sweet spot for most connections |
| Slow WAN (>100ms) | 500-1,000 | Avoid timeout, ensure reliability |

### MySQL Server Optimizations

For maximum bulk-load speed on the remote server:

```sql
-- Before migration
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL innodb_buffer_pool_size = 1073741824;  -- 1GB
SET GLOBAL bulk_insert_buffer_size = 268435456;   -- 256MB

-- After migration
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
```

---

## Checkpoint System

Both versions use `.migration_state.json` for resumability:

```json
{
  "employees": {
    "offset": 1673,
    "inserted": 1673,
    "done": true,
    "ts": "2026-06-12T14:23:11"
  },
  "attendance": {
    "offset": 2608000,
    "inserted": 2608000,
    "done": false,
    "ts": "2026-06-12T16:45:03"
  }
}
```

| Field | Meaning |
|-------|---------|
| `offset` | Rows successfully migrated (resume point) |
| `inserted` | Rows inserted in this run |
| `done` | `true` = complete, `false` = partial |
| `ts` | Last checkpoint timestamp |

### Manual Checkpoint Management

```bash
# Reset checkpoint
--reset-checkpoint                    # CLI flag
rm .migration_state.json              # Manual delete

# Remove specific table from checkpoint
python -c "
import json
s = json.load(open('.migration_state.json'))
s.pop('attendance', None)
json.dump(s, open('.migration_state.json', 'w'))
"

# Sync checkpoint to remote counts
python -c "
import mysql.connector, json
remote = mysql.connector.connect(**REMOTE)
cur = remote.cursor()
state = {}
for table in ['attendance', 'payroll_details']:
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    state[table] = {'offset': cur.fetchone()[0], 'done': True}
json.dump(state, open('.migration_state.json', 'w'))
"
```

---

## Migration Order

Tables are migrated in FK-dependency order (parents before children):

```
Layer 1: Reference (departments, stores, vendors, agencies)
Layer 2: Core (camps, camp_rooms, employees, erp_users)
Layer 3: Projects (projects, wbs, boq_items, activities)
Layer 4: HSE (incidents, inspections, training, permits)
Layer 5: Equipment (equipment, vehicles, fuel, maintenance)
Layer 6: HR (attendance[3.9M], leave_requests, appraisals)
Layer 7: Payroll (payroll_runs, payroll_details[150k], loans)
Layer 8: Procurement (purchase_requests, orders, receipts)
Layer 9: Recruitment (requisitions, postings, candidates)
Layer 10: Finance (bank_accounts, journal_entries, invoices)
Layer 11: WPS & Audit (wps_records[150k], erp_audit_log[550k])
```

---

## Skipped Tables

These tables are NEVER migrated (config/system tables):

- `erp_settings`, `erp_permissions`, `chart_of_accounts`
- `finance_settings`, `item_categories`
- `sp_*` (7 spreadsheet tables: sp_audit_entries, sp_cells, sp_presence, etc.)
- `kiosk_rate_limit`, `admin_users`, `dms_folders`

**To migrate a skipped table:** Use `--only <tr>`

```bash
python migrate_to_remote_v2.py --only erp_permissions
```

---

## Data Volumes

Full migration dataset (98 tables, ~4.87M rows):

| Domain | Key Tables | Rows |
|--------|-----------|------|
| HR & Workforce | employees, leave_requests, appraisals | 28,994 |
| Attendance | attendance | 3,905,883 |
| Payroll & WPS | payroll_runs, payroll_details, wps_records | 301,526 |
| Projects | projects, boq_items, activities, DSRs | 34,330 |
| HSE | incidents, inspections, toolbox_talks | 22,397 |
| Equipment & Fleet | equipment_log, vehicle_trips, fuel | 4,406 |
| Procurement | POs, PRs, GRNs, RFQs | 5,207 |
| Finance | JEs, invoices, fixed_assets | 8,463 |
| Audit Log | erp_audit_log | 550,006 |
| Other | camp, DMS, recruitment, subcontract | ~5,000 |
| **TOTAL** | **98 tables** | **~4,866,000** |

---

## Troubleshooting

### Common Issues (Both Versions)

| Issue | Solution |
|-------|----------|
| **Connection timeout** | Reduce `--chunk` size, increase `connection_timeout` in config |
| **Out of memory** | Use V1, or reduce `--parallel-tables` in V2 |
| **Slow performance** | Increase `--chunk`, check network latency with `ping` |
| **Table missing on remote** | Run remote schema setup first |
| **"Access denied"** | Verify credentials in LOCAL/REMOTE dicts |
| **Table shows 0 rows after migration** | Check if in SKIP_TABLES, use `--only <table>` |

### V2-Specific Issues

| Issue | Solution |
|-------|----------|
| **High CPU usage** | Reduce `--parallel-tables` or use V1 |
| **Connection pool exhausted** | Increase `pool_size` in config (max 20) |
| **Thread contention** | Reduce `--parallel-batches` |
| **Deadlock errors** | Ensure `INSERT IGNORE` is used, reduce `--parallel-batches` |

### Network Diagnostics

```bash
# Test latency to remote
ping remote.host.com

# Test MySQL connectivity
mysql -h remote.host.com -u user -p -e "SELECT 1"

# Monitor remote counts during migration
watch -n 10 'mysql -h remote.host.com -u user -p -e "SELECT COUNT(*) FROM attendance"'
```

### Recovery Procedures

After any interruption, simply run the same command again:

```bash
# V1
python migrate_to_remote.py

# V2
python migrate_to_remote_v2.py --mode auto --parallel-tables 4
```

For specific failed tables:
```bash
python migrate_to_remote_v2.py --only failed_table_1,failed_table_2
```

---

## Output Files

| File | Version | Description |
|------|---------|-------------|
| `.migration_state.json` | Both | Checkpoint file (do not delete unless resetting) |
| `migration_report.txt` | V1 | Basic completion summary |
| `migration_performance_report.txt` | V2 | Detailed performance metrics |

---

## Quick Start

### First-time users: Start with V1

```bash
# 1. Install dependency
pip install mysql-connector-python

# 2. Edit connection config in migrate_to_remote.py
#    - Set LOCAL and REMOTE connection parameters

# 3. Test connectivity
python -c "import mysql.connector; mysql.connector.connect(**LOCAL).close(); print('OK')"

# 4. Dry run to verify
python migrate_to_remote.py --dry-run

# 5. Run migration
python migrate_to_remote.py --chunk 5000
```

### Upgrade to V2 for speed

```bash
# 1. Use same config (copy from V1 to migrate_to_remote_v2.py)

# 2. Test with conservative settings
python migrate_to_remote_v2.py --dry-run --parallel-tables 2

# 3. Run with moderate parallelism
python migrate_to_remote_v2.py --mode auto --parallel-tables 4 --parallel-batches 2

# 4. Optimize based on performance
#    - Low CPU (<50%): increase --parallel-tables
#    - High memory (>80%): decrease --parallel-batches
#    - Network bottleneck: increase --chunk
```

### Production Recommendation

For production migration with good network (50+ Mbps, <50ms latency):

```bash
python migrate_to_remote_v2.py --mode full \
    --parallel-tables 6 \
    --parallel-batches 3 \
    --chunk 5000

# Expected: 1.5-2.5 hours for complete 4.8M row migration
```

---

## Version Selection Decision Tree

```
Start here
    │
    ▼
Is network >20 Mbps and stable?
    │
    ├── NO ──→ Use V1 (Classic)
    │           - Reliable on slow connections
    │           - Low memory (~50MB)
    │           - Predictable performance
    │
    └── YES
         │
         ▼
    Does remote server have 4+ CPU cores?
         │
         ├── NO ──→ Use V1 (Classic)
         │           - Avoids thread contention
         │           - Single-threaded is fine
         │
         └── YES
              │
              ▼
         Does remote have 2GB+ RAM?
              │
              ├── NO ──→ Use V2 (conservative)
              │           --parallel-tables 2 --parallel-batches 1
              │
              └── YES ──→ Use V2 (aggressive)
                          --parallel-tables 4-8 --parallel-batches 2-4
                          Achieve 3-5x speedup over V1
```

---

## Support & Documentation

- **V1 (Classic)**: Best for slow/unstable connections, low-resource environments
- **V2 (Parallel)**: Best for high-speed production, includes performance tuning

Both versions are production-tested on the complete MY ERP dataset (4.8M rows, 98 tables).

### File Structure

```
data_migration_python/
├── migrate_to_remote.py          # V1: Classic single-threaded
├── migrate_to_remote_v2.py       # V2: Parallel processing
├── README.md                     # This document
├── .migration_state.json         # Checkpoint (auto-generated)
├── migration_report.txt          # V1 completion report
├── migration_performance_report.txt # V2 performance report
└── requirements.txt              # Dependencies
```

### Dependencies

```
mysql-connector-python==8.3.0
# V2 only (included automatically):
# - threading (built-in)
# - concurrent.futures (built-in)
# - dataclasses (built-in for Python 3.7+)
```

---

## License

Internal use only - MY ERP System

---

## Version History

- **V2.0** - June 2026: Parallel processing, connection pooling, adaptive batching
- **V1.0** - June 2026: Initial release, resilient single-threaded pipeline
```

