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
