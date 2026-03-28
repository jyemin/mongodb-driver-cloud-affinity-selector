# MongoDB Driver Cloud Affinity Selector

A custom server selector and topology listener for the MongoDB Python driver 
that routes application traffic to mongos routers co-located in the same cloud 
provider and region as the application server.

## The Problem

Multi-cloud Atlas sharded clusters span multiple cloud providers. The driver
selects mongos routers using a latency-window algorithm — any router within
15ms of the fastest one is eligible, and the driver round-robins across all of
them. When AWS and GCP infrastructure share a metro, their RTTs easily fall
within that window.

Result: an app running in AWS can be routed through a GCP mongos, incurring
cross-cloud egress charges on every operation.

## How It Works

Two PyMongo extension points are combined:

1. **`server_selector`** — filters mongos candidates to prefer same-cloud,
   same-region routers before the latency threshold is applied.

2. **`event_listeners` (ServerListener)** — detects newly discovered mongos
   servers and kicks off background probes to fetch their cloud/region tags
   from the co-located mongod.

Selection falls back through three tiers:

| Tier | Condition | Rationale |
|------|-----------|-----------|
| 1 | Same cloud + same region | No cross-cloud traffic, minimal latency variance |
| 2 | Same cloud, any region | Avoids egress charges; HA fallback if same-region mongos is down |
| 3 | All candidates | Availability over affinity — never fail an operation for routing reasons |

## Implementations

| Language | Directory |
|----------|-----------|
| Python   | [`python/`](python/) |

## Requirements

- An Atlas multi-cloud sharded cluster
- Direct network access from app servers to Atlas mongod nodes (port 27017)

## Usage (Python)

```python
from cloud_affinity_selector import CloudAffinityManager
from pymongo import MongoClient

with CloudAffinityManager() as manager:
    client = MongoClient(
        "mongodb+srv://user:pass@cluster0.example.mongodb.net/",
        server_selector=manager,
        event_listeners=[manager],
    )
    # all operations now prefer same-cloud, same-region mongos routers
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `local_cloud` | `"auto"` | Cloud provider the app is running in: `"AWS"`, `"GCP"`, `"AZURE"`, `"auto"` (detect from instance metadata), or `None` (disable affinity). |
| `local_region` | `"auto"` | Cloud-native region name (e.g. `"us-east-1"`, `"us-west1"`, `"eastus"`), `"auto"` (detect from instance metadata), or `None` (cloud affinity only, no region preference). Ignored if `local_cloud` is `None`. |
| `mongod_port` | `27017` | Port of the mongod co-located with each mongos on the same host. |
| `max_fetch_workers` | `4` | Maximum background threads for mongod tag probes. One probe fires per discovered mongos. |
| `probe_timeout_ms` | `3000` | Timeout in milliseconds for each mongod probe (connect + hello + close). |

See [`python/example.py`](python/example.py) for a runnable demo with full
logging and CLI arguments.
