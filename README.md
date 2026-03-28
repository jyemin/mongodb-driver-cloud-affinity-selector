# MongoDB Driver Cloud Affinity Selector

A custom server selector and topology listener for MongoDB drivers that routes
application traffic to mongos routers co-located in the same cloud provider and
region as the application server.

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

See [`python/example.py`](python/example.py) for a runnable demo with full
logging and CLI arguments.
