"""
example.py
==========
Runs "hello" in a loop against an Atlas sharded cluster, using
CloudAffinityManager to prefer same-cloud, same-region mongos routers.

Usage:
    python3 example.py mongodb+srv://user:pass@cluster0.example.mongodb.net/ \
        [--cloud AWS|GCP|AZURE|auto|none] \
        [--region us-east-1|us-west1|eastus|auto|none]

Logs at DEBUG level so you can see cloud detection, tag fetch scheduling, and
which selector tier (1/2/3) is chosen on each operation. In production, INFO
is sufficient.
"""

import argparse
import logging
import time
from typing import Optional

from pymongo import MongoClient

from cloud_affinity_selector import CloudAffinityManager


def _parse_cloud(value: str) -> Optional[str]:
    if value.lower() == "none":
        return None
    if value.lower() == "auto":
        return "auto"
    return value.upper()


def _parse_region(value: str) -> Optional[str]:
    if value.lower() == "none":
        return None
    if value.lower() == "auto":
        return "auto"
    return value  # "auto" or a native region name like "us-west-1"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test cloud affinity mongos selection by running hello in a loop."
    )
    parser.add_argument("uri", help="MongoDB connection string (e.g. mongodb+srv://...)")
    parser.add_argument(
        "--cloud",
        default="auto",
        help="Cloud provider: AWS, GCP, AZURE, auto, or none.",
    )
    parser.add_argument(
        "--region",
        default="auto",
        help="Cloud-native region (e.g. us-east-1, us-west1, eastus), auto, or none.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    log = logging.getLogger(__name__)

    with CloudAffinityManager(
        local_cloud=_parse_cloud(args.cloud),
        local_region=_parse_region(args.region),
    ) as manager:
        client = MongoClient(
            args.uri,
            server_selector=manager,
            event_listeners=[manager],
        )
        try:
            while True:
                try:
                    result = client.admin.command("hello")
                    print(result, flush=True)
                except Exception as exc:
                    log.error("hello failed: %s", exc)
                time.sleep(2)
        finally:
            client.close()


if __name__ == "__main__":
    main()
