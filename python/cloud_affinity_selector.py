"""
cloud_affinity_selector.py
==========================
A custom PyMongo server selector that routes application traffic to mongos
routers that are co-located in the same cloud provider and region as the
application server.

THE PROBLEM
-----------
Multi-cloud Atlas sharded clusters span multiple cloud providers — for example,
AWS us-east-1 and GCP us-east4 within the same metro, plus GCP us-central1 for
HA. The driver selects mongos routers using a latency-window algorithm: it
measures the RTT to each mongos and admits any router within 15ms of the
fastest one. When AWS and GCP infrastructure sit in the same metro, their RTTs
can easily fall within that window. The driver then round-robins across all
eligible routers regardless of cloud origin.

Result: an app running in AWS can be routed through a GCP mongos, then to an
AWS shard primary, then back through GCP to return results. Every cross-cloud
leg incurs egress charges and adds latency variance.

WHY EXISTING LEVERS DON'T HELP
-------------------------------
Read preference tags (e.g. {"provider": "AWS"}) govern which *mongod* members
serve reads within a replica set shard. They have no effect on which *mongos*
the driver picks as its first hop. You can pin reads perfectly to AWS nodes and
still pay GCP egress on every operation if the driver routes through a GCP mongos.

HOW THIS SOLUTION WORKS
------------------------
PyMongo (and most other drivers) exposes two extension points that we combine here:

  1. server_selector — a callable passed to MongoClient that receives the list
     of mongos candidates and returns a (possibly smaller) list. The latency
     threshold (localThresholdMS) is applied by the driver *after* the custom
     selector runs, so the selector sees all candidates before latency winnowing.
     We use this hook to prefer same-cloud, same-region routers.

  2. event_listeners (ServerListener) — an object that PyMongo calls whenever
     the topology changes. We use this to detect newly discovered mongos servers
     and kick off background tag fetches.

The flow for each newly discovered mongos (e.g. mongos-abc.001.mongodb.net:27016):

  a. PyMongo fires ServerOpeningEvent as soon as the SRV record is resolved,
     before the main monitor has even connected. This gives us a head start.

  b. In a background thread, we open a short-lived *direct* connection to the
     co-located mongod on the same host (Atlas convention: mongos on port 27016,
     mongod on port 27017). We call the "hello" command, which requires no
     authentication and returns Atlas-injected replica-set tags like:
       {"provider": "AWS", "region": "US_EAST_1", "nodeType": "ELECTABLE"}

  c. We cache the placement (cloud provider and Atlas region), keyed by the
     mongos address.

  d. The server selector applies a tiered preference using the cache:
       - Same cloud AND same region  (ideal: no cross-cloud, minimal latency)
       - Same cloud, any region      (cross-region within cloud is acceptable)
       - All candidates              (last resort; availability over affinity)

Meanwhile, at startup we detect which cloud and region the app server is in
by probing the link-local metadata endpoints that AWS, GCP, and Azure inject
into every VM. Cloud-native region names (e.g. "us-west1") are translated to
Atlas region identifiers (e.g. "WESTERN_US") via a lookup table, so that the
detected region can be compared directly against the "region" tag in hello
responses.

KNOWN LIMITATIONS
-----------------
- Direct access to Atlas mongod nodes (port 27017) must be reachable from app
  servers.

- The assumption that a mongod is co-located on port 27017 of every mongos
  host (port 27016) is an Atlas internal convention, not a documented stable
  contract.

- The cloud-to-Atlas region lookup table (CLOUD_REGION_TO_ATLAS) was built
  from Atlas documentation at the time of writing. If Atlas adds new regions
  or changes identifiers, the table may need updating. An unknown cloud region
  causes region affinity to be skipped (cloud affinity still applies).

DEPLOYMENT NOTE
---------------
On environments without cloud metadata (e.g. a developer laptop or CI runner),
cloud and region both resolve to None and the selector becomes a transparent
pass-through. It is safe to deploy this selector everywhere without
environment-specific configuration.

REQUIREMENTS
------------
  Python 3.9+
  pip install pymongo
"""

import logging
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple

from pymongo import MongoClient, monitoring

from cloud_regions import CLOUD_REGION_TO_ATLAS

log = logging.getLogger(__name__)


def _lookup_atlas_region(cloud: str, native_region: str) -> Optional[str]:
    """Translate a cloud-native region name to the Atlas replica-set tag value.

    Returns None if the region is not found in the table, in which case
    region affinity will be skipped (cloud affinity still applies).
    """
    atlas_region = CLOUD_REGION_TO_ATLAS.get((cloud, native_region))
    if atlas_region is None:
        log.warning(
            "Cloud region %r (%r) not found in lookup table — "
            "region affinity disabled; cloud affinity still active. "
            "Consider adding it to CLOUD_REGION_TO_ATLAS in cloud_regions.py.",
            native_region,
            cloud,
        )
    return atlas_region


# ---------------------------------------------------------------------------
# Cloud provider and region auto-detection
# ---------------------------------------------------------------------------

# How long (seconds) to wait for a cloud metadata endpoint to respond.
# These are link-local addresses and should respond in <5ms on a real VM.
# Keep this short so startup is not penalised on non-cloud machines.
_METADATA_TIMEOUT_S = 0.5


def _detect_region_gcp() -> Optional[str]:
    """Return the GCP native region, or None if not on GCP."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/zone",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=_METADATA_TIMEOUT_S) as resp:
            if resp.status == 200:
                # Response is a full resource path: "projects/PROJECT/zones/ZONE"
                zone_path = resp.read().decode().strip()
                zone_name = zone_path.split("/")[-1]         # e.g. "us-west1-b"
                native_region = zone_name.rsplit("-", 1)[0]  # e.g. "us-west1"
                log.debug("Cloud detection: GCP (zone=%s, region=%s)", zone_name, native_region)
                return native_region
    except Exception:
        pass
    return None


def _detect_region_azure() -> Optional[str]:
    """Return the Azure native region, or None if not on Azure."""
    try:
        req = urllib.request.Request(
            "http://169.254.169.254/metadata/instance/compute/location"
            "?api-version=2021-02-01&format=text",
            headers={"Metadata": "true"},
        )
        with urllib.request.urlopen(req, timeout=_METADATA_TIMEOUT_S) as resp:
            if resp.status == 200:
                native_region = resp.read().decode().strip()  # e.g. "eastus"
                log.debug("Cloud detection: Azure (region=%s)", native_region)
                return native_region
    except Exception:
        pass
    return None


def _detect_region_aws() -> Optional[str]:
    """Return the AWS native region via IMDSv2, or None if not on AWS."""
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        )
        with urllib.request.urlopen(token_req, timeout=_METADATA_TIMEOUT_S) as resp:
            if resp.status == 200:
                token = resp.read().decode().strip()
                meta_req = urllib.request.Request(
                    "http://169.254.169.254/latest/meta-data/placement/region",
                    headers={"X-aws-ec2-metadata-token": token},
                )
                with urllib.request.urlopen(meta_req, timeout=_METADATA_TIMEOUT_S) as meta_resp:
                    if meta_resp.status == 200:
                        native_region = meta_resp.read().decode().strip()  # e.g. "us-east-1"
                        log.debug("Cloud detection: AWS via IMDSv2 (region=%s)", native_region)
                        return native_region
    except Exception:
        pass
    return None


_REGION_DETECTORS = {
    "GCP":   _detect_region_gcp,
    "AZURE": _detect_region_azure,
    "AWS":   _detect_region_aws,
}


def detect_local_placement(cloud: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """Return (cloud, native_region) for the host this process is running on.

    cloud         — "AWS", "GCP", "AZURE", or None if not on a recognized VM.
    native_region — The cloud provider's own region name (e.g. "us-east-1",
                    "us-west1", "eastus"), or None if cloud is None.

    If *cloud* is provided, only that provider's metadata endpoint is probed
    (faster, and avoids spurious timeout delays for other providers). If
    *cloud* is None, all three are probed in order: GCP, Azure, AWS.

    Returns (None, None) on developer laptops, CI runners, or any environment
    where no metadata endpoint is reachable. CloudAffinityManager becomes a
    no-op in that case.
    """
    if cloud is not None:
        detector = _REGION_DETECTORS.get(cloud)
        if detector is None:
            log.warning("Cloud detection: unknown provider %r — skipping region detection", cloud)
            return cloud, None
        region = detector()
        if region:
            return cloud, region
        log.debug("Cloud detection: %s metadata endpoint not reachable", cloud)
        return cloud, None

    # Full auto-detect: try each provider in order.
    for provider, detector in _REGION_DETECTORS.items():
        region = detector()
        if region:
            return provider, region

    log.debug("Cloud detection: unknown (no cloud metadata reachable)")
    return None, None


# ---------------------------------------------------------------------------
# Cloud Affinity Manager
# ---------------------------------------------------------------------------


class MongosPlacement:
    """The two tag fields we care about for a mongos: cloud provider and Atlas region."""

    __slots__ = ("provider", "region")

    def __init__(self, provider: str, region: str) -> None:
        self.provider = provider
        self.region = region

    def __repr__(self) -> str:
        return f"MongosPlacement(provider={self.provider!r}, region={self.region!r})"


class CloudAffinityManager(monitoring.ServerListener):
    """Prefer same-cloud, same-region mongos routers by combining a topology
    listener with a server selector.

    Register one instance as BOTH event_listeners and server_selector on the
    MongoClient:

        manager = CloudAffinityManager()

        client = MongoClient(
            "mongodb+srv://cluster0.example.mongodb.net/",
            server_selector=manager,   # filters mongos candidates by affinity
            event_listeners=[manager], # tracks topology additions/removals
        )

    As a context manager, CloudAffinityManager shuts down its background
    thread pool cleanly when the with-block exits:

        with CloudAffinityManager() as manager:
            client = MongoClient(..., server_selector=manager, event_listeners=[manager])
            ...
        # thread pool is shut down here
    """

    # Atlas currently co-locates a shard mongod on port 27017 with each
    # mongos on port 27016, on the same host. This is an internal Atlas
    # convention. It is not part of any public API contract and may change.
    _DEFAULT_MONGOD_PORT = 27017

    def __init__(
        self,
        local_cloud: Optional[str] = "auto",
        local_region: Optional[str] = "auto",
        mongod_port: int = _DEFAULT_MONGOD_PORT,
        max_fetch_workers: int = 4,
        probe_timeout_ms: int = 3000,
    ):
        """
        Args:
            local_cloud:
                The cloud provider this app server is running in: "AWS", "GCP",
                "AZURE", or None (disables all affinity filtering). Defaults to
                "auto", which calls detect_local_placement() at construction
                time. Pass an explicit value to skip the metadata probe, e.g.
                during testing or when the placement is known in advance.

            local_region:
                The cloud provider's native region name for this app server
                (e.g. "us-east-1" for AWS, "us-west1" for GCP, "eastus" for
                Azure), or None to skip region affinity while keeping cloud
                affinity. Defaults to "auto", which derives the region from
                the same metadata call as local_cloud. Ignored if local_cloud
                is None.

            mongod_port:
                The port on which Atlas mongod instances are listening on the
                same host as their co-located mongos. Defaults to 27017.
                Override if your Atlas deployment uses a different port.

            max_fetch_workers:
                Maximum number of background threads used for mongod tag
                fetches. One fetch fires per discovered mongos; each fetch
                opens a direct connection, calls hello, and closes.

            probe_timeout_ms:
                Timeout in milliseconds for each background mongod probe
                (connect + hello + close). Applies as serverSelectionTimeoutMS,
                connectTimeoutMS, and socketTimeoutMS on the short-lived probe
                client. Default is 3000 ms (3 seconds), which is generous for
                a co-located host on the same VM hypervisor network. On probe
                failure the mongos falls back to the unfiltered pool rather
                than blocking operations.
        """
        self._mongod_port = mongod_port
        self._probe_timeout_ms = probe_timeout_ms

        if local_cloud is None:
            # Affinity explicitly disabled.
            self._local_cloud = None
            self._local_region = None
        elif local_cloud == "auto":
            # Both cloud and region come from the same metadata call.
            detected_cloud, detected_region = detect_local_placement()
            self._local_cloud = detected_cloud
            self._local_region = detected_region if local_region == "auto" else local_region
        elif local_region == "auto":
            # Cloud is known; only probe that provider's metadata endpoint for region.
            self._local_cloud = local_cloud
            _, detected_region = detect_local_placement(cloud=self._local_cloud)
            self._local_region = detected_region
        else:
            self._local_cloud = local_cloud
            self._local_region = local_region

        # Translate the native region name to the Atlas identifier used in
        # hello tags (e.g. "us-west1" → "WESTERN_US"). Done once here so that
        # __call__, which runs on every operation, does no extra work.
        self._local_atlas_region: Optional[str] = (
            _lookup_atlas_region(self._local_cloud, self._local_region)
            if self._local_cloud and self._local_region
            else None
        )

        log.info(
            "CloudAffinityManager: cloud=%r region=%r (atlas: %r)",
            self._local_cloud,
            self._local_region,
            self._local_atlas_region,
        )

        # Primary data structure: maps a mongos (host, port) to the tags dict
        # retrieved from its co-located mongod via "hello". Example:
        #   ("mongos-abc.001.mongodb.net", 27016) -> MongosPlacement("AWS", "US_EAST_1")
        #
        # Written by background fetch threads; read by the server selector on
        # every database operation. Protected by _lock.
        self._tag_cache: dict[tuple, MongosPlacement] = {}

        # Tracks addresses whose fetch is currently in flight, to avoid
        # scheduling duplicate fetches if ServerOpeningEvent fires more than
        # once for the same address.
        self._pending: set[tuple] = set()

        self._lock = threading.Lock()

        # Bounded thread pool for background probes. A fixed pool prevents
        # unbounded thread creation during large simultaneous topology changes
        # (e.g. when the driver first resolves a large SRV record).
        self._executor = ThreadPoolExecutor(
            max_workers=max_fetch_workers,
            thread_name_prefix="cloud-affinity",
        )

    # -----------------------------------------------------------------------
    # monitoring.ServerListener interface
    # -----------------------------------------------------------------------

    def opened(self, event: monitoring.ServerOpeningEvent) -> None:
        """Fire a background tag fetch as soon as a mongos address is known.

        PyMongo calls this immediately after each address is added to the
        topology — for SRV connections, this happens right after DNS
        resolution, before the main MongoClient monitor has connected. By
        starting the fetch here we run in parallel with the main monitor,
        maximizing the chance that the cache is warm before the mongos ever
        becomes eligible for selection.

        Since this manager is only intended for sharded clusters, every
        address that surfaces here is assumed to be a mongos. We do not wait
        for the server type to be confirmed.
        """
        if not self._local_cloud:
            return

        address = event.server_address  # (host, port) tuple

        with self._lock:
            if self._executor._shutdown:
                return
            if address in self._tag_cache:
                log.debug("CloudAffinityManager: %s:%d already cached, skipping", *address)
                return
            if address in self._pending:
                log.debug("CloudAffinityManager: %s:%d fetch already in flight, skipping", *address)
                return
            self._pending.add(address)

        log.debug("CloudAffinityManager: scheduling tag fetch for mongos %s:%d", *address)
        self._executor.submit(self._fetch_and_cache, address)

    def description_changed(self, event: monitoring.ServerDescriptionChangedEvent) -> None:
        """Called when a server's description changes (e.g. latency update).

        No action taken here. All tag fetches are initiated in opened().

        Note: if the initial probe fails (e.g. a transient network issue at
        startup), the mongos stays absent from the cache and falls through to
        the fallback pool indefinitely — until the topology cycles the address
        through closed() + opened(). A natural retry point would be the
        Unknown → Mongos transition here, since by that point the main monitor
        has confirmed the host is reachable. That is left as a future
        improvement.
        """
        pass

    def closed(self, event: monitoring.ServerClosedEvent) -> None:
        """Evict a mongos from the cache when it is removed from the topology.

        PyMongo calls this during scale-down, failover, or when a server
        becomes permanently unreachable. Evicting ensures that if the same
        address reappears later (e.g. after a scale-up), opened() will fire
        again and schedule a fresh fetch.
        """
        address = event.server_address
        with self._lock:
            evicted = self._tag_cache.get(address)
            if evicted is not None:
                self._tag_cache = {k: v for k, v in self._tag_cache.items() if k != address}
            self._pending.discard(address)

        if evicted is not None:
            log.debug(
                "CloudAffinityManager: evicted %s:%d from cache (was %r)",
                *address,
                evicted,
            )

    # -----------------------------------------------------------------------
    # Background tag fetch
    # -----------------------------------------------------------------------

    def _fetch_and_cache(self, mongos_address: tuple) -> None:
        """Open a direct connection to the co-located mongod, call hello, cache tags.

        This runs in the thread pool — not on the main monitoring thread —
        so it is safe to do blocking I/O here.

        The co-located mongod is assumed to be on the same host as the mongos
        but on self._mongod_port (default 27017). The "hello" command on a
        replica-set member returns a "tags" field that Atlas populates with
        at minimum {"provider": "AWS"|"GCP"|"AZURE", "region": "...", "nodeType": "..."}.

        No authentication is required. "hello" is a pre-auth command that any
        client can call without credentials; it is explicitly designed to be
        safe to expose without authentication.

        On any failure (connection refused, timeout, etc.) the address is
        simply not added to the cache. The server selector will treat it as
        "unknown placement" and include it in the fallback pool, so the
        failure mode is slightly suboptimal routing rather than an outage.
        No retries are attempted; if the server is later removed and re-added
        to the topology, opened() will fire again and schedule a fresh fetch.
        """
        host, mongos_port = mongos_address
        mongod_host_port = f"{host}:{self._mongod_port}"
        tags = None

        try:
            log.debug(
                "CloudAffinityManager: connecting to co-located mongod at %s "
                "(paired with mongos :%d)",
                mongod_host_port,
                mongos_port,
            )

            # directConnection=True tells PyMongo to connect to exactly this
            # host:port without attempting any topology discovery. This is
            # important because we want to reach the specific mongod on this
            # host, not the primary of its replica set.
            #
            # No credentials are provided. "hello" is a pre-auth command;
            # MongoDB will respond to it without requiring authentication.
            #
            # Timeouts are kept short (probe_timeout_ms, default 3 s). This
            # probe is a one-time cost per discovered mongos; we'd rather fail
            # fast and fall back to unfiltered selection than block operations
            # waiting on a slow or unreachable host.
            probe = MongoClient(
                host=host,
                port=self._mongod_port,
                directConnection=True,
                tls=True,
                serverSelectionTimeoutMS=self._probe_timeout_ms,
                connectTimeoutMS=self._probe_timeout_ms,
                socketTimeoutMS=self._probe_timeout_ms,
            )
            try:
                # "hello" is the modern equivalent of "isMaster". On a replica
                # set member, the response includes a "tags" field with the
                # member-level tags configured in the Atlas cluster settings.
                # Atlas injects cloud/region tags automatically on multi-cloud
                # clusters, typically in the form:
                #   {"provider": "AWS", "region": "US_EAST_1", "nodeType": "ELECTABLE"}
                result = probe.admin.command("hello")
                raw_tags = result.get("tags", {})
                provider = raw_tags.get("provider")
                region = raw_tags.get("region")
                if provider and region:
                    tags = MongosPlacement(provider, region)
                    log.info(
                        "CloudAffinityManager: fetched placement for mongos %s:%d → %r",
                        host,
                        mongos_port,
                        tags,
                    )
                else:
                    log.warning(
                        "CloudAffinityManager: hello response from mongod paired with "
                        "mongos %s:%d had no provider/region tags (got: %r). "
                        "This mongos will always fall through to tier-3. "
                        "Are you connecting to an Atlas multi-cloud sharded cluster?",
                        host,
                        mongos_port,
                        raw_tags,
                    )
            finally:
                # Always close the probe client. This releases the connection
                # immediately rather than waiting for GC.
                probe.close()

        except Exception as exc:
            # Log at WARNING level so operators can diagnose connectivity
            # issues (e.g. Atlas network peering not configured for direct
            # shard access) without crashing the application.
            log.warning(
                "CloudAffinityManager: could not fetch tags for mongos %s:%d "
                "via mongod %s — will treat as unknown placement. Error: %s",
                host,
                mongos_port,
                mongod_host_port,
                exc,
            )

        # Update shared state under the lock.
        with self._lock:
            self._pending.discard(mongos_address)
            if tags is not None:
                # It is safe to write even if closed() ran while the fetch was
                # in flight. The server selector only receives candidates from
                # the live topology, so a removed mongos will never be selected.
                # The entry will be cleaned up if the address is later re-added
                # and removed again.
                self._tag_cache = {**self._tag_cache, mongos_address: tags}

    # -----------------------------------------------------------------------
    # Server selector callable
    # -----------------------------------------------------------------------

    def __call__(self, server_descriptions: list) -> list:
        """Filter mongos candidates using a tiered cloud + region affinity policy.

        PyMongo calls this on the hot path of every database operation, after
        suitability filtering but *before* the latency threshold
        (localThresholdMS) is applied. The driver will further narrow the list
        by latency after we return. We may narrow it further but must never
        expand it beyond what we received.

        This method must be fast. It does no I/O and holds the lock only for
        a single reference grab. The cache dict is never mutated in place —
        writers atomically replace it with a new dict, so once we hold a
        reference here it is stable for the rest of this call.

        Filtering tiers (each falls through to the next if no matches)
        ---------------------------------------------------------------
        1. Same cloud AND same region — ideal: no cross-cloud traffic, minimal
           intra-cloud latency variance. Both _local_atlas_region and the cached
           "region" tag are Atlas region identifiers, so the comparison is a
           direct string equality check with no normalization needed.

        2. Same cloud, any region — acceptable: avoids cross-cloud egress
           charges even if the router is in a different region. This is the
           safety net for the HA case where the same-region mongos is down.

        3. All candidates — last resort. Availability always wins over affinity.
           The worst case is a cross-cloud hop, not a failure.
        """
        log.debug(
            "CloudAffinityManager: server selection called with %d candidate(s): %s",
            len(server_descriptions),
            [sd.address for sd in server_descriptions],
        )

        if not self._local_cloud:
            # Affinity disabled (local_cloud=None) or cloud not detected — pass through unchanged.
            return server_descriptions

        with self._lock:
            cache_snapshot = self._tag_cache

        # Tier 1: same cloud + same region.
        # _local_atlas_region is the pre-translated Atlas identifier (e.g.
        # "WESTERN_US") derived from the native region at init time. The cached
        # tag value is already in Atlas format, so this is a direct string
        # comparison with no per-call normalization.
        if self._local_atlas_region:
            same_cloud_region = [
                sd for sd in server_descriptions
                if (p := cache_snapshot.get(sd.address)) is not None
                and p.provider == self._local_cloud
                and p.region == self._local_atlas_region
            ]
            if same_cloud_region:
                log.debug(
                    "CloudAffinityManager: tier-1 match — %d/%d same cloud+region (%s/%s)",
                    len(same_cloud_region),
                    len(server_descriptions),
                    self._local_cloud,
                    self._local_atlas_region,
                )
                return same_cloud_region

        # Tier 2: same cloud, any region.
        same_cloud = [
            sd for sd in server_descriptions
            if (p := cache_snapshot.get(sd.address)) is not None
            and p.provider == self._local_cloud
        ]
        if same_cloud:
            log.debug(
                "CloudAffinityManager: tier-2 match — %d/%d same cloud (%s), any region",
                len(same_cloud),
                len(server_descriptions),
                self._local_cloud,
            )
            return same_cloud

        # Tier 3: no affinity match. Could mean fetches are still in flight,
        # all probes failed, or there genuinely is no same-cloud mongos.
        # Fall back to the full list so the operation can proceed.
        log.debug(
            "CloudAffinityManager: tier-3 fallback — no same-cloud (%s) mongos found, "
            "using all %d candidate(s)",
            self._local_cloud,
            len(server_descriptions),
        )
        return server_descriptions

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Shut down the background thread pool.

        Call this when you are done with the manager, or use it as a context
        manager (see __enter__ / __exit__ below) which calls close() for you.
        Waits for any in-flight fetches to complete before returning.
        """
        log.debug("CloudAffinityManager: shutting down fetch thread pool")
        self._executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


