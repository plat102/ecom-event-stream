"""YARN ResourceManager REST client: cluster state, node health, headroom, app state.
Read-only — nothing here submits. stdlib urllib, so callers need no HTTP library.
"""
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

# The only state that means "processing data" — ACCEPTED can park forever, silently.
STATE_RUNNING = "RUNNING"


@dataclass(frozen=True)
class YarnNode:
    id: str
    state: str
    available_mb: int
    used_mb: int
    available_vcores: int
    health_report: str

    @property
    def is_healthy(self) -> bool:
        return self.state == "RUNNING"


@dataclass(frozen=True)
class YarnApp:
    id: str
    name: str
    state: str
    elapsed_ms: int
    allocated_mb: int
    allocated_vcores: int

    @property
    def is_running(self) -> bool:
        return self.state == STATE_RUNNING


class YarnClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base_url}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=self._timeout) as response:
            return json.loads(response.read())

    def cluster_info(self) -> dict:
        """ResourceManager's own state: `state` STARTED and `haState` ACTIVE when serving."""
        return self._get("/ws/v1/cluster/info")["clusterInfo"]

    def cluster_metrics(self) -> dict:
        """Cluster totals — `availableMB`, `allocatedMB`, node counts."""
        return self._get("/ws/v1/cluster/metrics")["clusterMetrics"]

    def nodes(self) -> list[YarnNode]:
        payload = self._get("/ws/v1/cluster/nodes")["nodes"]
        # An empty cluster serializes as null rather than an empty list.
        entries = payload.get("node", []) if payload else []
        return [
            YarnNode(
                id=entry["id"],
                state=entry["state"],
                available_mb=entry.get("availMemoryMB", 0),
                used_mb=entry.get("usedMemoryMB", 0),
                available_vcores=entry.get("availableVirtualCores", 0),
                health_report=entry.get("healthReport", ""),
            )
            for entry in entries
        ]

    def apps(
        self, name: str | None = None, states: tuple[str, ...] = ("RUNNING", "ACCEPTED")
    ) -> list[YarnApp]:
        payload = self._get(
            "/ws/v1/cluster/apps",
            {"states": ",".join(states), "applicationTypes": "SPARK"},
        )["apps"]
        entries = payload.get("app", []) if payload else []
        apps = [
            YarnApp(
                id=entry["id"],
                name=entry["name"],
                state=entry["state"],
                elapsed_ms=entry.get("elapsedTime", 0),
                allocated_mb=entry.get("allocatedMB", 0),
                allocated_vcores=entry.get("allocatedVCores", 0),
            )
            for entry in entries
        ]
        return [app for app in apps if name is None or app.name == name]

    def running_apps(self, name: str | None = None) -> list[YarnApp]:
        """RUNNING only: an ACCEPTED application is not processing anything yet."""
        return [app for app in self.apps(name=name) if app.is_running]
