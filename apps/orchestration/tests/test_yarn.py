"""Unit tests for the YARN REST client: response parsing, and the states that must not read
as healthy."""
from shared.connectors.yarn import YarnClient

APP_NAME = "ecom-stream-processor"


class StubYarn(YarnClient):
    """Replaces the one HTTP call with canned ResourceManager payloads."""

    def __init__(self, payloads: dict) -> None:
        super().__init__("http://resourcemanager:8088")
        self._payloads = payloads
        self.requested: list[tuple[str, dict | None]] = []

    def _get(self, path: str, params: dict | None = None) -> dict:
        self.requested.append((path, params))
        return self._payloads[path]


def _app(app_id: str, state: str, name: str = APP_NAME) -> dict:
    return {
        "id": app_id,
        "name": name,
        "state": state,
        "elapsedTime": 60000,
        "allocatedMB": 3072,
        "allocatedVCores": 2,
    }


def _apps(*entries: dict) -> dict:
    return {"/ws/v1/cluster/apps": {"apps": {"app": list(entries)} if entries else None}}


# ── nodes ─────────────────────────────────────────────────────────────


def test_nodes_parses_resource_headroom():
    client = StubYarn(
        {
            "/ws/v1/cluster/nodes": {
                "nodes": {
                    "node": [
                        {
                            "id": "nodemanager1:35139",
                            "state": "RUNNING",
                            "availMemoryMB": 5120,
                            "usedMemoryMB": 3072,
                            "availableVirtualCores": 6,
                            "healthReport": "",
                        }
                    ]
                }
            }
        }
    )
    node = client.nodes()[0]
    assert (node.available_mb, node.used_mb, node.available_vcores) == (5120, 3072, 6)
    assert node.is_healthy


def test_unhealthy_node_is_not_healthy_and_keeps_its_report():
    # the job stays RUNNING on an UNHEALTHY node, so this is the only place it shows
    client = StubYarn(
        {
            "/ws/v1/cluster/nodes": {
                "nodes": {
                    "node": [
                        {
                            "id": "nodemanager1:35139",
                            "state": "UNHEALTHY",
                            "availMemoryMB": 0,
                            "usedMemoryMB": 0,
                            "availableVirtualCores": 0,
                            "healthReport": "1/1 local-dirs are bad",
                        }
                    ]
                }
            }
        }
    )
    node = client.nodes()[0]
    assert not node.is_healthy
    assert node.health_report == "1/1 local-dirs are bad"


def test_empty_cluster_serializes_as_null_not_a_list():
    client = StubYarn({"/ws/v1/cluster/nodes": {"nodes": None}})
    assert client.nodes() == []


# ── apps ──────────────────────────────────────────────────────────────


def test_accepted_app_is_not_running():
    # an app over the queue's AM limit parks in ACCEPTED forever without raising
    client = StubYarn(_apps(_app("application_1_0001", "ACCEPTED")))
    assert client.apps() and client.running_apps() == []


def test_running_apps_filters_by_name():
    client = StubYarn(
        _apps(
            _app("application_1_0001", "RUNNING"),
            _app("application_1_0002", "RUNNING", name="other"),
        )
    )
    assert [app.id for app in client.running_apps(APP_NAME)] == ["application_1_0001"]


def test_two_running_apps_with_the_same_name_are_both_returned():
    # two streaming jobs cannot coexist, so the caller has to be able to see both
    client = StubYarn(
        _apps(_app("application_1_0001", "RUNNING"), _app("application_1_0002", "RUNNING"))
    )
    assert len(client.running_apps(APP_NAME)) == 2


def test_no_apps_at_all():
    assert StubYarn(_apps()).running_apps(APP_NAME) == []


def test_apps_query_asks_for_spark_applications_only():
    client = StubYarn(_apps())
    client.apps()
    path, params = client.requested[0]
    assert path == "/ws/v1/cluster/apps"
    assert params == {"states": "RUNNING,ACCEPTED", "applicationTypes": "SPARK"}
