"""What a YARN check reads. Thresholds arrive as arguments; nothing here reads config."""


def describe_node(node) -> str:
    report = f" — {node.health_report}" if node.health_report else ""
    return f"{node.id} ({node.state}{report})"


def cluster_master(client) -> dict:
    info = client.cluster_info()
    return {
        "state": info.get("state"),
        "ha_state": info.get("haState"),
        "rm_version": info.get("resourceManagerVersion"),
    }


def workers(client, *, minimum: int) -> dict:
    """Counts healthy NodeManagers and names the rest, so one message covers both."""
    nodes = client.nodes()
    healthy = [node for node in nodes if node.is_healthy]
    degraded = [node for node in nodes if not node.is_healthy]
    return {
        "running_nodes": len(healthy),
        "minimum": minimum,
        "node_ids": [node.id for node in healthy],
        "degraded": [describe_node(node) for node in degraded],
    }


def headroom(client, *, min_mb: int, min_vcores: int) -> dict:
    """Judged per node too: a container has to fit on one node, not on the total."""
    metrics = client.cluster_metrics()
    nodes = [node for node in client.nodes() if node.is_healthy]
    return {
        "cluster_available_mb": metrics.get("availableMB", 0),
        "cluster_allocated_mb": metrics.get("allocatedMB", 0),
        "cluster_available_vcores": metrics.get("availableVirtualCores", 0),
        "largest_node_available_mb": max((node.available_mb for node in nodes), default=0),
        "largest_node_available_vcores": max(
            (node.available_vcores for node in nodes), default=0
        ),
        "min_mb": min_mb,
        "min_vcores": min_vcores,
        "per_node": {node.id: [node.available_mb, node.available_vcores] for node in nodes},
    }


def running_apps(client, *, app_name: str) -> dict:
    """A pure reading, taken once: two readings could disagree inside one run."""
    apps = client.apps(name=app_name)
    return {
        "app_name": app_name,
        "apps": [
            {"id": app.id, "state": app.state, "elapsed_ms": app.elapsed_ms} for app in apps
        ],
        "running": [app.id for app in apps if app.is_running],
    }
