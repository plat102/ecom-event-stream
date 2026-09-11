"""Registers the monitoring hooks for Admin -> Plugins; Airflow 2 has no such step for
operators, which are imported from this directory directly."""
from airflow.plugins_manager import AirflowPlugin
from hooks.kafka_admin import KafkaAdminHook
from hooks.warehouse import WarehouseHook
from hooks.yarn import YarnHook


class MonitoringPlugin(AirflowPlugin):
    name = "ecom_monitoring"
    hooks = [KafkaAdminHook, YarnHook, WarehouseHook]
