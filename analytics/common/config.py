"""One configuration boundary for Spark, Airflow and bootstrap scripts."""

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def settings():
    path = Path(os.environ.get("ANALYTICS_CONFIG", ROOT / "config/analytics.yaml"))
    cfg = yaml.safe_load(path.read_text())
    for key, current in list(cfg.items()):
        value = os.environ.get("ANALYTICS_" + key.upper())
        if value is not None:
            cfg[key] = yaml.safe_load(value) if isinstance(current, (int, bool)) else value
    cfg["access_key"] = os.environ.get("MINIO_ROOT_USER", "tikblog")
    cfg["secret_key"] = os.environ.get("MINIO_ROOT_PASSWORD", "tikblog-local")
    if cfg["vacuum_retention_hours"] < 168:
        raise ValueError("VACUUM retention must be at least 168 hours")
    if cfg["merge_attempts"] < 1:
        raise ValueError("merge_attempts must be positive")
    return cfg


def monitoring():
    path = os.environ.get("MONITORING_CONFIG", str(ROOT / "config/monitoring.yaml"))
    return yaml.safe_load(Path(path).read_text())


def location(cfg, table):
    return f"s3a://{cfg['bucket']}/{table.replace('.', '/')}"


def checkpoint(cfg, pipeline):
    return f"s3a://{cfg['bucket']}/{cfg['checkpoints'][pipeline]}"


def status(value, thresholds):
    if thresholds is None or value is None:
        return "UNKNOWN"
    if value >= thresholds["red"]:
        return "RED"
    if value >= thresholds["yellow"]:
        return "YELLOW"
    return "GREEN"
