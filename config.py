"""Configuration loaded from environment variables (see .env.example).

Kept deliberately small and dependency-free so an architect can read the whole
contract in one screen. For a production job, swap `_get` for reads against your
approved secret store instead of a flat .env file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get(name: str, default: str | None = None, required: bool = False) -> str | None:
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


@dataclass
class SnowflakeConfig:
    # If connection_name is set, it names an entry in ~/.snowflake/connections.toml
    # and the account/user/auth fields below are optional (pulled from the file).
    connection_name: str | None
    account: str | None
    user: str | None
    auth_method: str            # "pat" | "keypair"
    pat: str | None
    private_key_path: str | None
    private_key_passphrase: str | None
    role: str | None
    warehouse: str | None
    database: str | None
    schema: str | None

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        conn_name = _get("SF_CONNECTION_NAME")
        auth = (_get("SF_AUTH_METHOD", "pat") or "pat").lower()
        cfg = cls(
            connection_name=conn_name,
            account=_get("SF_ACCOUNT", required=not conn_name),
            user=_get("SF_USER", required=not conn_name),
            auth_method=auth,
            pat=_get("SF_PAT"),
            private_key_path=_get("SF_PRIVATE_KEY_PATH"),
            private_key_passphrase=_get("SF_PRIVATE_KEY_PASSPHRASE"),
            role=_get("SF_ROLE"),
            warehouse=_get("SF_WAREHOUSE"),
            database=_get("SF_DATABASE"),
            schema=_get("SF_SCHEMA"),
        )
        if conn_name:
            return cfg  # credentials come from connections.toml
        if cfg.auth_method == "pat" and not cfg.pat:
            raise RuntimeError("SF_AUTH_METHOD=pat but SF_PAT is not set")
        if cfg.auth_method == "keypair" and not cfg.private_key_path:
            raise RuntimeError("SF_AUTH_METHOD=keypair but SF_PRIVATE_KEY_PATH is not set")
        return cfg


@dataclass
class TargetConfig:
    kind: str                   # "mysql" | "mssql"
    host: str
    port: int
    database: str
    user: str
    password: str
    odbc_driver: str | None     # mssql only

    @classmethod
    def from_env(cls) -> "TargetConfig":
        kind = (_get("TARGET_KIND", required=True) or "").lower()
        if kind not in ("mysql", "mssql"):
            raise RuntimeError(f"TARGET_KIND must be 'mysql' or 'mssql', got {kind!r}")
        default_port = "3306" if kind == "mysql" else "1433"
        return cls(
            kind=kind,
            host=_get("TARGET_HOST", required=True),
            port=int(_get("TARGET_PORT", default_port)),
            database=_get("TARGET_DATABASE", required=True),
            user=_get("TARGET_USER", required=True),
            password=_get("TARGET_PASSWORD", required=True),
            odbc_driver=_get("TARGET_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
        )
