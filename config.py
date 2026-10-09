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
    statement_timeout: int | None = None   # STATEMENT_TIMEOUT_IN_SECONDS for the session

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
            statement_timeout=_get_int("SF_STATEMENT_TIMEOUT_SECONDS"),
        )
        if conn_name:
            return cfg  # credentials come from connections.toml
        if cfg.auth_method == "pat" and not cfg.pat:
            raise RuntimeError("SF_AUTH_METHOD=pat but SF_PAT is not set")
        if cfg.auth_method == "keypair" and not cfg.private_key_path:
            raise RuntimeError("SF_AUTH_METHOD=keypair but SF_PRIVATE_KEY_PATH is not set")
        return cfg


def _get_bool(name: str, default: bool) -> bool:
    val = _get(name)
    if val is None or val == "":
        return default
    if val.strip().lower() in ("1", "true", "yes", "y", "on"):
        return True
    if val.strip().lower() in ("0", "false", "no", "n", "off"):
        return False
    raise RuntimeError(f"{name} must be yes or no, got {val!r}")


def _get_int(name: str) -> int | None:
    """A positive integer, or None when unset."""
    val = _get(name)
    if val is None or val.strip() == "":
        return None
    if not val.strip().isdigit() or int(val) <= 0:
        raise RuntimeError(f"{name} must be a positive whole number, got {val!r}")
    return int(val)


@dataclass
class TargetConfig:
    kind: str                   # "mysql" | "mssql"
    host: str
    port: int
    database: str
    user: str
    password: str
    statement_timeout: int | None = None    # seconds a target statement may wait
    # --- MySQL only ----------------------------------------------------------
    mysql_ssl_ca: str | None = None         # CA file: verify the server certificate
    mysql_ssl_verify: bool = True           # require a verified certificate
    mysql_ssl_verify_identity: bool = True  # with a CA: also check the host name
    # --- SQL Server only -----------------------------------------------------
    odbc_driver: str | None = None
    mssql_encrypt: bool = True              # TLS to SQL Server
    mssql_trust_server_cert: bool = False   # True skips certificate validation
    mssql_load_method: str = "bulk_insert"  # Transport B: "bulk_insert" | "client"
    mssql_bulk_dir: str | None = None       # --local-dir as SQL Server sees it

    @classmethod
    def from_env(cls) -> "TargetConfig":
        kind = (_get("TARGET_KIND", required=True) or "").lower()
        if kind not in ("mysql", "mssql"):
            raise RuntimeError(f"TARGET_KIND must be 'mysql' or 'mssql', got {kind!r}")
        default_port = "3306" if kind == "mysql" else "1433"
        load_method = (_get("TARGET_MSSQL_LOAD_METHOD", "bulk_insert") or "").lower()
        if load_method not in ("bulk_insert", "client"):
            raise RuntimeError(
                f"TARGET_MSSQL_LOAD_METHOD must be 'bulk_insert' or 'client', got {load_method!r}")
        return cls(
            kind=kind,
            host=_get("TARGET_HOST", required=True),
            port=int(_get("TARGET_PORT", default_port)),
            database=_get("TARGET_DATABASE", required=True),
            user=_get("TARGET_USER", required=True),
            password=_get("TARGET_PASSWORD", required=True),
            statement_timeout=_get_int("TARGET_STATEMENT_TIMEOUT_SECONDS"),
            mysql_ssl_ca=_get("TARGET_MYSQL_SSL_CA") or None,
            mysql_ssl_verify=_get_bool("TARGET_MYSQL_SSL_VERIFY", True),
            mysql_ssl_verify_identity=_get_bool("TARGET_MYSQL_SSL_VERIFY_IDENTITY", True),
            odbc_driver=_get("TARGET_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
            mssql_encrypt=_get_bool("TARGET_MSSQL_ENCRYPT", True),
            mssql_trust_server_cert=_get_bool("TARGET_MSSQL_TRUST_SERVER_CERT", False),
            mssql_load_method=load_method,
            mssql_bulk_dir=_get("TARGET_MSSQL_BULK_DIR"),
        )
