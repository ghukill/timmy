from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
from flask import current_app
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

DEFAULT_OPENSEARCH_REQUEST_TIMEOUT = 30
DEFAULT_OPENSEARCH_MAX_RETRIES = 3
LOCAL_OPENSEARCH_HOSTS = {"localhost", "opensearch"}
VALID_AUTH_SERVICE_TYPES = {"aoss", "es"}


def _normalize_endpoint(endpoint: str) -> tuple[str, int, bool]:
    """Normalize host, port, and TLS settings from an endpoint string."""
    parsed = urlparse(endpoint if "://" in endpoint else f"https://{endpoint}")
    host = parsed.hostname
    if not host:
        raise ValueError(f"Invalid OpenSearch endpoint: {endpoint!r}")

    if host in LOCAL_OPENSEARCH_HOSTS:
        return host, parsed.port or 9200, False

    return host, parsed.port or 443, True


def configure_opensearch_client(endpoint: str, auth_service_type: str = "es") -> OpenSearch:
    """Create an OpenSearch client for a local or AWS-managed endpoint."""
    host, port, use_ssl = _normalize_endpoint(endpoint)

    if host in LOCAL_OPENSEARCH_HOSTS:
        return OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=("admin", "admin"),
            use_ssl=use_ssl,
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            max_retries=DEFAULT_OPENSEARCH_MAX_RETRIES,
            retry_on_timeout=True,
            timeout=DEFAULT_OPENSEARCH_REQUEST_TIMEOUT,
        )

    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise RuntimeError("Could not locate AWS credentials for OpenSearch access.")

    region = os.getenv("AWS_REGION", "us-east-1")

    if auth_service_type not in VALID_AUTH_SERVICE_TYPES:
        valid = ", ".join(sorted(VALID_AUTH_SERVICE_TYPES))
        raise ValueError(
            "TIMDEX_OPENSEARCH_AUTH_SERVICE_TYPE must be one of "
            f"{valid}; got {auth_service_type!r}"
        )

    auth = AWSV4SignerAuth(
        credentials,
        region,
        service=auth_service_type,
    )
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=use_ssl,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        max_retries=DEFAULT_OPENSEARCH_MAX_RETRIES,
        retry_on_timeout=True,
        timeout=DEFAULT_OPENSEARCH_REQUEST_TIMEOUT,
    )


def get_opensearch_endpoint() -> str:
    """Return configured OpenSearch endpoint from Flask app config."""
    endpoint = current_app.config.get("TIMDEX_OPENSEARCH_ENDPOINT")
    if not endpoint:
        raise RuntimeError(
            "TIMDEX_OPENSEARCH_ENDPOINT is not configured. "
            "Set it in Flask config or via TIMMY_TIMDEX_OPENSEARCH_ENDPOINT."
        )
    return endpoint


def get_opensearch_auth_service_type() -> str:
    """Return configured OpenSearch auth service type from Flask app config."""
    return current_app.config.get("TIMDEX_OPENSEARCH_AUTH_SERVICE_TYPE", "es")


def get_opensearch_client() -> OpenSearch:
    """Return an OpenSearch client using Flask app config."""
    return configure_opensearch_client(
        get_opensearch_endpoint(),
        auth_service_type=get_opensearch_auth_service_type(),
    )
