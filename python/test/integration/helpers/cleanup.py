"""Cloud resource cleanup helpers for integration tests."""

import logging

logger = logging.getLogger(__name__)


class GCPCleanup:
    """Delete BigQuery tables whose names contain a given suffix.

    Parameters
    ----------
    project : str
        GCP project id, e.g. ``"canary-443022"``.
    dataset : str
        BigQuery dataset, e.g. ``"data"``.
    """

    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset

    def cleanup_tables(self, suffix: str) -> list[str]:
        """Remove all tables in *dataset* whose name contains *suffix*.

        Returns a list of deleted table ids.
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project)
        dataset_ref = f"{self.project}.{self.dataset}"
        deleted: list[str] = []

        for table in client.list_tables(dataset_ref):
            if suffix in table.table_id:
                full_id = f"{dataset_ref}.{table.table_id}"
                logger.info("Deleting BQ table %s", full_id)
                client.delete_table(full_id, not_found_ok=True)
                deleted.append(table.table_id)

        return deleted


class DataprocFlinkCleanup:
    """Cancel Dataproc Flink jobs by job ID."""

    def __init__(self, project: str, region: str):
        self.project = project
        self.region = region

    def cancel_jobs(self, job_ids: list[str]) -> list[str]:
        from google.cloud import dataproc_v1

        client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )
        cancelled = []
        for job_id in job_ids:
            try:
                logger.info("Cancelling Dataproc Flink job %s", job_id)
                client.cancel_job(project_id=self.project, region=self.region, job_id=job_id)
                cancelled.append(job_id)
            except Exception:
                logger.exception("Failed to cancel Dataproc Flink job %s", job_id)
        return cancelled


class AzureCleanup:
    """Stub for Azure table cleanup (Snowflake/Iceberg targets)."""

    def __init__(self, catalog: str):
        self.catalog = catalog

    def cleanup_tables(self, suffix: str) -> list[str]:
        # TODO: implement Snowflake-based cleanup for Azure canary tables
        logger.warning("Azure table cleanup not yet implemented for suffix=%s", suffix)
        return []


class DatabricksCleanup:
    """Delete Unity Catalog tables whose names contain a given suffix.

    Used by UC-backed canary teams (e.g. ``aws_databricks``) whose output
    tables live in Databricks UC, not Glue. Authenticates via OAuth
    client_credentials.

    Skips silently if ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``
    are not set so local pytest runs without UC creds don't fail teardown.

    Parameters
    ----------
    host : str
        Databricks workspace base URL (no trailing slash), e.g.
        ``"https://dbc-050d6f00-dcb3.cloud.databricks.com"``.
    warehouse_id : str
        Serverless SQL warehouse id used to run ``SHOW TABLES`` / ``DROP TABLE``.
    schemas : list[str]
        Fully-qualified ``<catalog>.<schema>`` namespaces to scan for matching
        tables (e.g. ``["workspace.poc"]``).
    """

    OAUTH_PATH = "/oidc/v1/token"
    SQL_STATEMENTS_PATH = "/api/2.0/sql/statements"

    def __init__(self, host: str, warehouse_id: str, schemas: list[str]):
        self.host = host.rstrip("/")
        self.warehouse_id = warehouse_id
        self.schemas = schemas

    def cleanup_tables(self, suffix: str) -> list[str]:
        import os

        import requests

        client_id = os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
        if not (client_id and client_secret):
            logger.warning(
                "DATABRICKS_CLIENT_ID/SECRET not set; skipping UC cleanup for suffix=%s",
                suffix,
            )
            return []

        token_resp = requests.post(
            self.host + self.OAUTH_PATH,
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            auth=(client_id, client_secret),
            timeout=30,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        deleted: list[str] = []
        for schema in self.schemas:
            for table in self._list_tables(schema, headers):
                if suffix in table:
                    fqn = f"{schema}.{table}"
                    logger.info("Dropping UC table %s", fqn)
                    self._run_sql(f"DROP TABLE IF EXISTS {fqn}", headers)
                    deleted.append(table)
        return deleted

    def _list_tables(self, schema: str, headers: dict) -> list[str]:
        result = self._run_sql(f"SHOW TABLES IN {schema}", headers)
        rows = (result.get("result") or {}).get("data_array") or []
        return [row[1] for row in rows if len(row) >= 2]

    def _run_sql(self, statement: str, headers: dict) -> dict:
        import time

        import requests

        post = requests.post(
            self.host + self.SQL_STATEMENTS_PATH,
            json={
                "warehouse_id": self.warehouse_id,
                "statement": statement,
                "wait_timeout": "30s",
            },
            headers=headers,
            timeout=60,
        )
        post.raise_for_status()
        data = post.json()
        statement_id = data["statement_id"]
        deadline = time.time() + 120
        while data["status"]["state"] in ("PENDING", "RUNNING"):
            if time.time() > deadline:
                raise RuntimeError(f"UC statement {statement_id} did not terminate within 120s")
            time.sleep(2)
            get = requests.get(
                self.host + self.SQL_STATEMENTS_PATH + f"/{statement_id}",
                headers=headers,
                timeout=30,
            )
            get.raise_for_status()
            data = get.json()
        if data["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(
                f"UC statement {statement_id} failed: {data['status']}"
            )
        return data


class AWSCleanup:
    """Delete AWS Glue tables whose names contain a given suffix.

    Parameters
    ----------
    database : str
        Glue catalog database name.
    """

    def __init__(self, database: str):
        self.database = database

    def cleanup_tables(self, suffix: str) -> list[str]:
        """Remove all Glue tables in *database* whose name contains *suffix*.

        Returns a list of deleted table names.
        """
        import os

        import boto3

        region = os.environ.get("AWS_REGION", "us-west-2")
        client = boto3.client("glue", region_name=region)
        deleted: list[str] = []

        paginator = client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=self.database):
            for table in page["TableList"]:
                if suffix in table["Name"]:
                    logger.info("Deleting Glue table %s.%s", self.database, table["Name"])
                    client.delete_table(DatabaseName=self.database, Name=table["Name"])
                    deleted.append(table["Name"])

        return deleted
