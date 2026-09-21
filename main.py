# bq-export/main.py
"""Export BigQuery query results to GCS as CSV and optionally load them into CloudSQL (Postgres)."""
import argparse
import json
import logging
import os
import sys
import time
from contextlib import contextmanager

import duckdb
import google.auth
import google.auth.transport.requests
import sqlalchemy as sa
from google.cloud import bigquery, storage

from identifiers import quote_ident, split_table, sql_literal
from swap import (
    DEFAULT_LOCK_TIMEOUT,
    DEFAULT_SWAP_RETRIES,
    DEFAULT_SWAP_SUFFIX,
    TRANSIENT_SUFFIX,
    assert_swap_safe,
    capture_object_names,
    defer_indexes,
    rotate_object_names,
)

VALID_LOAD_MODES = {"truncate_append", "append", "swap"}

# The GCS -> Postgres leg runs through DuckDB: it reads the gzipped CSV exports
# straight from GCS and writes them into Postgres over the `postgres` extension,
# which bulk-loads via COPY underneath. Table DDL stays on the SQLAlchemy
# connection, where statements can be parameterised.
DUCKDB_EXTENSIONS = ("httpfs", "postgres")
PG_ALIAS = "pg"
GCS_SECRET_NAME = "gcs_adc"
GCS_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"

logger = logging.getLogger("bq_export")


@contextmanager
def log_duration(label: str):
    """Log how long the wrapped block takes to run."""
    start = time.monotonic()
    yield
    logger.info("%s took %.2f seconds", label, time.monotonic() - start)


def render_template(value: str, variables: dict[str, str]) -> str:
    for key, env_value in variables.items():
        value = value.replace(f"{{{{{key}}}}}", env_value)
    return value


def validate_load_mode(load_mode: str, rds_target: str | None) -> None:
    if load_mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"Invalid load_mode={load_mode!r} for rds_target={rds_target}. "
            f"Expected one of: {sorted(VALID_LOAD_MODES)}"
        )


def build_gcs_uri(config: dict, gcs_bucket_prefix: str, template_vars: dict[str, str]) -> str:
    gcs_uri = config.get("gcs_uri")
    if gcs_uri:
        uri = render_template(gcs_uri, template_vars)
    else:
        gcs_path = config.get("gcs_path")
        if not gcs_path:
            raise ValueError("Either gcs_uri or gcs_path is required.")
        uri = f"{gcs_bucket_prefix.rstrip('/')}/{gcs_path.lstrip('/')}"

    # Exports are gzip-compressed, so ensure the object is named accordingly.
    return uri if uri.endswith(".gz") else f"{uri}.gz"


def build_query(config: dict, template_vars: dict[str, str]) -> str:
    sql_file = config.get("sql_file")
    if sql_file and os.path.isfile(sql_file):
        with open(sql_file, "r", encoding="utf-8") as f:
            return render_template(f.read(), template_vars)

    bq_source = config.get("bigquery_source")
    if not bq_source:
        raise ValueError("bigquery_source is required if sql_file is not provided.")

    bq_source = render_template(bq_source, template_vars)
    return f"SELECT * FROM `{bq_source}`"


def export_to_gcs(
    bigquery_client: bigquery.Client,
    gcs_uri: str,
    query: str,
    header_flag: bool,
) -> int:
    """Run the query and export results to GCS. Returns the number of result rows.

    The row count comes for free from the query job stats (no extra query). When
    the query returns no rows we skip the GCS extract entirely.
    """
    job_config = bigquery.job.ExtractJobConfig(
        destination_format="CSV",
        compression=bigquery.Compression.GZIP,
        print_header=header_flag,
    )

    logger.info("Running query and exporting directly to GCS: %s", gcs_uri)
    logger.info("BigQuery CSV header enabled: %s", header_flag)

    with log_duration("Export to GCS"):
        query_job = bigquery_client.query(query, job_config=bigquery.QueryJobConfig())
        row_count = query_job.result().total_rows

        if not row_count:
            logger.warning("Query returned no rows; skipping GCS extract for %s", gcs_uri)
            return 0

        extract_job = bigquery_client.extract_table(
            query_job.destination,
            gcs_uri,
            job_config=job_config,
        )
        extract_job.result()

    return row_count


def list_export_blobs(gcs_uri: str) -> tuple[str, list]:
    client = storage.Client()
    bucket_name, prefix = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)

    blobs = [blob for blob in bucket.list_blobs(prefix=prefix) if not blob.name.endswith("/")]
    if not blobs:
        raise ValueError(f"No GCS files found for uri/prefix: {gcs_uri}")
    return bucket_name, blobs


def libpq_dsn(postgres_conn: str) -> str:
    """Convert the job's SQLAlchemy URL into a libpq keyword DSN for ATTACH.

    DuckDB passes the string to libpq unchanged, and libpq understands neither
    SQLAlchemy's `postgresql+driver://` prefix nor its query-string options.
    """
    url = sa.engine.make_url(postgres_conn)

    fields = {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
    }
    for key, value in url.query.items():
        # Multi-valued query params have no libpq equivalent; take the last.
        fields[key] = value[-1] if isinstance(value, (list, tuple)) else value

    parts = []
    for key, value in fields.items():
        if value is None or value == "":
            continue
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"{key}='{escaped}'")
    return " ".join(parts)


def open_duckdb(postgres_conn: str | None) -> duckdb.DuckDBPyConnection:
    """Open the in-process DuckDB used for the GCS -> Postgres leg.

    Resource limits are read from the environment because the job runs in a pod
    with a memory limit: left at its default DuckDB sizes itself against the
    host's RAM, which is not what the container is allowed to use.
    """
    settings = {
        "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT"),
        "threads": os.environ.get("DUCKDB_THREADS"),
        "temp_directory": os.environ.get("DUCKDB_TEMP_DIRECTORY"),
    }
    con = duckdb.connect(config={k: v for k, v in settings.items() if v})

    for extension in DUCKDB_EXTENSIONS:
        try:
            con.execute(f"LOAD {extension}")
        except duckdb.Error:
            # Not baked into the image — a bare `python main.py` outside the
            # container, or a container run as a UID whose home is not the one
            # the image installed into. Needs to reach DuckDB's repository.
            logger.info("DuckDB extension %s not preinstalled; installing", extension)
            con.execute(f"INSTALL {extension}")
            con.execute(f"LOAD {extension}")

    if postgres_conn:
        con.execute(f"ATTACH {sql_literal(libpq_dsn(postgres_conn))} AS {PG_ALIAS} (TYPE postgres)")

    return con


def refresh_gcs_credentials(con: duckdb.DuckDBPyConnection) -> None:
    """Give DuckDB a fresh OAuth token for GCS, minted from the job's own ADC.

    GCS accepts a bearer token on the XML API that DuckDB's httpfs speaks, so the
    pod's existing workload identity is enough and there are no HMAC keys to
    provision or rotate. Tokens last about an hour, so this runs per task rather
    than once at startup — a config with many large tables outlives one token.
    """
    credentials, _ = google.auth.default(scopes=[GCS_READ_SCOPE])
    credentials.refresh(google.auth.transport.requests.Request())
    con.execute(
        f"CREATE OR REPLACE SECRET {GCS_SECRET_NAME} (TYPE gcs, BEARER_TOKEN ?)",
        [credentials.token],
    )


def build_read_csv(uris: list[str], header_flag: bool) -> str:
    """Build the read_csv() call that stands in for `COPY ... FROM STDIN`.

    `all_varchar` is deliberate. It hands every field to Postgres as text and
    lets the target column type do the casting, which is exactly what COPY did.
    Letting DuckDB infer types instead would sample only the head of the file, so
    a zero-padded id whose first rows happen to look numeric could load as an
    integer and lose its padding.
    """
    files = ", ".join(sql_literal(uri) for uri in uris)
    return (
        f"read_csv([{files}]"
        f", header={'true' if header_flag else 'false'}"
        ", all_varchar=true"
        ", delim=','"
        ", quote='\"'"
        ", escape='\"'"
        ", compression='gzip')"
    )


def insert_from_gcs(
    con: duckdb.DuckDBPyConnection,
    uris: list[str],
    fq_target: str,
    header_flag: bool,
) -> int:
    """Load the GCS objects into an already-existing Postgres table. Returns rows written."""
    for uri in uris:
        logger.info("Reading %s", uri)

    result = con.execute(
        f"INSERT INTO {PG_ALIAS}.{fq_target} SELECT * FROM {build_read_csv(uris, header_flag)}"
    ).fetchone()
    return int(result[0]) if result else 0


@contextmanager
def duckdb_transaction(con: duckdb.DuckDBPyConnection):
    """Run the block in one DuckDB transaction, rolling back on failure.

    The attached Postgres database joins this transaction, so a mid-load failure
    leaves the target exactly as it was.
    """
    con.execute("BEGIN")
    try:
        yield
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error as exc:
            logger.warning("Rollback after a failed load did not complete: %s", exc)
        raise
    con.execute("COMMIT")


def gcs_to_cloudsql_import(
    duck: duckdb.DuckDBPyConnection,
    gcs_uri: str,
    rds_target: str,
    header_flag: bool,
    load_mode: str,
) -> None:
    validate_load_mode(load_mode, rds_target)

    bucket_name, blobs = list_export_blobs(gcs_uri)
    uris = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

    schema, table = split_table(rds_target)
    fq_target = f"{quote_ident(schema)}.{quote_ident(table)}"

    if load_mode == "truncate_append":
        logger.info("Begin truncate and import into %s from GCS", fq_target)
    else:
        logger.info("Begin append import into %s from GCS", fq_target)

    logger.info("Load mode: %s", load_mode)
    logger.info("CSV header enabled: %s", header_flag)

    refresh_gcs_credentials(duck)

    with log_duration("Import into CloudSQL"), duckdb_transaction(duck):
        if load_mode == "truncate_append":
            # Routed through postgres_execute rather than DuckDB's own TRUNCATE:
            # DuckDB rewrites TRUNCATE on an attached table into a DELETE, which
            # leaves every previous row behind as a dead tuple for VACUUM to
            # reclaim. postgres_execute runs on the same Postgres session as the
            # INSERT below, so this is still a single transaction and readers keep
            # seeing the old data until it commits.
            truncate = sql_literal(f"TRUNCATE TABLE {fq_target}")
            duck.execute(f"CALL postgres_execute({PG_ALIAS}, {truncate})")

        rows = insert_from_gcs(duck, uris, fq_target, header_flag)

    logger.info("Loaded %d rows into %s", rows, fq_target)


def gcs_to_cloudsql_swap(
    postgres_engine: sa.Engine,
    duck: duckdb.DuckDBPyConnection,
    gcs_uri: str,
    rds_target: str,
    header_flag: bool,
    config: dict,
) -> None:
    """Load the staging twin, then rotate the two tables' names.

    Unlike truncate_append — which holds an ACCESS EXCLUSIVE lock on the target for
    the entire load — readers keep querying the live table throughout and are only
    blocked for the rotation, which is a catalog update measured in milliseconds.

    Both tables are permanent (see init/), so nothing is created or dropped here.
    The rotation exchanges their names, which leaves the twin holding the data the
    live table had: it becomes the next run's staging table.
    """
    swap_suffix = config.get("swap_suffix", DEFAULT_SWAP_SUFFIX)
    lock_timeout = config.get("swap_lock_timeout", DEFAULT_LOCK_TIMEOUT)
    retries = int(config.get("swap_retries", DEFAULT_SWAP_RETRIES))
    build_indexes_after_load = config.get("swap_defer_indexes", True)
    allow_unsafe = config.get("allow_unsafe_swap", False)

    schema, table = split_table(rds_target)
    staging, transient = f"{table}{swap_suffix}", f"{table}{TRANSIENT_SUFFIX}"
    fq_target = f"{quote_ident(schema)}.{quote_ident(table)}"
    fq_staging = f"{quote_ident(schema)}.{quote_ident(staging)}"
    fq_transient = f"{quote_ident(schema)}.{quote_ident(transient)}"

    bucket_name, blobs = list_export_blobs(gcs_uri)
    uris = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

    logger.info("Begin swap load into %s via %s", fq_target, fq_staging)
    logger.info("Load mode: swap")
    logger.info("CSV header enabled: %s", header_flag)

    with postgres_engine.connect() as conn:
        # Everything here runs before a single row is loaded, so an unrotatable
        # target fails fast instead of after the export and the load.
        with conn.begin():
            assert_swap_safe(conn, fq_target, fq_staging, allow_unsafe)

            # The rotation parks the live name here mid-transaction. An earlier
            # version of this job left a table under a similar name when a run
            # crashed, and anything sitting on the name now would fail the second
            # rename with the data already loaded.
            if conn.execute(
                sa.text("SELECT to_regclass(:t)"), {"t": fq_transient}
            ).scalar() is not None:
                raise ValueError(
                    f"{fq_transient} already exists. swap mode needs that name free to "
                    f"rotate {fq_target} and {fq_staging}. It is not created by this job, "
                    "so drop it once you have confirmed it holds nothing you need."
                )

            live_names = capture_object_names(conn, fq_target)
            twin_names = capture_object_names(conn, fq_staging)

            # The twin is permanent, so it still holds the rows from the run before
            # last. Readers are on the live table, so this is not visible to them.
            conn.execute(sa.text(f"TRUNCATE TABLE {fq_staging}"))

            rebuild = defer_indexes(conn, schema, staging) if build_indexes_after_load else []
            if rebuild:
                logger.info("Deferred %d index/constraint build(s) until after the load", len(rebuild))

        # The truncate above was committed on the SQLAlchemy connection, so DuckDB's
        # separate session needs its cached copy of the Postgres catalog dropped.
        refresh_gcs_credentials(duck)
        duck.execute("CALL pg_clear_cache()")

        with log_duration("Load into staging table"), duckdb_transaction(duck):
            rows = insert_from_gcs(duck, uris, fq_staging, header_flag)

        logger.info("Loaded %d rows into %s", rows, fq_staging)

        if rebuild:
            with log_duration("Rebuild indexes on staging table"), conn.begin():
                for statement in rebuild:
                    conn.execute(sa.text(statement))

        # Stats live on the relation, which the rotation moves wholesale, so
        # analysing here means the newly live table has a usable plan from its
        # first query.
        with log_duration("Analyze staging table"), conn.begin():
            conn.execute(sa.text(f"ANALYZE {fq_staging}"))

        for attempt in range(1, retries + 1):
            try:
                with log_duration("Swap"), conn.begin():
                    # Bounded so a long-running reader delays the rotation instead of
                    # queueing every other query behind our ACCESS EXCLUSIVE lock.
                    conn.execute(sa.text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
                    conn.execute(sa.text(f"ALTER TABLE {fq_target} RENAME TO {quote_ident(transient)}"))
                    conn.execute(sa.text(f"ALTER TABLE {fq_staging} RENAME TO {quote_ident(table)}"))
                    conn.execute(sa.text(f"ALTER TABLE {fq_transient} RENAME TO {quote_ident(staging)}"))
                    # Inside the same transaction, so the tables are never visible
                    # carrying each other's index names.
                    rotate_object_names(conn, schema, table, staging, live_names, twin_names)
                return
            except sa.exc.OperationalError as exc:
                if attempt == retries:
                    raise
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Swap attempt %d/%d could not take the lock (%s); retrying in %ds",
                    attempt,
                    retries,
                    exc.orig,
                    backoff,
                )
                time.sleep(backoff)


def copy_to_target(
    bigquery_client: bigquery.Client,
    postgres_engine: sa.Engine | None,
    duck: duckdb.DuckDBPyConnection,
    gcs_uri: str,
    rds_target: str | None,
    query: str,
    gcs_export_only: bool,
    header_flag: bool,
    load_mode: str,
    config: dict,
) -> None:
    row_count = export_to_gcs(bigquery_client, gcs_uri, query, header_flag)
    logger.info("Exported %d rows to %s", row_count, gcs_uri)

    if gcs_export_only:
        return

    if not row_count:
        logger.warning(
            "No rows to import into %s; skipping %s to avoid emptying/altering the table.",
            rds_target,
            load_mode,
        )
        return

    if not postgres_engine:
        raise ValueError("A postgres connection is required when gcs_export_only=false.")
    if not rds_target:
        raise ValueError("rds_target is required when importing into CloudSQL.")

    if load_mode == "swap":
        gcs_to_cloudsql_swap(postgres_engine, duck, gcs_uri, rds_target, header_flag, config)
    else:
        gcs_to_cloudsql_import(duck, gcs_uri, rds_target, header_flag, load_mode)
    logger.info("Imported into %s", rds_target)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--postgres_conn", required=False, help="Postgres connection string")
    return parser.parse_args(argv)


def run(config_path: str, postgres_conn: str | None) -> None:
    bq_project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    gcs_bucket_prefix = os.environ["GCS_BUCKET_PREFIX"]

    template_vars = {
        "BQ_PROJECT_ID": bq_project_id,
        "GCS_BUCKET_PREFIX": gcs_bucket_prefix.rstrip("/"),
    }

    with open(config_path, "r", encoding="utf-8") as read_json:
        configs = json.load(read_json)

    bigquery_client = bigquery.Client(project=bq_project_id)
    postgres_engine = sa.create_engine(postgres_conn) if postgres_conn else None
    duck = open_duckdb(postgres_conn)

    try:
        for config in configs:
            gcs_uri = build_gcs_uri(config, gcs_bucket_prefix, template_vars)
            gcs_export_only = config.get("gcs_export_only", False)
            rds_target = config.get("rds_target")

            # Default keeps existing behavior for backward compatibility.
            #   truncate_append: TRUNCATE target table, then COPY data
            #   append:          COPY data directly without truncating
            #   swap:            COPY into a staging clone, then rename it over the target
            load_mode = config.get("load_mode", "truncate_append")
            validate_load_mode(load_mode, rds_target)

            # Default to True so BigQuery exports a header row and Postgres COPY
            # expects one. This prevents COPY from treating the first data row as
            # a header (or vice versa) and silently dropping a row.
            header_flag = config.get("header_flag", True)

            query = build_query(config, template_vars)

            copy_to_target(
                bigquery_client,
                postgres_engine,
                duck,
                gcs_uri,
                rds_target,
                query,
                gcs_export_only,
                header_flag,
                load_mode,
                config,
            )
    finally:
        duck.close()
        if postgres_engine is not None:
            postgres_engine.dispose()

    logger.info("All tasks completed.")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    try:
        run(args.config, args.postgres_conn)
    except Exception:
        logger.exception("Export failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
