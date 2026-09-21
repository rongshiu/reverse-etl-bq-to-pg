# bq-export

A small, config-driven ETL job that exports BigQuery data to Google Cloud Storage and
loads it into CloudSQL (Postgres) with DuckDB.

Each run reads a JSON config describing one or more **export tasks**. For every task the
job:

```
BigQuery query ──▶ extract to GCS (CSV.gz) ──▶ DuckDB reads GCS, writes Postgres
   (sql_file or                                        (optional)
    SELECT * source)
```

The Postgres load is optional — set `gcs_export_only: true` to stop after the GCS export.

> In production this job is scheduled on Airflow-EKS via the Kubernetes Operator. The
> guide below covers configuration and local execution.

---

## Quick start

Run a local Postgres and execute a config against it:

```sh
# Authenticate so the container can reach BigQuery + GCS
gcloud auth application-default login

# Build and run (edit the --config target in docker-compose.yaml first)
docker compose up --build
```

This starts a Postgres container, waits for it to be healthy, then runs the export.

---

## Configuration

`--config` points to a JSON file containing a **list** of task objects. Each task:

| Field             | Required          | Default            | Description                                                              |
| ----------------- | ----------------- | ------------------ | ------------------------------------------------------------------------ |
| `sql_file`        | one of these two¹ | —                  | Path to a `.sql` file to run. Rendered with template vars before running.|
| `bigquery_source` | one of these two¹ | —                  | Fully-qualified table; runs `SELECT * FROM \`<source>\`` when no `sql_file`. |
| `gcs_uri`         | one of these two² | —                  | Full destination URI (`gs://bucket/path.csv`). Rendered with template vars. |
| `gcs_path`        | one of these two² | —                  | Path appended to `GCS_BUCKET_PREFIX` to form the destination URI.        |
| `rds_target`      | when loading      | —                  | Target Postgres table, e.g. `public.my_table`.                           |
| `gcs_export_only` | no                | `false`            | If `true`, skip the Postgres load.                                       |
| `load_mode`       | no                | `truncate_append`  | `truncate_append` = TRUNCATE then COPY · `append` = COPY without truncating · `swap` = [load into a clone, then rename](#swap-mode-near-zero-downtime-reloads). |
| `header_flag`     | no                | `true`             | Whether the CSV carries a header row (BigQuery writes one / the load expects one). |

¹ Exactly one of `sql_file` or `bigquery_source` per task.
² Exactly one of `gcs_uri` or `gcs_path` per task.

### Template variables

`sql_file` contents, `bigquery_source`, and `gcs_uri` support `{{VAR}}` substitution:

| Token                  | Source                  |
| ---------------------- | ----------------------- |
| `{{BQ_PROJECT_ID}}`    | `GOOGLE_CLOUD_PROJECT`  |
| `{{GCS_BUCKET_PREFIX}}`| `GCS_BUCKET_PREFIX`     |

### Examples

Run a SQL file and load into Postgres:

```json
[
  {
    "sql_file": "sql/ai_insights_dump.sql",
    "gcs_path": "bq-export/ai_insight.csv",
    "rds_target": "public.ai_insight",
    "load_mode": "truncate_append"
  }
]
```

Reload a large table with near-zero downtime:

```json
[
  {
    "sql_file": "sql/cdphub/main_customer_overview_report_fact.sql",
    "gcs_path": "bq-export/main_customer_overview_report_fact.csv",
    "rds_target": "public.main_customer_overview_report_fact",
    "load_mode": "swap"
  }
]
```

Dump a whole table to GCS only (no Postgres load):

```json
[
  {
    "bigquery_source": "{{BQ_PROJECT_ID}}.stg_aeon_ab.stg_ab__customer_profile",
    "gcs_uri": "{{GCS_BUCKET_PREFIX}}/stg_aeon_ab/stg_ab__customer_profile.csv",
    "gcs_export_only": true
  }
]
```

---

## The GCS → Postgres load (DuckDB)

The BigQuery → GCS extract is unchanged: BigQuery writes gzipped CSV. The load leg
then runs in-process through DuckDB, which reads those objects straight from GCS and
writes into Postgres over the `postgres` extension (which bulk-loads via `COPY`
underneath). Table DDL — the whole of swap mode — stays on the SQLAlchemy connection.

### GCS credentials

No HMAC keys. DuckDB's `httpfs` can authenticate to GCS with an OAuth bearer token, so
the job mints a short-lived one from its own ADC / workload identity:

```python
credentials, _ = google.auth.default(scopes=[GCS_READ_SCOPE])
```

Tokens last about an hour, so the secret is refreshed per task rather than once at
startup — a config with many large tables outlives a single token. Do **not** switch this
to `PROVIDER credential_chain`: for GCS that falls through to the *AWS* credential chain
and will pick up unrelated AWS credentials if any are present.

### Two deliberate choices

**`all_varchar=true` on every read.** Every field is handed to Postgres as text and the
target column type does the casting — exactly what `COPY` did. Letting DuckDB infer types
instead would sample only the head of the file, so a zero-padded `member_id` whose first
rows happen to look numeric would load as an integer and lose its padding.

**`TRUNCATE` is issued via `postgres_execute`, not DuckDB's own `TRUNCATE`.** DuckDB
accepts `TRUNCATE` on an attached table but rewrites it into a `DELETE`, which leaves
every previous row behind as a dead tuple (measured: ~2x the heap size after a reload).
`postgres_execute` runs on the same Postgres session as the `INSERT`, so the truncate and
the load remain a single transaction — readers see the old data until it commits, and a
failed load rolls the truncate back.

### Environment

| Variable | Required | Description |
| -------- | -------- | ----------- |
| `DUCKDB_MEMORY_LIMIT` | no | e.g. `2GB`. Left unset DuckDB sizes itself against the *host's* RAM, not the pod's limit, which risks an OOMKill. |
| `DUCKDB_THREADS` | no | Cap CPU use to the pod's request. |
| `DUCKDB_TEMP_DIRECTORY` | no | Writable path for spilling. Needs a volume; without one a spill fails. |

### Throughput

DuckDB is **not** faster than the previous `psycopg2` streaming `COPY`. Measured on
`dim_360_customer.csv.gz` (50,000 rows, 50 columns) into a local Postgres:

```
DuckDB   : 50,000 rows in 0.95s  (52,504 rows/s)
psycopg2 : 50,000 rows in 0.56s  (89,358 rows/s)
```

Decoding to DuckDB vectors and re-encoding to `COPY` costs more than it saves; Postgres
ingest was already the bottleneck. Row content is byte-identical between the two paths
(verified by md5 over all rows). The reasons to be here are typed/Parquet sources,
in-flight transformation, and multi-file globbing — not speed.

---

## Swap mode (near-zero-downtime reloads)

`truncate_append` holds an `ACCESS EXCLUSIVE` lock on the target for the **entire** load —
every reader blocks from the TRUNCATE until the last row lands. On a large table that is
minutes to hours of downtime.

`load_mode: "swap"` loads a permanent staging twin and rotates the two names instead:

```
TRUNCATE t_stg                              ── readers still on t
DuckDB loads t_stg                          ── readers still on t
rebuild indexes, ANALYZE t_stg              ── readers still on t
ALTER TABLE t      RENAME TO t_old_swap ─┐
ALTER TABLE t_stg  RENAME TO t          ─┼── one short transaction; the only downtime
ALTER TABLE t_old_swap RENAME TO t_stg  ─┘
```

Nothing is created or dropped. The third rename hands the live table's old storage
back as the next run's staging table, so the two tables ping-pong. Both are
permanent and defined in `init/` — the job requires them to already exist.

Readers query the old data for the whole load and are blocked only for the rename, which
is a catalog update. Measured on a local Postgres against a load that takes 2.5s: worst
reader wait was **20 ms** under `swap` versus **2.1 s** under `truncate_append`. The
cutover is atomic — no reader ever sees a partially loaded table.

If anything fails before the rotation, the job aborts and the live table is untouched.
All three renames are in one transaction, so a crash mid-rotation leaves nothing behind
— there is no `_old_swap` table to clean up.

### Options

| Field                | Default | Description                                                                 |
| -------------------- | ------- | --------------------------------------------------------------------------- |
| `swap_suffix`        | `_stg`  | Suffix for the staging table.                                               |
| `swap_lock_timeout`  | `5s`    | How long the rename waits for its lock before backing off, so it never queues the whole database behind a long-running reader. |
| `swap_retries`       | `5`     | Rename attempts before failing, with exponential backoff.                   |
| `swap_defer_indexes` | `true`  | Drop indexes on the staging table before the load and rebuild after — markedly faster than maintaining them row by row. |
| `allow_unsafe_swap`  | `false` | Proceed despite the safety checks below (logs a warning per finding).        |

### Why the twin is permanent

The job used to clone the target with `CREATE TABLE ... (LIKE ... INCLUDING ALL)` on
every run. `LIKE` does not carry grants, owner, the table comment, storage parameters or
tablespace, so the job had to read all of those off the live table and replay them onto
the clone afterwards.

A permanent twin removes that entirely. The rotation moves each relation wholesale, so
grants, owner, comment, storage parameters and statistics travel with it — nothing needs
copying. What replaces the replay is a **check**: because the twin becomes the live table,
any difference between the two is a silent change to the live table, so the job compares
them and refuses to rotate a twin that is not interchangeable. Compared are columns
(name, type, order, NOT NULL, default), grants, owner, comment, storage parameters,
tablespace, persistence, triggers, RLS policies, and index/constraint structure.

Index and constraint **names** are deliberately excluded from that comparison — they
share a schema-wide namespace, so the twin's can never match the live table's. Instead
the rotation swaps each matching pair through a temporary name, inside the same
transaction, so names stay as `init/` declared them instead of alternating every run.

Keeping two tables in step is the cost of this design: **any future `ALTER TABLE` on the
live table must be applied to the twin as well.** Forget one and the next swap fails with
the exact difference rather than loading into a mismatched table:

```
"public"."dim_360_customer" is not safe to swap:
  - columns differ between "public"."dim_360_customer" and "public"."dim_360_customer_stg"
    — only on "public"."dim_360_customer": []; only on "..._stg": [(51, 'extra', 'text', False, '')]
```

It also costs disk: the twin permanently holds a full copy of the table.

### Safety checks

The target's definition is cloned with `LIKE ... INCLUDING ALL`, which does not carry
everything, and objects that bind to the table's **OID** keep pointing at the old table
after the rename. The job refuses to swap when it finds:

| Blocker                       | Why                                                        |
| ----------------------------- | ---------------------------------------------------------- |
| Dependent views / matviews    | They bind to the OID and would follow the relation out to staging. |
| Inbound foreign keys          | Same — they would reference whichever table is now staging. |
| `serial` columns              | The sequence stays with the relation, not the name.         |
| Twin differs from live        | Any of the properties listed above. Reported with the exact difference. |

Two blockers are gone. `LIKE` could not clone a **partitioned** table, but renaming a
partitioned parent is fine, so partitioned targets are now allowed (the migration has to
create the twin's partitions). **Triggers** and **RLS policies** are no longer lost,
because the twin is created once by a migration rather than cloned per run — they are
compared instead of blocked.

These are hard errors by default. Use `truncate_append` for such tables, or set
`allow_unsafe_swap: true` once you have confirmed the consequence is acceptable.

One limitation worth knowing: `rds_target` is split on `.`, so a schema or table name
containing a literal dot is not supported.

### Table ownership

The rotation renames **both** tables, and only a table's **owner** may rename it. That is
free when one role does everything, but migrations and ingestion often run as different
service accounts — and then whichever one did not create the table cannot swap it:

```
ValueError: "public"."dim_360_customer" is owned by 'cdp_table_owner', but the job
connects as 'sa-...iam', which cannot act as that owner.
```

The fix is to own tables as a shared, login-less role and make both service accounts
members of it, so either can act as owner without depending on the other's identity.
Run this as `postgres` (`cloudsqlsuperuser`), since granting a role requires
`ADMIN OPTION` on it:

```sql
CREATE ROLE cdp_table_owner NOLOGIN;
GRANT cdp_table_owner TO "cdphub-ms";
GRANT cdp_table_owner TO "sa-cdphub-migration-svc-dev@prj-dev-cdphub-svc-05.iam";
GRANT cdp_table_owner TO "sa-cdphub-pg-ingestion-cdp@prj-dev-cdp-svc-91.iam";

GRANT USAGE, CREATE ON SCHEMA public
TO cdp_table_owner;

ALTER TABLE public.dim_360_customer
OWNER TO cdp_table_owner;
ALTER TABLE public.dim_360_customer_stg
OWNER TO cdp_table_owner;
```

Notes:

- **Grant to every role that connects**, migration *and* ingestion. Name them exactly as
  Postgres sees them — for Cloud SQL IAM users that is the service-account email with
  `.gserviceaccount.com` stripped, and it must match `DB_USER`, not the SA you assume is
  in use.
- **Re-own each new table, twin included.** Tables are owned by whoever ran the
  migration, so a fresh target and its `_stg` twin each need their own
  `ALTER TABLE ... OWNER TO cdp_table_owner` (see `sql/init/owner.sql`).
- **Members must `INHERIT`.** The job never issues `SET ROLE`, so a `NOINHERIT` member
  holds the membership without holding the owner's privileges and still fails the rename.

Verify from the job's own connection before running a load:

```sql
SELECT current_user,
       pg_has_role(current_user, 'cdp_table_owner', 'USAGE') AS can_act_as_owner;
```

The same check runs at the start of every swap (`assert_swap_safe`), so a misconfigured
grant fails immediately rather than after the BigQuery export and the load.

### Recovery

A crashed run leaves the twin holding partial data, which the next run truncates before
loading. There is nothing to drop: the rotation is a single transaction, and both tables
are permanent. If the twin was lost entirely, recreate it from `sql/init/obt.up.sql`.

Note that swap mode holds roughly **double the table's disk space** permanently, since
the twin always carries a full copy.

---

## Environment

Required at runtime:

| Variable                         | Purpose                                                       |
| -------------------------------- | ------------------------------------------------------------- |
| `GOOGLE_CLOUD_PROJECT`           | BigQuery project ID.                                          |
| `GCS_BUCKET_PREFIX`              | Bucket prefix used to build a URI from `gcs_path`.            |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to the service-account JSON (BigQuery + GCS access). DuckDB reuses these same credentials to reach GCS — see [The GCS → Postgres load](#the-gcs--postgres-load-duckdb). |

The Postgres connection string is passed via `--postgres_conn` and is only needed when a
task loads into Postgres.

The local `.env` (read by `docker-compose.yaml`) configures the bundled Postgres:

```env
DB_USER=app
DB_PASSWORD=app
DB_HOST=postgres
DB_NAME=postgres
```

---

## Running

### Python

```sh
python main.py --config ./config/ai_insight.json \
  --postgres_conn "postgresql+psycopg2://user:pass@host:5432/dbname"
```

Omit `--postgres_conn` when every task sets `gcs_export_only: true`.

### Docker

The image runs as the non-root user `appuser` and uses `ENTRYPOINT ["python", "main.py"]`,
so you pass **only CLI arguments**, and credentials mount under `/home/appuser/.config/gcloud`.

```sh
# Build
docker build -t bq-export .

# Run an export
docker run --rm \
  -e GOOGLE_CLOUD_PROJECT=prj-dev-cdp-svc-91 \
  -e GCS_BUCKET_PREFIX=gs://bkt-prj-dev-cdp-svc-ase1-cdp-fortii/dev \
  -v /path/to/sa-credentials.json:/home/appuser/.config/gcloud/application_default_credentials.json \
  bq-export --config ./config/ai_insight.json --postgres_conn "$POSTGRES_CONN"

# Open a shell for debugging (override the entrypoint)
docker run -it --rm --entrypoint bash \
  -v "$(pwd)":/app \
  -v /path/to/sa-credentials.json:/home/appuser/.config/gcloud/application_default_credentials.json \
  bq-export
```

---

## Project layout

```
main.py             # entrypoint and ETL logic
config/             # task configs (one JSON list per run)
sql/                # query files referenced by configs
init/               # SQL migrations for target tables (up/down)
Dockerfile
docker-compose.yaml # local Postgres + the export job
```

---

## Troubleshooting

```sh
# "no space left on device" — reclaim Docker disk
docker system prune -af

# Inspect CPU / memory inside a running container
apt install procps
top -p {pid}
grep VmPeak /proc/{pid}/status
```

---

**Maintainer:** Edward Chong

