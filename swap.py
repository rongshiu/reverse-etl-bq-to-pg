"""The catalog half of load_mode "swap": compare the pair, then rotate their names.

Both tables are permanent (see sql/init/), so a run creates and drops nothing.
It loads the staging twin, then exchanges the two tables' names — which leaves
the twin holding the data the live table had, ready to be the next run's twin.

Nothing here touches BigQuery, GCS or DuckDB; every function takes a
`sa.Connection` and reads or writes the Postgres catalog. main.py owns the
orchestration that pairs this with the export and the load.
"""

import logging

import sqlalchemy as sa

from identifiers import quote_ident

# A rename cannot swap two names directly, so the rotation parks the live name here
# for the length of one statement. Nothing is ever left behind under this suffix:
# the three renames share a transaction, so a crash rolls all of them back.
TRANSIENT_SUFFIX = "_swap_tmp"

# swap mode: load the permanent twin at <table><swap_suffix>, then rotate the two
# names so the twin becomes live and the live table becomes the next run's twin.
DEFAULT_SWAP_SUFFIX = "_stg"

# The rotation takes an ACCESS EXCLUSIVE lock. Bounded so a long-running reader
# delays it instead of queueing the whole database behind it.
DEFAULT_LOCK_TIMEOUT = "5s"
DEFAULT_SWAP_RETRIES = 5

logger = logging.getLogger("bq_export")


def table_fingerprint(conn: sa.Connection, fq_table: str) -> dict:
    """Describe everything about a table that the rotation silently carries over.

    The old swap cloned the live table and then replayed its grants, owner, comment
    and storage options onto the clone. With a permanent staging twin there is
    nothing to replay — but also nothing forcing the two to stay alike, so instead
    of copying these properties the job compares them and refuses to rotate a twin
    that is not interchangeable with the live table.

    Index and constraint *names* are excluded on purpose: they share a
    schema-wide namespace, so the twin's can never match. Their structure is
    compared through index_signature() instead.
    """
    columns = conn.execute(
        sa.text("""
            SELECT a.attnum, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
                   COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')
            FROM pg_attribute a
            LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE a.attrelid = to_regclass(:t) AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """),
        {"t": fq_table},
    ).fetchall()

    acl = conn.execute(
        sa.text("""
            SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
                   a.privilege_type, a.is_grantable
            FROM pg_class c, aclexplode(c.relacl) a
            WHERE c.oid = to_regclass(:t)
        """),
        {"t": fq_table},
    ).fetchall()

    owner, comment, reloptions, tablespace, persistence = conn.execute(
        sa.text("""
            SELECT pg_get_userbyid(c.relowner),
                   COALESCE(obj_description(c.oid, 'pg_class'), ''),
                   COALESCE(array_to_string(c.reloptions, ', '), ''),
                   COALESCE(ts.spcname, ''),
                   c.relpersistence
            FROM pg_class c
            LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
            WHERE c.oid = to_regclass(:t)
        """),
        {"t": fq_table},
    ).one()

    triggers = conn.execute(
        sa.text("""
            SELECT t.tgname, t.tgtype, p.proname
            FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
            WHERE t.tgrelid = to_regclass(:t) AND NOT t.tgisinternal
        """),
        {"t": fq_table},
    ).fetchall()

    policies = conn.execute(
        sa.text("""
            SELECT polname, polcmd,
                   COALESCE(pg_get_expr(polqual, polrelid), ''),
                   COALESCE(pg_get_expr(polwithcheck, polrelid), '')
            FROM pg_policy WHERE polrelid = to_regclass(:t)
        """),
        {"t": fq_table},
    ).fetchall()

    constraints, indexes = capture_object_names(conn, fq_table)

    return {
        "columns": [tuple(row) for row in columns],
        "grants": sorted(tuple(row) for row in acl),
        "owner": owner,
        "comment": comment,
        "storage parameters": reloptions,
        "tablespace": tablespace,
        "persistence": persistence,
        "triggers": sorted(tuple(row) for row in triggers),
        "row-level security policies": sorted(tuple(row) for row in policies),
        "constraints": sorted(constraints),
        "indexes": sorted(indexes),
    }


def assert_swap_safe(
    conn: sa.Connection,
    fq_target: str,
    fq_staging: str,
    allow_unsafe: bool,
) -> None:
    """Refuse to rotate unless the twin is interchangeable and nothing binds to the OID.

    Two separate concerns:

    * The rotation exchanges the two tables' names, so whatever the twin carries
      becomes what the live table carries. Any difference between them is a silent
      change to the live table on the next load.
    * A rename does not follow the table's OID, so anything bound to it — views,
      inbound foreign keys, a serial's owned sequence — keeps pointing at the
      relation that just became staging.
    """
    missing = [
        name
        for name in (fq_target, fq_staging)
        if conn.execute(
            sa.text("SELECT to_regclass(:t)"), {"t": name}
        ).scalar() is None
    ]
    if missing:
        raise ValueError(
            f"swap mode needs both {fq_target} and its staging twin {fq_staging} to exist; "
            f"missing: {', '.join(missing)}. The job no longer creates the twin — add it to "
            "the init/ migrations (see sql/init/obt.up.sql)."
        )

    # The rotation renames both tables, which only their owner may do. When the job
    # role differs from the owner (e.g. migrations and ingestion run as separate
    # users sharing an owner role), that right comes from role membership — and
    # 'USAGE' rather than 'MEMBER' because a NOINHERIT member holds the membership
    # without holding the privileges. Checked here so a misconfigured grant fails
    # before the export and the load rather than at the rename.
    for name in (fq_target, fq_staging):
        owner, may_act_as_owner = conn.execute(
            sa.text("""
                SELECT pg_get_userbyid(c.relowner),
                       pg_has_role(current_user, c.relowner, 'USAGE')
                FROM pg_class c WHERE c.oid = to_regclass(:t)
            """),
            {"t": name},
        ).one()
        if not may_act_as_owner:
            current_user = conn.execute(sa.text("SELECT current_user")).scalar()
            raise ValueError(
                f"{name} is owned by {owner!r}, but the job connects as {current_user!r}, "
                f"which cannot act as that owner. swap mode renames both tables, so it needs "
                f"ownership of each. Run `GRANT {owner} TO \"{current_user}\";` (and ensure "
                f"the role INHERITs), or use load_mode=truncate_append."
            )

    blockers: list[str] = []

    for name in (fq_target, fq_staging):
        inbound_fks = conn.execute(
            sa.text("""
                SELECT conname, conrelid::regclass::text
                FROM pg_constraint
                WHERE confrelid = to_regclass(:t) AND contype = 'f'
            """),
            {"t": name},
        ).fetchall()
        if inbound_fks:
            detail = ", ".join(f"{fk} on {tbl}" for fk, tbl in inbound_fks)
            blockers.append(
                f"foreign keys reference {name} and would follow it out of place: {detail}"
            )

        dependents = conn.execute(
            sa.text("""
                SELECT DISTINCT c.relname
                FROM pg_depend d
                JOIN pg_rewrite r ON r.oid = d.objid
                JOIN pg_class c ON c.oid = r.ev_class
                WHERE d.refobjid = to_regclass(:t)
                  AND d.classid = 'pg_rewrite'::regclass
                  AND c.oid <> to_regclass(:t)
            """),
            {"t": name},
        ).scalars().all()
        if dependents:
            blockers.append(
                f"views/matviews depend on {name} and would follow it out of place: "
                f"{', '.join(dependents)}"
            )

        serials = conn.execute(
            sa.text("""
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                WHERE a.attrelid = to_regclass(:t)
                  AND pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval%'
            """),
            {"t": name},
        ).scalars().all()
        if serials:
            blockers.append(
                f"serial columns on {name} own a sequence that stays with the relation, "
                f"not the name: {', '.join(serials)}"
            )

    live = table_fingerprint(conn, fq_target)
    twin = table_fingerprint(conn, fq_staging)
    for prop, live_value in live.items():
        twin_value = twin[prop]
        if live_value == twin_value:
            continue

        if isinstance(live_value, list):
            # Report only what actually differs; these lists run to one entry per
            # column, and dumping both in full buries the drift that matters.
            only_live = [item for item in live_value if item not in twin_value]
            only_twin = [item for item in twin_value if item not in live_value]
            detail = f"only on {fq_target}: {only_live!r}; only on {fq_staging}: {only_twin!r}"
        else:
            detail = f"{live_value!r} vs {twin_value!r}"

        blockers.append(f"{prop} differ between {fq_target} and {fq_staging} — {detail}")

    if not blockers:
        return

    if allow_unsafe:
        for blocker in blockers:
            logger.warning("swap safety check overridden for %s: %s", fq_target, blocker)
        return

    raise ValueError(
        f"{fq_target} is not safe to swap:\n  - "
        + "\n  - ".join(blockers)
        + "\nBring the staging twin back in line with the live table (both are defined in "
        "init/), use load_mode=truncate_append, or set allow_unsafe_swap=true if you have "
        "verified these differences are acceptable."
    )


def index_signature(definition: str) -> str:
    """Reduce a `CREATE INDEX` statement to what identifies it apart from its name.

    `pg_get_indexdef` embeds both the index name and the table name, neither of
    which match between the live table and its staging clone. Everything from
    `USING` onwards does match, and that plus uniqueness is enough to pair them up.
    """
    unique = definition.startswith("CREATE UNIQUE INDEX")
    _, _, tail = definition.partition(" USING ")
    return f"{unique}|{tail}"


def capture_object_names(conn: sa.Connection, fq_table: str) -> tuple[dict, dict]:
    """Map index/constraint signatures to the names they carry on the given table."""
    constraints: dict[str, list[str]] = {}
    for name, definition in conn.execute(
        sa.text("""
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = to_regclass(:t) AND contype IN ('p', 'u', 'x')
        """),
        {"t": fq_table},
    ):
        constraints.setdefault(definition, []).append(name)

    indexes: dict[str, list[str]] = {}
    for name, definition in conn.execute(
        sa.text("""
            SELECT ic.relname, pg_get_indexdef(i.indexrelid)
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = to_regclass(:t)
              AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
        """),
        {"t": fq_table},
    ):
        indexes.setdefault(index_signature(definition), []).append(name)

    for names in (*constraints.values(), *indexes.values()):
        names.sort()
    return constraints, indexes


def rotate_object_names(
    conn: sa.Connection,
    schema: str,
    table: str,
    staging: str,
    live_names: tuple[dict, dict],
    twin_names: tuple[dict, dict],
) -> None:
    """Exchange the two tables' index and constraint names, following the rotation.

    Index names share a schema-wide namespace, so the twin's indexes can never carry
    the live names while the live table still exists. After the rotation the table
    now called `table` is physically the twin and so carries the twin's names. Left
    alone those names would alternate every run and stop matching init/, so each
    pair is swapped through a temporary name.
    """
    live_constraints, live_indexes = live_names
    twin_constraints, twin_indexes = twin_names

    fq_table = f"{quote_ident(schema)}.{quote_ident(table)}"
    fq_staging = f"{quote_ident(schema)}.{quote_ident(staging)}"

    def swap(signature_map_a: dict, signature_map_b: dict, rename) -> None:
        for signature, wanted in signature_map_a.items():
            # `wanted` are the names the live table had; `held` are the twin's, which
            # the live table is carrying now that the two have exchanged places.
            held = signature_map_b.get(signature, [])
            for want, have in zip(wanted, held):
                if want == have:
                    continue
                transient = f"{have}{TRANSIENT_SUFFIX}"
                rename(fq_staging, want, transient)
                rename(fq_table, have, want)
                rename(fq_staging, transient, have)

    def rename_constraint(owner_table: str, current: str, new: str) -> None:
        conn.execute(
            sa.text(
                f"ALTER TABLE {owner_table} RENAME CONSTRAINT "
                f"{quote_ident(current)} TO {quote_ident(new)}"
            )
        )

    def rename_index(_owner_table: str, current: str, new: str) -> None:
        conn.execute(
            sa.text(
                f"ALTER INDEX {quote_ident(schema)}.{quote_ident(current)} "
                f"RENAME TO {quote_ident(new)}"
            )
        )

    swap(live_constraints, twin_constraints, rename_constraint)
    swap(live_indexes, twin_indexes, rename_index)


def defer_indexes(conn: sa.Connection, schema: str, staging: str) -> list[str]:
    """Drop the staging table's indexes and return the DDL that rebuilds them.

    Loading into an unindexed table and building indexes afterwards is markedly
    faster than maintaining every index row-by-row during COPY. The definitions
    are read back off the staging table, so the generated names are already the
    staging ones and no rewriting is needed.
    """
    fq_staging = f"{quote_ident(schema)}.{quote_ident(staging)}"
    rebuild: list[str] = []

    constraints = conn.execute(
        sa.text("""
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = to_regclass(:t) AND contype IN ('p', 'u', 'x')
        """),
        {"t": fq_staging},
    ).fetchall()
    for name, definition in constraints:
        rebuild.append(f"ALTER TABLE {fq_staging} ADD CONSTRAINT {quote_ident(name)} {definition}")
        conn.execute(sa.text(f"ALTER TABLE {fq_staging} DROP CONSTRAINT {quote_ident(name)}"))

    indexes = conn.execute(
        sa.text("""
            SELECT ic.relname, pg_get_indexdef(i.indexrelid)
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = to_regclass(:t)
              AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
        """),
        {"t": fq_staging},
    ).fetchall()
    for name, definition in indexes:
        rebuild.append(definition)
        conn.execute(sa.text(f"DROP INDEX {quote_ident(schema)}.{quote_ident(name)}"))

    return rebuild
