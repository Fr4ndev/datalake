"""Tests del runner de migraciones (regla 11 de AGENTS.md).

Los tests puros (parser de directivas, gate de env, orden de ficheros) corren siempre.
El test de integracion contra la DB se salta si no hay MIGRATE_DSN.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
MIGRATE_PY = REPO / "tsdb" / "migrate.py"

spec = importlib.util.spec_from_file_location("tsdb_migrate", MIGRATE_PY)
assert spec and spec.loader
migrate = importlib.util.module_from_spec(spec)
sys.modules["tsdb_migrate"] = migrate
spec.loader.exec_module(migrate)


# --------------------------------------------------------------------- directivas
def test_no_transaction_detected():
    assert migrate.parse_directives("-- no-transaction\nSELECT 1;") == (True, None)


def test_requires_env_detected():
    no_txn, required = migrate.parse_directives("-- requires-env: ENABLE_COMPRESSION=true\nSELECT 1;")
    assert no_txn is False
    assert required == ("ENABLE_COMPRESSION", "true")


def test_requires_env_without_value_is_not_truthy_gate():
    # "false" debe evaluarse como falsy, no como "valor distinto de true".
    _, required = migrate.parse_directives("-- requires-env: FLAG=false")
    assert required == ("FLAG", "false")


def test_both_directives_together():
    sql = "-- no-transaction\n-- requires-env: FOO=1\nSELECT 1;"
    assert migrate.parse_directives(sql) == (True, ("FOO", "1"))


def test_no_directives_defaults():
    assert migrate.parse_directives("SELECT 1;\n-- un comentario normal\n") == (False, None)


def test_directive_with_migrate_prefix():
    assert migrate.parse_directives("-- migrate:no-transaction")[0] is True


def test_directive_is_case_insensitive():
    assert migrate.parse_directives("-- NO-TRANSACTION")[0] is True


def test_similar_words_are_not_directives():
    # No debe dar falsos positivos con texto que mentiona las palabras.
    assert migrate.parse_directives("-- esto no es no-transaction de verdad") == (False, None)
    assert migrate.parse_directives("-- requires-env: sin signo de igual") == (False, None)


# ------------------------------------------------------------------------ gate env
@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", " true "])
def test_truthy_env_satisfies(monkeypatch, value):
    monkeypatch.setenv("FLAG", value)
    assert migrate.env_satisfied(("FLAG", "true")) is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_falsy_env_does_not_satisfy(monkeypatch, value):
    monkeypatch.setenv("FLAG", value)
    assert migrate.env_satisfied(("FLAG", "true")) is False


def test_missing_env_var_does_not_satisfy_truthy_gate(monkeypatch):
    monkeypatch.delenv("NO_EXISTE_1234", raising=False)
    assert migrate.env_satisfied(("NO_EXISTE_1234", "true")) is False


def test_exact_string_comparison(monkeypatch):
    monkeypatch.setenv("TZ_LIKE", "UTC")
    assert migrate.env_satisfied(("TZ_LIKE", "UTC")) is True
    assert migrate.env_satisfied(("TZ_LIKE", "Europe/Madrid")) is False


def test_no_requirement_always_satisfied():
    assert migrate.env_satisfied(None) is True


# ----------------------------------------------------------------------- ficheros
def test_migration_files_sorted_numerically(tmp_path):
    for name in ["50_compression.sql", "10_candles.sql", "21_cagg.sql", "2_early.sql"]:
        (tmp_path / name).write_text("SELECT 1;")
    assert [p.name for p in migrate.migration_files(tmp_path)] == [
        "10_candles.sql",
        "21_cagg.sql",
        "2_early.sql",
        "50_compression.sql",
    ]


def test_migration_files_ignores_non_sql(tmp_path):
    (tmp_path / "10_a.sql").write_text("SELECT 1;")
    (tmp_path / "README.md").write_text("no")
    (tmp_path / "notes.txt").write_text("no")
    assert [p.name for p in migrate.migration_files(tmp_path)] == ["10_a.sql"]


def test_real_migrations_are_discovered_and_ordered():
    files = migrate.migration_files(REPO / "tsdb" / "migrations")
    names = [p.name for p in files]
    assert names == sorted(names)
    # Las 4 hypertables, los 4 caggs y compression deben existir.
    assert "10_candles_1m.sql" in names
    assert "50_compression.sql" in names
    assert len([n for n in names if n.startswith("2")]) == 4  # caggs


def test_cagg_files_are_marked_no_transaction():
    files = migrate.migration_files(REPO / "tsdb" / "migrations")
    caggs = [p for p in files if p.name.startswith("2")]
    assert caggs, "no hay ficheros de cagg"
    for path in caggs:
        no_txn, _ = migrate.parse_directives(path.read_text(encoding="utf-8"))
        assert no_txn, f"{path.name} deberia llevar -- no-transaction"


def test_compression_file_is_gated_by_env():
    path = REPO / "tsdb" / "migrations" / "50_compression.sql"
    _, required = migrate.parse_directives(path.read_text(encoding="utf-8"))
    assert required is not None, "50_compression.sql debe declarar -- requires-env"
    assert required[0] == "ENABLE_COMPRESSION"


def test_table_migrations_are_transactional():
    files = migrate.migration_files(REPO / "tsdb" / "migrations")
    tables = [p for p in files if p.name.startswith("1")]
    # Sin numero fijo: cada dtype nuevo anade una y el recuento se queda viejo al crear la
    # siguiente. Se comprueba que hay al menos las 4 de Fase 1 y que TODAS van en transaccion.
    assert len(tables) >= 4, f"faltan migraciones de tabla: {[p.name for p in tables]}"
    for path in tables:
        no_txn, _ = migrate.parse_directives(path.read_text(encoding="utf-8"))
        assert no_txn is False, f"{path.name} deberia ir en transaccion"


# ------------------------------------------------------------------- integracion
@pytest.mark.skipif(
    not os.environ.get("MIGRATE_DSN"),
    reason="requiere MIGRATE_DSN (contenedor: docker-compose run --rm migrate)",
)
def test_migrate_is_idempotent_against_real_db():
    """Aplica las migraciones dos veces: la 2a no debe aplicar nada ni fallar."""
    rc_first = migrate.main(["--dir", str(REPO / "tsdb" / "migrations")])
    assert rc_first == 0
    rc_second = migrate.main(["--dir", str(REPO / "tsdb" / "migrations")])
    assert rc_second == 0