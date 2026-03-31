"""
Unit tests for selective_ci_checks.py.

Tests the classify() and build_import_graph() functions against
synthetic connector structures to verify correct behavior for all
tiers: direct connectors, shared bases, non-connector changes,
and safe defaults.
"""

import sys
from pathlib import Path

import pytest
import yaml

# Add scripts dir to path so we can import the module
# tests/unit/ -> tests/ -> metadata-ingestion/ -> metadata-ingestion/scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))
from selective_ci_checks import (
    CIDecisions,
    build_import_graph,
    classify,
    load_connector_registry,
    validate,
)


@pytest.fixture
def repo_root(tmp_path):
    """Create a minimal but realistic connector structure for testing."""
    mi = tmp_path / "metadata-ingestion"
    src = mi / "src" / "datahub" / "ingestion" / "source"
    tests = mi / "tests" / "integration"

    # Direct connector -- auto-discovered by convention (no connector.yaml)
    (src / "powerbi").mkdir(parents=True)
    (src / "powerbi" / "powerbi.py").write_text(
        "from datahub.ingestion.api.source import Source\n"
        "class PowerBISource(Source): pass\n"
    )
    (src / "powerbi" / "__init__.py").write_text("")
    (tests / "powerbi").mkdir(parents=True)

    # Shared base with connector.yaml
    (src / "sql").mkdir(parents=True)
    (src / "sql" / "connector.yaml").write_text(
        yaml.dump(
            {
                "is_shared_base": True,
                "test_paths": [
                    "tests/integration/mysql/",
                    "tests/integration/postgres/",
                ],
            }
        )
    )
    (src / "sql" / "sql_common.py").write_text("class SQLAlchemySource: pass\n")
    (src / "sql" / "__init__.py").write_text("")

    # Consumer that imports from sql base -- auto-discovered by convention
    (src / "mysql").mkdir(parents=True)
    (src / "mysql" / "mysql.py").write_text(
        "from datahub.ingestion.source.sql.sql_common import SQLAlchemySource\n"
        "class MySQLSource(SQLAlchemySource): pass\n"
    )
    (src / "mysql" / "__init__.py").write_text("")
    (tests / "mysql").mkdir(parents=True)

    # Another consumer -- auto-discovered
    (src / "postgres").mkdir(parents=True)
    (src / "postgres" / "postgres.py").write_text(
        "from datahub.ingestion.source.sql.sql_common import SQLAlchemySource\n"
        "class PostgresSource(SQLAlchemySource): pass\n"
    )
    (src / "postgres" / "__init__.py").write_text("")
    (tests / "postgres").mkdir(parents=True)

    # Connector with no shared deps -- auto-discovered
    (src / "kafka").mkdir(parents=True)
    (src / "kafka" / "kafka.py").write_text(
        "from datahub.ingestion.api.source import Source\n"
        "class KafkaSource(Source): pass\n"
    )
    (src / "kafka" / "__init__.py").write_text("")
    (tests / "kafka").mkdir(parents=True)

    # Create tests/ and scripts/ dirs for SAFE_PREFIXES
    (mi / "scripts").mkdir(parents=True, exist_ok=True)

    return tmp_path


class TestNonConnectorChanges:
    """Any file NOT under source/{connector}/ triggers full suite."""

    def test_api_change(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/api/source.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_setup_py(self, repo_root):
        d = classify(["metadata-ingestion/setup.py"], repo_root)
        assert d.run_all_integration is True

    def test_metadata_models(self, repo_root):
        d = classify(["metadata-models/src/main/resources/entity.graphql"], repo_root)
        assert d.run_all_integration is True

    def test_emitter_change(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/emitter/mce_builder.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_workflow_change(self, repo_root):
        d = classify([".github/workflows/metadata-ingestion.yml"], repo_root)
        # Not under metadata-ingestion/ or metadata-models/ -- ignored
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestSharedBase:
    """Shared base changes trigger all consumers via import analysis."""

    def test_sql_base_change_triggers_consumers(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        # mysql and postgres detected via import analysis AND shared base test_paths
        assert "tests/integration/mysql/" in paths
        assert "tests/integration/postgres/" in paths
        # powerbi does NOT import from sql
        assert "tests/integration/powerbi/" not in paths


class TestDirectConnector:
    """Direct connector change triggers only that connector's tests."""

    def test_powerbi_only(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"
        assert d.test_matrix[0]["test_path"] == "tests/integration/powerbi/"

    def test_kafka_only(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "kafka"


class TestSafeDefaults:
    """Unknown or unmatched files trigger full suite."""

    def test_unknown_metadata_ingestion_file(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/brand_new.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_unknown_source_dir(self, repo_root):
        (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
            / "unknown_connector"
        ).mkdir()
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/unknown_connector/foo.py"
            ],
            repo_root,
        )
        assert d.run_all_integration is True

    def test_non_metadata_file_ignored(self, repo_root):
        d = classify(["docs/README.md"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_empty_diff(self, repo_root):
        d = classify([], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestIntegrationTestChanges:
    """Integration test / golden file changes trigger that connector's tests."""

    def test_test_file_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/tests/integration/powerbi/test_powerbi.py"], repo_root
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"

    def test_golden_file_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/tests/integration/kafka/golden_mces.json"], repo_root
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "kafka"


class TestSafePrefixes:
    """Unit tests, scripts, docs don't trigger integration tests."""

    def test_unit_test_change(self, repo_root):
        d = classify(["metadata-ingestion/tests/unit/test_something.py"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_script_change(self, repo_root):
        d = classify(["metadata-ingestion/scripts/selective_ci_checks.py"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_docs_change(self, repo_root):
        d = classify(["metadata-ingestion/docs/dev_guides/selective_ci.md"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestMixedChanges:
    """Multiple changed files are handled correctly."""

    def test_connector_plus_non_connector(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/setup.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is True

    def test_two_connectors(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        connectors = {e["connector"] for e in d.test_matrix}
        assert connectors == {"powerbi", "kafka"}

    def test_connector_plus_test(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/tests/integration/powerbi/test_powerbi.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"


class TestExtraSourcePaths:
    """Connectors with extra_source_paths trigger correctly."""

    def test_extra_source_path_triggers_connector(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        # Add extra_source_paths to powerbi
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "extra_source_paths": [
                        "src/datahub/ingestion/source/powerbi_report_server/"
                    ],
                }
            )
        )
        # Create the extra source dir
        (src / "powerbi_report_server").mkdir(parents=True, exist_ok=True)
        (src / "powerbi_report_server" / "__init__.py").write_text("")
        (tests / "powerbi_report_server").mkdir(parents=True, exist_ok=True)

        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi_report_server/server.py"
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        connectors = {e["connector"] for e in d.test_matrix}
        assert "powerbi" in connectors
        assert "powerbi_report_server" in connectors


class TestExtraTestPaths:
    """Connectors with extra_test_paths include all test dirs."""

    def test_extra_test_paths_included(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        # Add extra_test_paths to powerbi
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "extra_test_paths": ["tests/integration/powerbi_extras/"],
                }
            )
        )
        (tests / "powerbi_extras").mkdir(parents=True, exist_ok=True)

        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/powerbi/" in paths
        assert "tests/integration/powerbi_extras/" in paths


class TestSafetyNet:
    """Changed source dirs that produce no tests fall back to full suite."""

    def test_shared_base_with_empty_test_paths(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )

        # Create a shared base with empty test_paths and no consumers
        (src / "orphan_base").mkdir(parents=True)
        (src / "orphan_base" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "is_shared_base": True,
                    "test_paths": [],
                }
            )
        )
        (src / "orphan_base" / "base.py").write_text("class OrphanBase: pass\n")

        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/orphan_base/base.py"],
            repo_root,
        )
        # Safety net: changed source dirs with no test output -> full suite
        assert d.run_all_integration is True


class TestImportGraph:
    """Verify import graph is built correctly."""

    def test_mysql_depends_on_sql(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        mysql_deps = graph.get("src/datahub/ingestion/source/mysql", set())
        assert "src/datahub/ingestion/source/sql" in mysql_deps

    def test_powerbi_does_not_depend_on_sql(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        powerbi_deps = graph.get("src/datahub/ingestion/source/powerbi", set())
        assert "src/datahub/ingestion/source/sql" not in powerbi_deps

    def test_no_self_imports(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        for src_dir, deps in graph.items():
            assert src_dir not in deps, f"{src_dir} has self-import"

    def test_bare_import_detected(self, repo_root):
        """Test that 'import datahub.ingestion.source.sql' (bare import) is detected."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        (src / "bare_importer").mkdir(parents=True)
        (src / "bare_importer" / "source.py").write_text(
            "import datahub.ingestion.source.sql.sql_common\n"
        )
        (src / "bare_importer" / "__init__.py").write_text("")
        (tests / "bare_importer").mkdir(parents=True)

        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        bare_deps = graph.get("src/datahub/ingestion/source/bare_importer", set())
        assert "src/datahub/ingestion/source/sql" in bare_deps

    def test_syntax_error_skipped(self, repo_root):
        """A .py file with syntax errors doesn't crash the import graph build."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "broken.py").write_text("def broken(\n")

        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        # Should complete without error
        assert "src/datahub/ingestion/source/powerbi" in graph


class TestConventionDiscovery:
    """Verify convention-based connector discovery."""

    def test_convention_discovers_matching_dirs(self, repo_root):
        registry = load_connector_registry(repo_root)
        names = {c["_source_dir"].split("/")[-1] for c in registry}
        assert "powerbi" in names
        assert "kafka" in names
        assert "mysql" in names
        assert "postgres" in names

    def test_shared_base_discovered_via_yaml(self, repo_root):
        registry = load_connector_registry(repo_root)
        sql_entries = [c for c in registry if c["_source_dir"].endswith("/sql")]
        assert len(sql_entries) == 1
        assert sql_entries[0].get("is_shared_base") is True

    def test_underscore_dirs_excluded(self, repo_root):
        """__pycache__ and similar dirs are not treated as connectors."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "__pycache__").mkdir(parents=True, exist_ok=True)
        registry = load_connector_registry(repo_root)
        names = {c["_source_dir"].split("/")[-1] for c in registry}
        assert "__pycache__" not in names


class TestMalformedYAML:
    """Malformed connector.yaml files are handled gracefully."""

    def test_invalid_yaml_syntax_exits(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("is_shared_base: [unterminated")
        with pytest.raises(SystemExit):
            load_connector_registry(repo_root)

    def test_yaml_non_dict_exits(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("- just\n- a\n- list\n")
        with pytest.raises(SystemExit):
            load_connector_registry(repo_root)

    def test_empty_yaml_uses_convention(self, repo_root):
        """An empty connector.yaml falls back to convention discovery."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("")

        registry = load_connector_registry(repo_root)
        powerbi = [c for c in registry if c["_source_dir"].endswith("/powerbi")]
        assert len(powerbi) == 1
        # Convention should still discover the test path
        assert powerbi[0].get("test_path") == "tests/integration/powerbi/"


class TestValidate:
    """Validate function catches misconfigurations."""

    def test_valid_config_no_errors(self, repo_root):
        errors = validate(repo_root)
        assert errors == []

    def test_nonexistent_test_path(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "test_path": "tests/integration/does_not_exist/",
                }
            )
        )
        errors = validate(repo_root)
        assert any("does not exist" in e for e in errors)

    def test_shared_base_empty_no_dependents(self, repo_root):
        """Shared base with empty test_paths and no dependents is flagged."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "orphan_base").mkdir(parents=True)
        (src / "orphan_base" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "is_shared_base": True,
                    "test_paths": [],
                }
            )
        )
        (src / "orphan_base" / "base.py").write_text("class OrphanBase: pass\n")

        errors = validate(repo_root)
        assert any("orphan_base" in e and "no detected downstream" in e for e in errors)

    def test_shared_base_empty_with_dependents_ok(self, repo_root):
        """Shared base with empty test_paths but has dependents is OK."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        (src / "common_base").mkdir(parents=True)
        (src / "common_base" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "is_shared_base": True,
                    "test_paths": [],
                }
            )
        )
        (src / "common_base" / "utils.py").write_text("def helper(): pass\n")

        # Create a connector that imports from common_base
        (src / "consumer").mkdir(parents=True)
        (src / "consumer" / "source.py").write_text(
            "from datahub.ingestion.source.common_base.utils import helper\n"
        )
        (src / "consumer" / "__init__.py").write_text("")
        (tests / "consumer").mkdir(parents=True)

        errors = validate(repo_root)
        assert not any("common_base" in e for e in errors)


class TestCIDecisionsInvariant:
    """CIDecisions enforces mutual exclusivity."""

    def test_run_all_with_matrix_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            CIDecisions(
                run_all_integration=True,
                test_matrix=[{"connector": "x", "test_path": "y"}],
            )

    def test_run_all_empty_matrix_ok(self):
        d = CIDecisions(run_all_integration=True)
        assert d.test_matrix == []

    def test_selective_with_matrix_ok(self):
        d = CIDecisions(test_matrix=[{"connector": "x", "test_path": "y"}])
        assert d.run_all_integration is False
