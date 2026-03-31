#!/usr/bin/env python3
"""
Selective CI checks for DataHub metadata-ingestion.
Modelled after Apache Airflow's selective_checks.py.

Outputs a JSON object consumed by GitHub Actions downstream jobs.
Only files under source/{connector}/ get selective treatment.
Everything else that triggers the workflow -> run_all_integration=true.
"""

import ast
import json
import subprocess
import sys
import typing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TypedDict

import yaml

# The only path that gets selective treatment.
# Everything else that triggers the workflow -> run_all_integration=true.
CONNECTOR_SOURCE_PREFIX = "metadata-ingestion/src/datahub/ingestion/source/"

# Integration test path prefix — changes here trigger that connector's tests
INTEGRATION_TEST_PREFIX = "metadata-ingestion/tests/integration/"

# Paths under metadata-ingestion/ that are safe to ignore
# (no integration tests needed)
SAFE_PREFIXES = [
    "metadata-ingestion/tests/unit/",
    "metadata-ingestion/tests/performance/",
    "metadata-ingestion/tests/conftest",
    "metadata-ingestion/scripts/",
    "metadata-ingestion/docs/",
]


class TestMatrixEntry(TypedDict):
    connector: str
    test_path: str


@dataclass
class CIDecisions:
    test_matrix: list[TestMatrixEntry] = field(default_factory=list)
    run_all_integration: bool = False

    def __post_init__(self) -> None:
        if self.run_all_integration and self.test_matrix:
            raise ValueError(
                "run_all_integration=True and non-empty test_matrix are mutually exclusive"
            )


def get_changed_files(base_ref: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"ERROR: git diff failed (exit {result.returncode}).\n"
            f"  stderr: {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def load_connector_registry(repo_root: Path) -> list[dict]:
    """
    Auto-discover connectors using convention (source dir -> test dir match)
    plus explicit connector.yaml overrides for exceptions.
    """
    mi = repo_root / "metadata-ingestion"
    src = mi / "src" / "datahub" / "ingestion" / "source"
    tests = mi / "tests" / "integration"

    if not src.is_dir():
        print(f"ERROR: Source directory {src} does not exist.", file=sys.stderr)
        sys.exit(1)

    registry = []

    for source_dir in sorted(src.iterdir()):
        if not source_dir.is_dir() or source_dir.name.startswith("_"):
            continue

        rel_source = str(source_dir.relative_to(mi))
        connector_yaml = source_dir / "connector.yaml"

        # Load explicit config if present, otherwise use convention
        if connector_yaml.exists():
            try:
                raw = yaml.safe_load(connector_yaml.read_text())
            except (yaml.YAMLError, OSError) as e:
                print(
                    f"ERROR: Failed to load {connector_yaml}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            if raw is None:
                data = {}
            elif not isinstance(raw, dict):
                print(
                    f"ERROR: {connector_yaml} must be a YAML mapping, got {type(raw).__name__}",
                    file=sys.stderr,
                )
                sys.exit(1)
            else:
                data = raw
        else:
            data = {}

        data["_source_dir"] = rel_source

        # Convention: test_path defaults to tests/integration/{source_dir_name}/
        if "test_path" not in data and not data.get("is_shared_base"):
            default_test_dir = tests / source_dir.name
            if default_test_dir.exists():
                data["test_path"] = f"tests/integration/{source_dir.name}/"

        # Only include if it has a test_path or is a shared base
        if (
            data.get("test_path")
            or data.get("is_shared_base")
            or data.get("test_paths")
        ):
            registry.append(data)

    return registry


def build_import_graph(repo_root: Path, registry: list[dict]) -> dict[str, set[str]]:
    """
    Scan Python imports to build: source_dir -> set of source_dirs it imports from.

    For each connector, parse its .py files with ast and extract import targets.
    Map those imports back to known source directories in the registry.
    """
    mi_root = repo_root / "metadata-ingestion"

    # Build a lookup: Python module prefix -> source_dir
    module_to_source_dir: dict[str, str] = {}
    for connector in registry:
        src_dir = connector["_source_dir"]
        module_prefix = src_dir.replace("/", ".").removeprefix("src.")
        module_to_source_dir[module_prefix] = src_dir

    graph: dict[str, set[str]] = {}

    for connector in registry:
        src_dir = connector["_source_dir"]
        deps: set[str] = set()
        connector_path = mi_root / src_dir

        for py_file in connector_path.rglob("*.py"):
            try:
                source_text = py_file.read_text()
            except (OSError, UnicodeDecodeError) as e:
                print(
                    f"  WARNING: Could not read {py_file} (connector: {src_dir}): {e}",
                    file=sys.stderr,
                )
                continue

            try:
                tree = ast.parse(source_text)
            except SyntaxError:
                print(
                    f"  WARNING: Syntax error in {py_file} (connector: {src_dir}), skipping",
                    file=sys.stderr,
                )
                continue

            for node in ast.walk(tree):
                imported_modules: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    imported_modules.append(node.module)
                elif isinstance(node, ast.Import):
                    imported_modules.extend(alias.name for alias in node.names)
                else:
                    continue

                for imported_module in imported_modules:
                    for module_prefix, dep_src_dir in module_to_source_dir.items():
                        if dep_src_dir == src_dir:
                            continue
                        if imported_module.startswith(module_prefix):
                            deps.add(dep_src_dir)

        graph[src_dir] = deps

    return graph


def classify(changed_files: list[str], repo_root: Path) -> CIDecisions:
    d = CIDecisions()

    if not changed_files:
        return d

    # Classify each changed file into: connector source, integration test, safe, or unknown
    for f in changed_files:
        if f.startswith(CONNECTOR_SOURCE_PREFIX):
            continue  # handled below (connector source change)
        if f.startswith(INTEGRATION_TEST_PREFIX):
            continue  # handled below (integration test / golden file change)
        if any(f.startswith(p) for p in SAFE_PREFIXES):
            continue  # unit tests, scripts, docs — no integration tests needed
        if f.startswith("metadata-ingestion/") or f.startswith("metadata-models/"):
            d.run_all_integration = True
            return d

    # Load connector registry and import graph
    registry = load_connector_registry(repo_root)
    import_graph = build_import_graph(repo_root, registry)

    # Find which connector source directories were directly changed.
    # Only .py file changes count — non-code files (connector.yaml, .json, .md)
    # don't affect runtime behavior and shouldn't trigger tests.
    source_py_files = [
        f
        for f in changed_files
        if f.startswith(CONNECTOR_SOURCE_PREFIX) and f.endswith(".py")
    ]

    changed_source_dirs: set[str] = set()
    known_source_prefixes: list[str] = []

    for connector in registry:
        source_dir = connector["_source_dir"]
        all_source_paths = [source_dir] + connector.get("extra_source_paths", [])

        for p in all_source_paths:
            known_source_prefixes.append(f"metadata-ingestion/{p}")

        if any(
            f.startswith(f"metadata-ingestion/{p}")
            for f in source_py_files
            for p in all_source_paths
        ):
            changed_source_dirs.add(source_dir)

    # Check for .py files under source/ not covered by any connector -> full suite
    for f in source_py_files:
        if not any(f.startswith(p) for p in known_source_prefixes):
            d.run_all_integration = True
            return d

    # Resolve import-derived dependencies and build test matrix
    test_paths: set[str] = set()

    # Integration test / golden file changes → add that test dir directly
    for f in changed_files:
        if f.startswith(INTEGRATION_TEST_PREFIX):
            # Extract: metadata-ingestion/tests/integration/{connector_name}/...
            rest = f[len(INTEGRATION_TEST_PREFIX) :]
            if "/" in rest:
                test_dir_name = rest.split("/")[0]
                test_paths.add(f"tests/integration/{test_dir_name}/")

    for connector in registry:
        source_dir = connector["_source_dir"]
        deps = import_graph.get(source_dir, set())

        if source_dir in changed_source_dirs or deps & changed_source_dirs:
            if connector.get("test_path"):
                test_paths.add(connector["test_path"])
            test_paths.update(connector.get("extra_test_paths", []))
            if connector.get("is_shared_base"):
                test_paths.update(connector.get("test_paths", []))

    # Safety net: if we found changed source dirs but produced no tests,
    # something is wrong — fall back to running everything.
    if changed_source_dirs and not test_paths:
        print(
            f"  WARNING: Changed source dirs {sorted(changed_source_dirs)} produced no "
            f"test matrix entries. Falling back to run_all_integration=true.",
            file=sys.stderr,
        )
        d.run_all_integration = True
        return d

    d.test_matrix = sorted(
        [TestMatrixEntry(connector=Path(tp).name, test_path=tp) for tp in test_paths],
        key=lambda x: x["connector"],
    )
    return d


def validate(repo_root: Path) -> list[str]:
    """Validate connector.yaml files and cross-check shared base coverage."""
    errors: list[str] = []
    warnings: list[str] = []
    registry = load_connector_registry(repo_root)
    import_graph = build_import_graph(repo_root, registry)

    # 1. Every test_path declared must exist on disk
    mi = repo_root / "metadata-ingestion"
    for c in registry:
        all_paths = (
            [c.get("test_path", "")]
            + c.get("extra_test_paths", [])
            + c.get("test_paths", [])
        )
        for tp in all_paths:
            if tp and not (mi / tp).exists():
                errors.append(
                    f"{c.get('name', c['_source_dir'])}: test path '{tp}' does not exist"
                )

    # 2. Every non-shared-base connector must produce at least one test path
    for c in registry:
        if c.get("is_shared_base"):
            continue
        has_tests = bool(c.get("test_path") or c.get("extra_test_paths"))
        if not has_tests:
            errors.append(
                f"{c['_source_dir']}: registered but has no test_path or "
                f"extra_test_paths — changes will produce no tests"
            )

    # 3. Shared bases with empty test_paths must have downstream dependents
    shared_bases = {c["_source_dir"]: c for c in registry if c.get("is_shared_base")}
    for base_dir, base_conf in shared_bases.items():
        if not base_conf.get("test_paths"):
            dependents = [
                other["_source_dir"]
                for other in registry
                if base_dir in import_graph.get(other["_source_dir"], set())
            ]
            if not dependents:
                errors.append(
                    f"{base_dir}: shared base with empty test_paths and no "
                    f"detected downstream dependents — changes will produce no tests"
                )

    # 4. Warn about test dirs not covered by any connector
    tests_dir = mi / "tests" / "integration"
    if tests_dir.exists():
        for test_dir in sorted(tests_dir.iterdir()):
            if not test_dir.is_dir() or test_dir.name.startswith("_"):
                continue
            test_path = f"tests/integration/{test_dir.name}/"
            covered = any(
                test_path == c.get("test_path")
                or test_path in c.get("extra_test_paths", [])
                or test_path in c.get("test_paths", [])
                for c in registry
            )
            if not covered:
                warnings.append(
                    f"tests/integration/{test_dir.name}/ is not covered by any "
                    f"connector — it will only run when run_all_integration=true"
                )

    # Print warnings to stderr
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)

    return errors


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Selective CI checks for metadata-ingestion"
    )
    parser.add_argument(
        "--base-ref", default="master", help="Base branch to diff against"
    )
    parser.add_argument(
        "--output", choices=["json", "gha"], default="gha", help="Output format"
    )
    parser.add_argument(
        "--validate", action="store_true", help="Validate connector.yaml mappings"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without setting outputs",
    )
    parser.add_argument(
        "--force-all", action="store_true", help="Force run_all_integration=true"
    )
    args = parser.parse_args()

    # Script lives at metadata-ingestion/scripts/selective_ci_checks.py
    repo_root = Path(__file__).resolve().parent.parent.parent
    if not (repo_root / "metadata-ingestion").is_dir():
        print(
            f"ERROR: Computed repo root {repo_root} does not contain metadata-ingestion/. "
            f"Is this script in the expected location?",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.validate:
        errors = validate(repo_root)
        if errors:
            print("Validation errors:", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            sys.exit(1)
        else:
            print("All connector mappings valid.")
            sys.exit(0)

    if args.force_all:
        decisions = CIDecisions(run_all_integration=True)
    else:
        changed = get_changed_files(args.base_ref)
        decisions = classify(changed, repo_root)

    if args.dry_run or args.output == "json":
        print(json.dumps(asdict(decisions), indent=2))
    else:
        output_file = os.environ.get("GITHUB_OUTPUT")
        fh: typing.IO[str]
        if output_file:
            try:
                fh = open(output_file, "a")
            except OSError as e:
                print(
                    f"ERROR: Cannot write to GITHUB_OUTPUT '{output_file}': {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            fh = sys.stdout
        try:
            for key, value in asdict(decisions).items():
                if isinstance(value, list):
                    fh.write(f"{key}={json.dumps(value)}\n")
                else:
                    fh.write(f"{key}={str(value).lower()}\n")
        finally:
            if fh is not sys.stdout:
                fh.close()
