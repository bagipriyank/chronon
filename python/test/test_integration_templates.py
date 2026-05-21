"""Unit tests for integration test template generation."""

import os

import pytest
from click.testing import CliRunner

from integration.helpers.cli import compile_configs
from integration.helpers.templates import (
    _apply_renames,
    _discover_sources,
    _find_imported_stems,
    cleanup_test_configs,
    generate_test_configs,
    resolve_conf,
)

_canary_root = os.path.join(os.path.dirname(__file__), "canary")


class TestConfResolver:
    """Test the resolve_conf helper."""

    def test_resolve_conf_with_version(self):
        assert resolve_conf("compiled/joins/gcp/demo.v1__1", "abc") == "compiled/joins/gcp/demo_abc.v1__1"

    def test_resolve_conf_without_version(self):
        assert resolve_conf("compiled/joins/azure/demo.v2", "abc") == "compiled/joins/azure/demo_abc.v2"

    def test_resolve_conf_no_dot(self):
        assert resolve_conf("compiled/joins/gcp/demo", "abc") == "compiled/joins/gcp/demo_abc"

    def test_resolve_staging_query(self):
        assert resolve_conf("compiled/staging_queries/aws/exports.dim_listings__0", "abc") == \
            "compiled/staging_queries/aws/exports_abc.dim_listings__0"


class TestCompiledConfsExist:
    """Generate templates, compile, and verify test-scoped confs are produced."""

    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", _canary_root)
        monkeypatch.setenv("ARTIFACT_PREFIX", "gs://test-artifacts")
        monkeypatch.setenv("CUSTOMER_ID", "test")
        monkeypatch.syspath_prepend(_canary_root)

    def _cleanup_compiled(self, tid):
        compiled_dir = os.path.join(_canary_root, "compiled")
        if not os.path.isdir(compiled_dir):
            return
        for root, _, files in os.walk(compiled_dir):
            for f in files:
                if tid in f:
                    os.remove(os.path.join(root, f))

    def _compile_and_check(self, cloud):
        tid = "ctest"
        try:
            generate_test_configs(tid, _canary_root, cloud=cloud)
            runner = CliRunner()
            compile_configs(runner, _canary_root, clean=False)

            compiled_dir = os.path.join(_canary_root, "compiled")
            actually_produced = []
            for root, _, files in os.walk(compiled_dir):
                for f in files:
                    if tid in f:
                        actually_produced.append(
                            os.path.relpath(os.path.join(root, f), _canary_root)
                        )

            assert actually_produced, (
                f"No compiled confs produced for cloud={cloud} with test_id={tid}. "
                f"Check that _discover_sources finds .py files under the canary config dirs."
            )
        finally:
            cleanup_test_configs(tid, _canary_root, cloud=cloud)
            self._cleanup_compiled(tid)

    def test_gcp_confs(self):
        self._compile_and_check("gcp")

    def test_aws_confs(self):
        self._compile_and_check("aws")

    def test_azure_confs(self):
        self._compile_and_check("azure")

    def test_k8s_confs(self):
        self._compile_and_check("k8s")


class TestDiscoverSources:
    """Verify _discover_sources finds config files for each cloud."""

    def test_gcp_has_sources(self):
        sources = _discover_sources(_canary_root, "gcp")
        stems = {os.path.basename(s) for s in sources}
        assert "exports.py" in stems
        assert "dim_listings.py" in stems
        assert "demo.py" in stems
        assert "__init__.py" not in stems

    def test_aws_has_sources(self):
        sources = _discover_sources(_canary_root, "aws")
        stems = {os.path.basename(s) for s in sources}
        assert "exports.py" in stems
        assert "dim_listings.py" in stems
        assert "demo.py" in stems

    def test_azure_has_sources(self):
        sources = _discover_sources(_canary_root, "azure")
        stems = {os.path.basename(s) for s in sources}
        assert "exports.py" in stems
        assert "dim_listings.py" in stems
        assert "demo.py" in stems

    def test_k8s_has_sources(self):
        sources = _discover_sources(_canary_root, "k8s")
        stems = {os.path.basename(s) for s in sources}
        assert "exports.py" in stems
        assert "purchases.py" in stems
        assert "training_set.py" in stems


class TestRewriteImports:
    def test_simple_rename(self):
        content = "from staging_queries.gcp import purchases_import\nv = purchases_import.v1"
        renames = {"purchases_import": "purchases_import_abc123"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "import purchases_import_abc123" in result
        assert "purchases_import_abc123.v1" in result

    def test_multiple_renames(self):
        content = "import purchases_import, purchases_notds_import"
        renames = {
            "purchases_import": "purchases_import_xyz",
            "purchases_notds_import": "purchases_notds_import_xyz",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert result == "import purchases_import_xyz, purchases_notds_import_xyz"

    def test_word_boundary_prevents_partial_match(self):
        """purchases should not match inside purchases_import."""
        content = "from staging_queries.gcp import purchases_import, purchases_notds_import"
        renames = {"purchases": "purchases_test1"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        # purchases_import and purchases_notds_import should NOT be affected
        assert result == content

    def test_word_boundary_matches_standalone(self):
        """purchases should match when it appears as a standalone word."""
        content = "from group_bys.gcp import purchases\nJoinPart(group_by=purchases.v1_test)"
        renames = {"purchases": "purchases_test1"}
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "import purchases_test1" in result
        assert "purchases_test1.v1_test" in result

    def test_longest_first_ordering(self):
        """Longer names are replaced first to prevent double-suffixing."""
        content = "import checkouts_import, checkouts_notds_import\ncheckouts_import and checkouts_notds_import"
        renames = {
            "checkouts_import": "checkouts_import_t1",
            "checkouts_notds_import": "checkouts_notds_import_t1",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        assert "checkouts_import_t1 and checkouts_notds_import_t1" in result

    def test_only_imported_stems_are_rewritten(self):
        """Variable names matching a module stem should not be rewritten
        if the file doesn't import that module."""
        content = (
            "from staging_queries.gcp import exports\n"
            "user_activities = exports.user_activities\n"
        )
        renames = {
            "exports": "exports_t1",
            "user_activities": "user_activities_t1",
        }
        found = _find_imported_stems(content, renames)
        result = _apply_renames(content, found)
        # exports is imported -> should be renamed
        assert "import exports_t1" in result
        assert "exports_t1.user_activities" in result
        # user_activities is NOT imported -> the variable name stays
        assert "user_activities = " in result
        assert "user_activities_t1 = " not in result

    def test_no_renames_passthrough(self):
        content = "some arbitrary content"
        result = _apply_renames(content, {})
        assert result == content

    def test_jinja2_literals_survive(self):
        """{{ start_date }} and {{ end_date }} pass through unchanged."""
        content = 'import some_module\nquery="SELECT * WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}"'
        found = _find_imported_stems(content, {"some_module": "some_module_test1"})
        result = _apply_renames(content, found)
        assert "{{ start_date }}" in result
        assert "{{ end_date }}" in result


class TestGenerateAndCleanup:
    def test_round_trip(self, tmp_path):
        """generate_test_configs copies files, cleanup deletes the copies."""
        # Set up a minimal canary structure
        sq_dir = tmp_path / "staging_queries" / "gcp"
        sq_dir.mkdir(parents=True)
        gb_dir = tmp_path / "group_bys" / "gcp"
        gb_dir.mkdir(parents=True)
        join_dir = tmp_path / "joins" / "gcp"
        join_dir.mkdir(parents=True)

        # Create minimal source files
        (sq_dir / "purchases_import.py").write_text("v1 = 'purchases_import'")
        (sq_dir / "purchases_notds_import.py").write_text("v1 = 'purchases_notds_import'")
        (sq_dir / "checkouts_import.py").write_text("v1 = 'checkouts_import'")
        (sq_dir / "checkouts_notds_import.py").write_text("v1 = 'checkouts_notds_import'")
        (sq_dir / "exports.py").write_text("v1 = 'exports'")
        (gb_dir / "purchases.py").write_text(
            "from staging_queries.gcp import purchases_import\nv1 = purchases_import"
        )
        (join_dir / "training_set.py").write_text(
            "from group_bys.gcp import purchases\nv1 = purchases"
        )
        (join_dir / "demo.py").write_text("v1 = 'demo'")

        tid = "abc12345"
        generated = generate_test_configs(tid, str(tmp_path), cloud="gcp")
        assert len(generated) == 8

        # Original files should still exist (untouched)
        assert (sq_dir / "exports.py").exists()
        assert (gb_dir / "purchases.py").exists()

        # Copies should exist
        assert (sq_dir / f"exports_{tid}.py").exists()
        assert (gb_dir / f"purchases_{tid}.py").exists()

        # Verify import renames propagated into copies
        gb_content = (gb_dir / f"purchases_{tid}.py").read_text()
        assert f"purchases_import_{tid}" in gb_content

        join_content = (join_dir / f"training_set_{tid}.py").read_text()
        assert f"purchases_{tid}" in join_content

        # Originals should NOT be modified
        gb_orig = (gb_dir / "purchases.py").read_text()
        assert f"purchases_import_{tid}" not in gb_orig
        assert "purchases_import" in gb_orig

        # Cleanup should delete copies
        deleted = cleanup_test_configs(tid, str(tmp_path), cloud="gcp")
        assert len(deleted) == 8

        # Original files should still exist
        assert (sq_dir / "exports.py").exists()
        assert (gb_dir / "purchases.py").exists()

        # Copies should be gone
        assert not (sq_dir / f"exports_{tid}.py").exists()
        assert not (gb_dir / f"purchases_{tid}.py").exists()
