import os
from unittest.mock import MagicMock, patch

import pytest

from ai.chronon.repo.k8s_runner import (
    DEFAULT_K8S_SUBMISSION_MODE,
    K8S_ENTRY,
    K8S_SUBMISSION_MODE_ENV,
    K8sRunner,
)

SAMPLE_CONF = "compiled/group_bys/sample_team/sample_group_by.v1__0"


def _make_k8s_env(file_upload_path="gs://my-bucket/spark-staging"):
    return {
        "SPARK_K8S_MASTER": "k8s://https://10.0.0.1:6443",
        "SPARK_K8S_NAMESPACE": "spark-jobs",
        "SPARK_K8S_SERVICE_ACCOUNT": "spark-sa",
        "SPARK_K8S_IMAGE": "registry.example.com/spark:latest",
        "SPARK_K8S_FILE_UPLOAD_PATH": file_upload_path,
    }


@pytest.fixture
def k8s_env(monkeypatch):
    for k, v in _make_k8s_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("CUSTOMER_ID", "test")


@pytest.fixture
def runner_args(repo):
    return {
        "repo": repo,
        "conf": SAMPLE_CONF,
        "mode": "backfill",
        "env": "dev",
        "ds": "2024-01-01",
        "end_ds": "2024-01-01",
        "start_ds": None,
        "app_name": "test_app",
        "online_jar": "/tmp/test.jar",
        "online_class": "com.example.MyApiImpl",
        "online_jar_fetch": None,
        "sub_help": False,
        "conf_type": "group_bys",
        "args": "",
        "spark_submit_path": "spark-submit",
        "spark_streaming_submit_path": "spark-submit",
        "list_apps": "echo []",
        "render_info": "scripts/render_info.py",
        "online_args": None,
        "parallelism": None,
        "kafka_bootstrap": None,
        "latest_savepoint": None,
        "custom_savepoint": None,
        "no_savepoint": None,
        "version_check": None,
        "additional_jars": None,
        "flink_jars_uri": None,
        "mock_source": None,
        "validate": None,
        "validate_rows": None,
        "no_cloud_logging": False,
        "debug": False,
        "uploader": None,
        "warehouse_bucket": None,
        "version": "1.2.3",
        "k8s_submission_mode": None,
    }


class TestEnvValidation:
    def test_missing_all_env_vars_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="K8sRunner requires these environment variables"):
                K8sRunner._validate_env()

    def test_missing_one_env_var_lists_it(self, k8s_env, monkeypatch):
        monkeypatch.delenv("SPARK_K8S_IMAGE")
        with pytest.raises(ValueError, match="SPARK_K8S_IMAGE"):
            K8sRunner._validate_env()

    def test_all_env_vars_set_passes(self, k8s_env):
        K8sRunner._validate_env()


class TestNoK8sEnvRequired:
    def test_info_mode_without_k8s_env(self, runner_args, monkeypatch):
        runner_args["mode"] = "info"
        monkeypatch.setenv("CUSTOMER_ID", "test")
        monkeypatch.setattr(os.path, "exists", lambda _: True)
        captured = {}
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.check_call", lambda cmd: captured.update(cmd=cmd))

        K8sRunner(runner_args).run()

        assert "python3" in captured["cmd"]
        assert K8S_ENTRY not in captured["cmd"]

    def test_fetch_mode_without_k8s_env(self, runner_args, monkeypatch):
        runner_args["mode"] = "fetch"
        monkeypatch.setenv("CUSTOMER_ID", "test")
        monkeypatch.setattr(os.path, "exists", lambda _: True)
        captured = {}
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.check_call", lambda cmd: captured.update(cmd=cmd))

        K8sRunner(runner_args).run()

        assert "ai.chronon.online.fetcher.FetcherMain" in captured["cmd"]


class TestParseExtraConf:
    def test_empty_string(self):
        assert K8sRunner._parse_extra_conf("") == {}

    def test_single_pair(self):
        assert K8sRunner._parse_extra_conf("spark.executor.memory=4g") == {"spark.executor.memory": "4g"}

    def test_multiple_pairs(self):
        raw = "spark.executor.memory=4g;spark.driver.memory=2g"
        result = K8sRunner._parse_extra_conf(raw)
        assert result == {
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
        }

    def test_value_with_comma(self):
        raw = "spark.jars=a.jar,b.jar;spark.executor.memory=4g"
        result = K8sRunner._parse_extra_conf(raw)
        assert result == {
            "spark.jars": "a.jar,b.jar",
            "spark.executor.memory": "4g",
        }

    def test_legacy_comma_delimiter_raises(self):
        with pytest.raises(ValueError, match="semicolon-delimited"):
            K8sRunner._parse_extra_conf("spark.executor.memory=4g,spark.driver.memory=2g")

    def test_malformed_token_raises(self):
        with pytest.raises(ValueError, match="Malformed token"):
            K8sRunner._parse_extra_conf("spark.executor.memory=4g;bad-token")


class TestStageConfig:
    @pytest.mark.parametrize(
        "upload_path",
        [
            "gs://my-bucket/spark-staging",
            "s3://my-bucket/spark-staging",
            "abfss://container@account.dfs.core.windows.net/staging",
        ],
        ids=["gcs", "s3", "azure"],
    )
    def test_stage_config_calls_upload(self, runner_args, monkeypatch, upload_path):
        for k, v in _make_k8s_env(file_upload_path=upload_path).items():
            monkeypatch.setenv(k, v)
        monkeypatch.setenv("CUSTOMER_ID", "test")
        monkeypatch.setattr(os.path, "exists", lambda _: True)

        runner = K8sRunner(runner_args)
        runner._load_k8s_config()
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        local_path = os.path.join(runner_args["repo"], SAMPLE_CONF)
        result = runner._stage_config(local_path)

        mock_upload.assert_called_once()
        actual_remote = mock_upload.call_args[0][1]
        assert actual_remote.startswith(upload_path + "/")
        assert actual_remote.endswith("/sample_group_by.v1__0")
        assert result == actual_remote


class TestRunDispatch:
    @pytest.fixture
    def captured_check_call(self, monkeypatch):
        monkeypatch.setattr(os.path, "exists", lambda _: True)
        captured = {}

        def _mock(cmd, env=None):
            captured["cmd"] = cmd
            captured["env"] = env

        monkeypatch.setattr("ai.chronon.repo.k8s_runner.check_call", _mock)
        return captured

    def test_full_run_backfill_invokes_k8s_submitter(self, k8s_env, runner_args, monkeypatch, captured_check_call):
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        K8sRunner(runner_args).run()

        cmd = captured_check_call["cmd"]
        assert f"java -cp /tmp/test.jar {K8S_ENTRY}" in cmd
        assert "--job-type=spark" in cmd
        assert "--main-class=ai.chronon.spark.Driver" in cmd
        assert "--zipline-version=1.2.3" in cmd
        assert "--conf-path=sample_group_by.v1__0" in cmd
        assert "--conf-path=compiled/" not in cmd
        assert "--files=" in cmd

    def test_online_jar_and_class_passthrough(self, k8s_env, runner_args, monkeypatch, captured_check_call):
        runner_args["mode"] = "upload-to-kv"
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        K8sRunner(runner_args).run()

        cmd = captured_check_call["cmd"]
        assert "--online-jar=/tmp/test.jar" in cmd
        assert "--online-class=com.example.MyApiImpl" in cmd

    def test_info_mode_does_not_invoke_k8s_submitter(self, k8s_env, runner_args, captured_check_call):
        runner_args["mode"] = "info"
        runner_args["render_info"] = "scripts/render_info.py"

        K8sRunner(runner_args).run()

        cmd = captured_check_call["cmd"]
        assert K8S_ENTRY not in cmd
        assert "python3" in cmd

    def test_sub_help_uses_spark_driver_entrypoint(self, k8s_env, runner_args, captured_check_call):
        runner_args["sub_help"] = True
        runner_args["mode"] = "backfill"

        K8sRunner(runner_args).run()

        cmd = captured_check_call["cmd"]
        assert "ai.chronon.spark.Driver" in cmd
        assert "--help" in cmd

    def test_fetch_mode_uses_fetcher_entrypoint(self, k8s_env, runner_args, captured_check_call):
        runner_args["mode"] = "fetch"

        K8sRunner(runner_args).run()

        cmd = captured_check_call["cmd"]
        assert "ai.chronon.online.fetcher.FetcherMain" in cmd
        assert "ai.chronon.spark.Driver" not in cmd

    def test_missing_app_jar_raises(self, k8s_env, runner_args, captured_check_call):
        runner_args["online_jar"] = None

        with pytest.raises(ValueError, match="requires an application JAR"):
            K8sRunner(runner_args).run()

    def test_missing_jar_sub_help_raises(self, k8s_env, runner_args, captured_check_call):
        runner_args["online_jar"] = None
        runner_args["sub_help"] = True

        with pytest.raises(ValueError, match="requires a JAR"):
            K8sRunner(runner_args).run()

    def test_streaming_raises(self, k8s_env, runner_args, captured_check_call):
        runner_args["mode"] = "streaming"

        with pytest.raises(ValueError, match="Streaming is not yet supported"):
            K8sRunner(runner_args).run()

    def test_k8s_submitter_sets_default_submission_mode_env(self, k8s_env, runner_args, monkeypatch, captured_check_call):
        monkeypatch.delenv(K8S_SUBMISSION_MODE_ENV, raising=False)
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        K8sRunner(runner_args).run()

        assert captured_check_call["env"] is not None
        assert captured_check_call["env"][K8S_SUBMISSION_MODE_ENV] == DEFAULT_K8S_SUBMISSION_MODE

    def test_k8s_submission_mode_cli_override(self, k8s_env, runner_args, monkeypatch, captured_check_call):
        monkeypatch.setenv(K8S_SUBMISSION_MODE_ENV, "spark-submit")
        runner_args["k8s_submission_mode"] = "spark-operator"
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        K8sRunner(runner_args).run()

        assert captured_check_call["env"][K8S_SUBMISSION_MODE_ENV] == "spark-operator"

    def test_k8s_submission_mode_inherits_parent_env(self, k8s_env, runner_args, monkeypatch, captured_check_call):
        monkeypatch.setenv(K8S_SUBMISSION_MODE_ENV, "spark-operator")
        mock_upload = MagicMock(side_effect=lambda local, remote: remote)
        monkeypatch.setattr("ai.chronon.repo.k8s_runner.upload_to_blob_store", mock_upload)

        K8sRunner(runner_args).run()

        assert captured_check_call["env"][K8S_SUBMISSION_MODE_ENV] == "spark-operator"

    def test_info_mode_does_not_set_submission_mode_env(self, k8s_env, runner_args, captured_check_call):
        runner_args["mode"] = "info"
        runner_args["render_info"] = "scripts/render_info.py"

        K8sRunner(runner_args).run()

        assert captured_check_call.get("env") is None
