import os
import uuid

from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import ROUTES
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import check_call, extract_filename_from_path, upload_to_blob_store

LOG = get_logger()

K8S_ENTRY = "ai.chronon.integrations.cloud_k8s.K8sSubmitter"
ZIPLINE_K8S_JAR_DEFAULT = "cloud_k8s_lib_deploy.jar"
K8S_SUBMISSION_MODE_ENV = "CHRONON_K8S_SUBMISSION_MODE"
DEFAULT_K8S_SUBMISSION_MODE = "spark-submit"

REQUIRED_ENV_VARS = [
    "SPARK_K8S_MASTER",
    "SPARK_K8S_IMAGE",
    "SPARK_K8S_FILE_UPLOAD_PATH",
]


class K8sRunner(Runner):
    def __init__(self, args):
        jar_path = args.get("online_jar") or ""
        self.job_id = str(uuid.uuid4())
        self._version = args.get("version") or "local"
        self._k8s_submission_mode = args.get("k8s_submission_mode")
        super().__init__(args, os.path.expanduser(jar_path))

    def _resolved_k8s_submission_mode(self) -> str:
        explicit = self._k8s_submission_mode
        if explicit is not None and str(explicit).strip():
            return str(explicit).strip()
        inherited = os.environ.get(K8S_SUBMISSION_MODE_ENV)
        if inherited is not None and str(inherited).strip():
            return str(inherited).strip()
        return DEFAULT_K8S_SUBMISSION_MODE

    def _subprocess_env_for_k8s_submitter(self):
        env = os.environ.copy()
        env[K8S_SUBMISSION_MODE_ENV] = self._resolved_k8s_submission_mode()
        return env

    def _load_k8s_config(self):
        self._validate_env()
        self._file_upload_path = os.environ["SPARK_K8S_FILE_UPLOAD_PATH"]
        _ = self._parse_extra_conf(os.environ.get("SPARK_K8S_CONF", ""))

    @staticmethod
    def _validate_env():
        missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
        if missing:
            raise ValueError(f"K8sRunner requires these environment variables to be set: {missing}")

    @staticmethod
    def _parse_extra_conf(raw: str) -> dict:
        if not raw.strip():
            return {}
        if ";" not in raw and "," in raw:
            raise ValueError("SPARK_K8S_CONF must be semicolon-delimited (;) not comma-delimited (,)")

        result = {}
        for pair in raw.split(";"):
            pair = pair.strip()
            if not pair:
                continue
            if "=" not in pair:
                raise ValueError(f"Malformed token in SPARK_K8S_CONF: {pair!r} (expected key=value)")
            k, v = pair.split("=", 1)
            k, v = k.strip(), v.strip()
            if not k:
                raise ValueError(f"Empty key in SPARK_K8S_CONF token: {pair!r}")
            result[k] = v
        return result

    def _stage_config(self, local_path: str) -> str:
        filename = extract_filename_from_path(local_path)
        run_id = uuid.uuid4().hex[:12]
        remote_uri = f"{self._file_upload_path.rstrip('/')}/{run_id}/{filename}"
        LOG.info(f"Staging config {local_path} -> {remote_uri}")
        return upload_to_blob_store(local_path, remote_uri)

    def generate_k8s_submitter_args(self, user_args: str, metadata_conf_path: str = None):
        staged_files = []
        if metadata_conf_path:
            staged_files.append(self._stage_config(metadata_conf_path))

        file_args = f" --files={','.join(staged_files)}" if staged_files else ""

        final_args = (
            "{user_args} --jar-uri={jar_uri} --job-type=spark --main-class=ai.chronon.spark.Driver "
            "--zipline-version={zipline_version} --job-id={job_id}"
        )
        return final_args.format(
            user_args=user_args,
            jar_uri=self.jar_path,
            zipline_version=self._version,
            job_id=self.job_id,
        ) + file_args

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or self.mode == "fetch":
            if not self.jar_path:
                raise ValueError(
                    "K8sRunner requires a JAR for sub-help / fetch. Set CHRONON_ONLINE_JAR or pass --online-jar."
                )
            entrypoint = "ai.chronon.online.fetcher.FetcherMain" if self.mode == "fetch" else "ai.chronon.spark.Driver"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        elif self.mode in ["streaming", "streaming-client"]:
            raise ValueError("Streaming is not yet supported for K8s runner.")
        else:
            if not self.jar_path:
                raise ValueError(
                    "K8sRunner requires an application JAR for K8sSubmitter. "
                    "Set CHRONON_ONLINE_JAR or pass --online-jar."
                )

            self._load_k8s_config()
            remote_conf_path = extract_filename_from_path(self.conf) if self.conf else None
            user_args = "{subcommand} {args} {additional_args}".format(
                subcommand=ROUTES[self.conf_type][self.mode],
                args=self._gen_final_args(start_ds=self.start_ds, override_conf_path=remote_conf_path),
                additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
            )
            submitter_args = self.generate_k8s_submitter_args(
                user_args=user_args,
                metadata_conf_path=self.local_abs_conf_path if self.conf else None,
            )
            command = f"java -cp {self.jar_path} {K8S_ENTRY} {submitter_args}"
            command_list.append(command)

        if len(command_list) == 1:
            cmd = command_list[0]
            if K8S_ENTRY in cmd:
                check_call(cmd, env=self._subprocess_env_for_k8s_submitter())
            else:
                check_call(cmd)
        elif len(command_list) > 1:
            raise ValueError("Parallel execution is not yet supported for K8s runner.")
