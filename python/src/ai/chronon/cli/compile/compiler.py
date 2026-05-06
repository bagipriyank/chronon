import os
import shutil
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ai.chronon.cli.compile.display.compiled_obj
import ai.chronon.cli.compile.parse_configs as parser
import ai.chronon.cli.logger as logger
from ai.chronon.cli.compile import serializer
from ai.chronon.cli.compile.compile_context import CompileContext, ConfigInfo
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj
from ai.chronon.cli.compile.parse_teams import merge_team_execution_info
from ai.chronon.cli.formatter import Format, PromptException
from ai.chronon.cli.theme import console
from ai.chronon.types import MetaData
from gen_thrift.api.ttypes import ConfType

logger = logger.get_logger()


@dataclass
class CompileResult:
    config_info: ConfigInfo
    obj_dict: Dict[str, Any]
    error_dict: Dict[str, List[BaseException]]


class Compiler:
    def __init__(self, compile_context: CompileContext):
        self.compile_context = compile_context

    def compile(self, dry_run=False, validate_all=False) -> Dict[ConfType, CompileResult]:
        # Clean staging directory at the start to ensure fresh compilation
        staging_dir = self.compile_context.staging_output_dir()
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

        config_infos = self.compile_context.config_infos

        compile_results = {}
        all_compiled_objects = []  # Collect all compiled objects for change validation

        for config_info in config_infos:
            configs, compiled_objects = self._compile_class_configs(config_info)
            compile_results[config_info.config_type] = configs

            # Collect compiled objects for change validation
            all_compiled_objects.extend(compiled_objects)

        # Validate changes once after all classes have been processed
        self.compile_context.validator.validate_changes(all_compiled_objects, validate_all)

        # Show the nice display first
        if self.compile_context.format != Format.JSON:
            console.print(
                self.compile_context.compile_status.render(
                    self.compile_context.ignore_python_errors
                )
            )

        # Check for confirmation before finalizing files
        if not self.compile_context.force:
            if self.compile_context.format != Format.JSON:
                self.compile_context.validator.check_pending_changes_confirmation(
                    self.compile_context.compile_status
                )
            else:
                # In case of JSON format we need to prompt the user for confirmation if changes are not versioned.
                non_version_changes = self.compile_context.validator._non_version_changes()
                if non_version_changes:
                    raise PromptException(
                        prompt=f"The following configs are changing in-place (changing semantics without changing the version)."
                        f" {', '.join([v.name for v in non_version_changes])} Do you want to proceed?",
                        options=["yes", "no"],
                        instructions="If 'yes' run with --force to proceed with the compilation.",
                    )

        # Only proceed with file operations if there are no compilation errors
        if dry_run:
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)
        elif not self.has_compilation_errors() or self.compile_context.ignore_python_errors:
            self._compile_team_metadata()

            # check if staging_output_dir exists
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                # replace staging_output_dir to output_dir
                output_dir = self.compile_context.output_dir()
                if os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                shutil.move(staging_dir, output_dir)
                if self.compile_context.format != Format.JSON:
                    console.print(f"Compilation successful. Compiled files saved to {output_dir}")
            else:
                if self.compile_context.format != Format.JSON:
                    console.print(
                        f"Staging directory {staging_dir} does not exist. "
                        "Happens when every chronon config fails to compile or when no chronon configs exist."
                    )
        else:
            # Clean up staging directory when there are errors (don't move to output)
            if self.compile_context.format != Format.JSON:
                console.print("Compilation errors detected. Not saving compiled files to output directory.")
            staging_dir = self.compile_context.staging_output_dir()
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

        return compile_results

    def has_compilation_errors(self) -> bool:
        """Check if there are any compilation errors across all class trackers."""
        for tracker in self.compile_context.compile_status.cls_to_tracker.values():
            if tracker.files_to_errors:
                return True
        return False

    def _compile_team_metadata(self):
        """
        Compile the team metadata and return the compiled object.
        """
        teams_dict = self.compile_context.teams_dict
        for team in teams_dict:
            m = MetaData()
            merge_team_execution_info(m, teams_dict, team)

            tjson = serializer.thrift_simple_json(m)
            name = f"{team}.{team}_team_metadata"
            result = CompiledObj(
                name=name,
                obj=m,
                file=name,
                errors=None,
                obj_type=MetaData.__name__,
                tjson=tjson,
            )
            self._write_object(result)
            self.compile_context.compile_status.add_object_update_display(result, MetaData.__name__)

        # Done writing team metadata, close the class
        self.compile_context.compile_status.close_cls(MetaData.__name__)

    def _compile_class_configs(
        self, config_info: ConfigInfo
    ) -> Tuple[CompileResult, List[CompiledObj]]:
        compile_result = CompileResult(config_info=config_info, obj_dict={}, error_dict={})

        input_dir = self.compile_context.input_dir(config_info.cls)

        compiled_objects = parser.from_folder(config_info.cls, input_dir, self.compile_context)

        objects, errors = self._write_objects_in_folder(compiled_objects)

        if objects:
            compile_result.obj_dict.update(objects)

        if errors:
            compile_result.error_dict.update(errors)

        self.compile_context.compile_status.close_cls(config_info.cls.__name__)

        return compile_result, compiled_objects

    def _write_objects_in_folder(
        self,
        compiled_objects: List[ai.chronon.cli.compile.display.compiled_obj.CompiledObj],
    ) -> Tuple[Dict[str, Any], Dict[str, List[BaseException]]]:
        error_dict = {}
        object_dict = {}

        for co in compiled_objects:
            if co.obj:
                if co.errors:
                    error_dict[co.name] = co.errors

                    if self.compile_context.format != Format.JSON:
                        for error in co.errors:
                            self.compile_context.compile_status.print_live_console(
                                f"Error processing conf {co.name}: {error}"
                            )
                            traceback.print_exception(type(error), error, error.__traceback__)

                else:
                    self._write_object(co)
                    object_dict[co.name] = co.obj
            else:
                error_dict[co.file] = co.errors

                if self.compile_context.format != Format.JSON:
                    self.compile_context.compile_status.print_live_console(
                        f"Error processing file {co.file}: {co.errors}"
                    )
                    for error in co.errors:
                        traceback.print_exception(type(error), error, error.__traceback__)

        return object_dict, error_dict

    def _write_object(self, compiled_obj: CompiledObj) -> Optional[List[BaseException]]:
        output_path = self.compile_context.staging_output_path(compiled_obj)

        folder = os.path.dirname(output_path)

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(output_path, "w") as f:
            f.write(compiled_obj.tjson)
