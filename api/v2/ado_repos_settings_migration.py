from pylon.core.tools import log
from flask import request
from sqlalchemy import text
from tools import api_tools, auth, config as c, db, context
from sqlalchemy.orm.attributes import flag_modified

from ...models.elitea_tools import EliteATool



class PromptLibAPI(api_tools.APIModeHandler):

    def _deprecate_all_ado_repos_configurations(self, pid, dry_run=False):
        """
        Deprecate all Configurations with type 'ado_repos' for a project by iterating one by one.

        Args:
            pid: Project ID
            dry_run: If True, skip actual deprecation

        Returns:
            dict with deprecation results
        """
        result = {
            "configs_found": 0,
            "configs_deprecated": 0,
            "configs_already_deprecated": 0,
            "configs_failed": 0,
            "details": []
        }

        while True:
            # Query next non-deprecated ado_repos configuration
            try:
                config = context.rpc_manager.timeout(5).configurations_get_first_filtered_project(
                    project_id=pid,
                    filter_fields={'type': 'ado_repos'}
                )
            except Exception as e:
                log.error(f"Failed to query Configuration via RPC for project {pid}: {str(e)}")
                break

            if not config:
                # No more configurations found
                break

            result["configs_found"] += 1
            config_id = config.get('id')
            config_section = config.get('section', '')
            elitea_title = config.get('elitea_title', '')

            config_detail = {
                "config_id": config_id,
                "elitea_title": elitea_title,
                "original_section": config_section
            }

            # Check if already deprecated
            if config_section and config_section.endswith('_deprecated'):
                config_detail["status"] = "already_deprecated"
                result["configs_already_deprecated"] += 1
                result["details"].append(config_detail)
                # Since it's already deprecated, we won't find it again with the same filter
                # But we need to continue to find other non-deprecated ones
                # Actually, we need to break as we're getting the same config again
                break

            if dry_run:
                config_detail["status"] = "would_deprecate"
                config_detail["new_section"] = f"{config_section}_deprecated" if config_section else "_deprecated"
                result["configs_deprecated"] += 1
                result["details"].append(config_detail)
                # In dry run, we'd get the same config again, so we need to break
                break

            # Deprecate the configuration
            if config_section:
                deprecated_section = f"{config_section}_deprecated"
            else:
                deprecated_section = "_deprecated"

            try:
                context.rpc_manager.timeout(5).configurations_update(
                    project_id=pid,
                    config_id=config_id,
                    payload={'section': deprecated_section}
                )
                log.info(f"Deprecated Configuration {config_id} (elitea_title='{elitea_title}'): section changed from '{config_section}' to '{deprecated_section}'")
                config_detail["status"] = "deprecated"
                config_detail["new_section"] = deprecated_section
                result["configs_deprecated"] += 1
            except Exception as e:
                log.error(f"Failed to deprecate Configuration {config_id}: {str(e)}")
                config_detail["status"] = "failed"
                config_detail["error"] = str(e)
                result["configs_failed"] += 1
                # Break to avoid infinite loop on persistent errors
                result["details"].append(config_detail)
                break

            result["details"].append(config_detail)

        return result

    @auth.decorators.check_api({
        "permissions": ["models.applications.migration_toolkit_names.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self):
        """
        Migrate ado_repos toolkit settings by extracting repository_id and ado_configuration from Configuration.

        Request body:
        {
            "project_id": 0,  // 0 for all projects, or specific project_id
            "dry_run": false  // If true, only report what would be changed
        }
        """

        payload = dict(request.json) if request.json else {}

        # Validate project_id
        project_id = payload.get('project_id')
        if project_id is not None:
            try:
                project_id = int(project_id)
                if project_id < 0:
                    return {"error": "Invalid project_id. Must be >= 0."}, 400
            except ValueError:
                return {"error": "Invalid project_id. Must be an integer."}, 400

        dry_run = payload.get('dry_run', False)

        # Get project IDs
        project_ids = [project_id] if project_id else [
            i['id'] for i in self.module.context.rpc_manager.call.project_list(
                filter_={'create_success': True}
            )
        ]

        log.info(f"Starting ado_repos settings migration for projects {project_ids} (dry_run={dry_run})")

        # Track migration results
        results = {
            "migrated_projects": [],
            "failed_projects": [],
            "detailed_results": {}
        }

        for pid in project_ids:
            project_result = {
                "project_id": pid,
                "toolkits_found": 0,
                "toolkits_migrated": 0,
                "toolkits_skipped": 0,
                "toolkits_failed": 0,
                "configs_deprecation_result": {},
                "details": []
            }

            try:
                # Deprecate all ado_repos configurations for this project first (regardless of toolkit existence)
                config_deprecation_result = self._deprecate_all_ado_repos_configurations(pid, dry_run)
                project_result["configs_deprecation_result"] = config_deprecation_result
                log.info(f"Configuration deprecation for project {pid}: found={config_deprecation_result['configs_found']}, deprecated={config_deprecation_result['configs_deprecated']}, already_deprecated={config_deprecation_result['configs_already_deprecated']}, failed={config_deprecation_result['configs_failed']}")

                with db.get_session(pid) as session:
                    # Find all ado_repos toolkits
                    ado_repos_tools = session.query(EliteATool).filter(
                        EliteATool.type == 'ado_repos'
                    ).all()

                    project_result["toolkits_found"] = len(ado_repos_tools)
                    log.info(f"Found {len(ado_repos_tools)} ado_repos toolkits in project {pid}")

                    for tool in ado_repos_tools:
                        tool_detail = {
                            "tool_id": tool.id,
                            "tool_name": tool.name,
                            "status": "unknown"
                        }

                        try:
                            settings = tool.settings or {}
                            ado_repos_config = settings.get('ado_repos_configuration', {})

                            # Check if already migrated (has both repository_id and ado_configuration at root level)
                            if ('repository_id' in settings and settings['repository_id'] is not None and
                                'ado_configuration' in settings and settings['ado_configuration'] is not None):
                                tool_detail["status"] = "skipped"
                                tool_detail["reason"] = "already_migrated"
                                tool_detail["repository_id"] = settings['repository_id']
                                tool_detail["ado_configuration"] = settings['ado_configuration']

                                project_result["toolkits_skipped"] += 1
                                project_result["details"].append(tool_detail)
                                continue

                            # Extract elitea_title from ado_repos_configuration
                            if not isinstance(ado_repos_config, dict):
                                log.debug(f"Tool {tool.id} has invalid ado_repos_configuration structure")
                                tool_detail["status"] = "skipped"
                                tool_detail["reason"] = "invalid_ado_repos_configuration"
                                project_result["toolkits_skipped"] += 1
                                project_result["details"].append(tool_detail)
                                continue

                            elitea_title = ado_repos_config.get('elitea_title')
                            if not elitea_title:
                                log.debug(f"Tool {tool.id} has no elitea_title in ado_repos_configuration")
                                tool_detail["status"] = "skipped"
                                tool_detail["reason"] = "no_elitea_title"
                                project_result["toolkits_skipped"] += 1
                                project_result["details"].append(tool_detail)
                                continue

                            tool_detail["elitea_title"] = elitea_title

                            # Query Configuration by elitea_title using RPC
                            try:
                                config = context.rpc_manager.timeout(5).configurations_get_first_filtered_project(
                                    project_id=pid,
                                    filter_fields={'elitea_title': elitea_title}
                                )
                            except Exception as e:
                                log.error(f"Failed to query Configuration via RPC for elitea_title '{elitea_title}': {str(e)}")
                                config = None

                            # Handle configuration not found
                            if not config:
                                log.error(f"Configuration with elitea_title '{elitea_title}' not found for tool {tool.id}")
                                repository_id = None
                                ado_configuration = None
                                tool_detail["status"] = "migrated_with_warning"
                                tool_detail["warning"] = f"Configuration '{elitea_title}' not found, repository_id and ado_configuration set to null"
                            else:
                                # Extract repository_id and ado_configuration from Configuration data (RPC returns dict)
                                config_data = config.get('data') or {}
                                repository_id = config_data.get('repository_id', '')
                                ado_configuration = config_data.get('ado_configuration', {})

                                if not repository_id:
                                    log.debug(f"Configuration '{elitea_title}' has no repository_id, using empty string")
                                    repository_id = ''

                                if not ado_configuration:
                                    log.debug(f"Configuration '{elitea_title}' has no ado_configuration, using empty dict")
                                    ado_configuration = {}

                                tool_detail["status"] = "migrated"

                            tool_detail["repository_id"] = repository_id
                            tool_detail["ado_configuration"] = ado_configuration
                            tool_detail["old_settings"] = {
                                "had_repository_id": 'repository_id' in settings,
                                "had_ado_configuration": 'ado_configuration' in settings,
                                "had_ado_repos_configuration": 'ado_repos_configuration' in settings
                            }

                            # Update settings (backwards compatible - keep ado_repos_configuration)
                            if not dry_run:
                                settings['repository_id'] = repository_id
                                settings['ado_configuration'] = ado_configuration
                                tool.settings = settings
                                flag_modified(tool, 'settings')
                                session.flush()  # Ensure change is written to session
                                log.debug(f"Updated tool {tool.id} with repository_id={repository_id}, ado_configuration={ado_configuration}")


                            tool_detail["new_settings"] = {
                                "repository_id": repository_id,
                                "ado_configuration": ado_configuration,
                                "ado_repos_configuration_preserved": True
                            }

                            project_result["toolkits_migrated"] += 1
                            project_result["details"].append(tool_detail)

                        except Exception as e:
                            log.error(f"Error processing tool {tool.id}: {str(e)}")
                            tool_detail["status"] = "failed"
                            tool_detail["error"] = str(e)
                            project_result["toolkits_failed"] += 1
                            project_result["details"].append(tool_detail)

                    # Commit changes if not dry_run
                    if not dry_run and project_result["toolkits_migrated"] > 0:
                        session.commit()
                        log.info(f"Committed {project_result['toolkits_migrated']} toolkit updates for project {pid}")
                    elif dry_run:
                        log.info(f"Dry run: would migrate {project_result['toolkits_migrated']} toolkits in project {pid}")

                # Mark project as successful if any toolkits were processed
                if project_result["toolkits_failed"] == 0:
                    results["migrated_projects"].append(pid)
                else:
                    results["failed_projects"].append({
                        "project_id": pid,
                        "error": f"{project_result['toolkits_failed']} toolkit(s) failed to migrate"
                    })

                results["detailed_results"][pid] = project_result

            except Exception as e:
                log.error(f"Unexpected error for project {pid}: {str(e)}")
                results["failed_projects"].append({
                    "project_id": pid,
                    "error": str(e)
                })
                results["detailed_results"][pid] = {
                    "project_id": pid,
                    "error": str(e),
                    "toolkits_found": 0,
                    "toolkits_migrated": 0,
                    "toolkits_skipped": 0,
                    "toolkits_failed": 0
                }

        # Build summary response
        total_found = sum(r["toolkits_found"] for r in results["detailed_results"].values())
        total_migrated = sum(r["toolkits_migrated"] for r in results["detailed_results"].values())
        total_skipped = sum(r["toolkits_skipped"] for r in results["detailed_results"].values())
        total_failed = sum(r["toolkits_failed"] for r in results["detailed_results"].values())

        response = {
            "message": "ado_repos settings migration completed" if not dry_run else "ado_repos settings migration dry run completed",
            "dry_run": dry_run,
            "summary": {
                "total_projects": len(project_ids),
                "migrated_projects_count": len(results["migrated_projects"]),
                "failed_projects_count": len(results["failed_projects"]),
                "total_toolkits_found": total_found,
                "total_toolkits_migrated": total_migrated,
                "total_toolkits_skipped": total_skipped,
                "total_toolkits_failed": total_failed
            },
            "migrated_projects": results["migrated_projects"],
            "failed_projects": results["failed_projects"],
            "detailed_results": results["detailed_results"]
        }

        status_code = 200 if len(results["failed_projects"]) == 0 else 207  # 207 Multi-Status for partial success

        return response, status_code


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }

