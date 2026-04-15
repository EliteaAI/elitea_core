from pylon.core.tools import log
from flask import request
from tools import api_tools, auth, config as c, db

from ...models.elitea_tools import EliteATool


# Mapping of cloud boolean to hosting string
# cloud: true -> "Cloud", cloud: false -> "Server"
CLOUD_TO_HOSTING = {
    True: "Cloud",
    False: "Server"
}

# Toolkit types and their corresponding configuration keys
TOOLKIT_CONFIG_MAPPING = {
    # Jira toolkits
    "jira": "jira_configuration",
    # Confluence toolkits
    "confluence": "confluence_configuration",
}


class PromptLibAPI(api_tools.APIModeHandler):
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
        Migrate 'cloud' boolean from Jira/Confluence toolkit settings to 'hosting' field
        in the corresponding configuration.

        This migration:
        1. Finds Jira/Confluence toolkits with 'cloud' field in settings
        2. Maps cloud boolean to hosting string: true -> "Cloud", false -> "Server"
        3. Updates the corresponding configuration's data.hosting field
        4. Removes the 'cloud' field from toolkit settings

        Request body:
        {
            "project_id": 0,  // 0 for all projects, or specific project_id
            "dry_run": false  // If true, only report what would be changed
        }
        """
        # Import Configuration model here to avoid circular imports
        from plugins.configurations.models.configuration import Configuration

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

        log.info(f"{'[DRY RUN] ' if dry_run else ''}Migrating Jira/Confluence cloud->hosting for projects {project_ids}")

        # Track migration results
        results = {
            "migrated_toolkits": [],
            "migrated_configurations": [],
            "skipped_toolkits": [],
            "failed_projects": [],
            "errors": []
        }

        for pid in project_ids:
            try:
                with db.get_session(pid) as session:
                    # Find all Jira/Confluence toolkits
                    toolkits = session.query(EliteATool).filter(
                        EliteATool.type.in_(TOOLKIT_CONFIG_MAPPING.keys())
                    ).all()

                    if toolkits:
                        log.info(f"Found {len(toolkits)} Jira/Confluence toolkits in project {pid}")

                    for toolkit in toolkits:
                        settings = toolkit.settings or {}

                        # Check if toolkit has 'cloud' field
                        if 'cloud' not in settings:
                            results["skipped_toolkits"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "toolkit_name": toolkit.name,
                                "toolkit_type": toolkit.type,
                                "reason": "No 'cloud' field in settings"
                            })
                            continue

                        cloud_value = settings.get('cloud')
                        hosting_value = CLOUD_TO_HOSTING.get(cloud_value)

                        if hosting_value is None:
                            results["skipped_toolkits"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "toolkit_name": toolkit.name,
                                "toolkit_type": toolkit.type,
                                "reason": f"Invalid 'cloud' value: {cloud_value} (expected boolean)"
                            })
                            continue

                        # Get configuration key for this toolkit type
                        config_key = TOOLKIT_CONFIG_MAPPING.get(toolkit.type)
                        if not config_key:
                            results["errors"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "error": f"Unknown toolkit type: {toolkit.type}"
                            })
                            continue

                        # Get configuration reference from toolkit settings
                        config_ref = settings.get(config_key)
                        if not config_ref:
                            results["skipped_toolkits"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "toolkit_name": toolkit.name,
                                "toolkit_type": toolkit.type,
                                "reason": f"No '{config_key}' reference in settings"
                            })
                            continue

                        elitea_title = config_ref.get('elitea_title')
                        is_private = config_ref.get('private', True)

                        if not elitea_title:
                            results["skipped_toolkits"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "toolkit_name": toolkit.name,
                                "toolkit_type": toolkit.type,
                                "reason": f"No 'elitea_title' in {config_key} reference"
                            })
                            continue

                        # Determine which project's configuration to update
                        # Private configurations are in the same project
                        # Shared configurations need to be found (for now, assume same project)
                        config_project_id = pid if is_private else pid

                        toolkit_info = {
                            "project_id": pid,
                            "toolkit_id": toolkit.id,
                            "toolkit_name": toolkit.name,
                            "toolkit_type": toolkit.type,
                            "old_cloud_value": cloud_value,
                            "new_hosting_value": hosting_value,
                            "config_key": config_key,
                            "config_elitea_title": elitea_title,
                            "config_private": is_private
                        }

                        # Find and update the configuration
                        # Only remove 'cloud' from toolkit settings if the config was
                        # successfully updated first (never remove from table 1 before
                        # the value has been written to table 2).
                        config_updated = False
                        try:
                            with db.get_session(config_project_id) as config_session:
                                configuration = config_session.query(Configuration).filter(
                                    Configuration.elitea_title == elitea_title
                                ).first()

                                if not configuration:
                                    results["skipped_toolkits"].append({
                                        **toolkit_info,
                                        "reason": f"Configuration '{elitea_title}' not found in project {config_project_id}"
                                    })
                                    continue

                                config_data = configuration.data or {}
                                old_hosting = config_data.get('hosting')

                                config_info = {
                                    "project_id": config_project_id,
                                    "config_id": configuration.id,
                                    "config_elitea_title": elitea_title,
                                    "config_type": configuration.type,
                                    "old_hosting": old_hosting,
                                    "new_hosting": hosting_value,
                                    "will_update": old_hosting != hosting_value
                                }

                                if not dry_run:
                                    # Update configuration hosting
                                    new_config_data = dict(config_data)
                                    new_config_data['hosting'] = hosting_value
                                    configuration.data = new_config_data
                                    config_session.commit()
                                    config_info["updated"] = True

                                # Mark config as successfully updated (or dry-run confirmed)
                                config_updated = True
                                results["migrated_configurations"].append(config_info)

                        except Exception as e:
                            log.error(f"Error updating configuration for toolkit {toolkit.id}: {str(e)}")
                            results["errors"].append({
                                "project_id": pid,
                                "toolkit_id": toolkit.id,
                                "config_elitea_title": elitea_title,
                                "error": str(e)
                            })
                            continue

                        if not dry_run:
                            if config_updated:
                                # Remove 'cloud' field from toolkit settings only after
                                # the configuration has been successfully persisted.
                                new_settings = dict(settings)
                                del new_settings['cloud']
                                toolkit.settings = new_settings
                                toolkit_info["cloud_removed"] = True
                            else:
                                # Should not reach here (exception path uses continue),
                                # but guard defensively.
                                toolkit_info["cloud_removed"] = False
                                log.warning(
                                    f"Skipping 'cloud' removal for toolkit {toolkit.id} "
                                    f"because configuration update was not confirmed."
                                )

                        results["migrated_toolkits"].append(toolkit_info)

                    if not dry_run:
                        session.commit()
                        log.info(f"Successfully migrated Jira/Confluence toolkits for project {pid}")

            except Exception as e:
                log.error(f"Error migrating project {pid}: {str(e)}")
                results["failed_projects"].append({
                    "project_id": pid,
                    "error": str(e)
                })

        # Build response
        response = {
            "message": f"{'[DRY RUN] ' if dry_run else ''}Jira/Confluence cloud->hosting migration completed",
            "dry_run": dry_run,
            "results": results,
            "summary": {
                "total_projects": len(project_ids),
                "failed_projects": len(results["failed_projects"]),
                "toolkits_migrated": len(results["migrated_toolkits"]),
                "toolkits_skipped": len(results["skipped_toolkits"]),
                "configurations_updated": len([c for c in results["migrated_configurations"] if c.get("will_update", True)]),
                "errors": len(results["errors"])
            }
        }

        status_code = 200 if len(results["failed_projects"]) == 0 and len(results["errors"]) == 0 else 207

        return response, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
