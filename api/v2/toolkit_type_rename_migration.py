from pylon.core.tools import log
from flask import request
from sqlalchemy.orm.attributes import flag_modified
from tools import api_tools, auth, config as c, db

from ...models.elitea_tools import EliteATool


# Mapping of old toolkit types to new toolkit types.
#
# Required fields per entry:
#   new_type, old_toolkit, new_toolkit
#
# Optional (only when the provider slug itself changed, e.g. deepwiki ->
# wikis). Both must be supplied together. When present, the migration
# also rewrites settings.provider and the provider slug in
# meta.interface.{app_url,create_url}:
#   old_provider, new_provider
TOOLKIT_TYPE_RENAMES = {
    "SyngenServiceProvider_SyngenToolkit": {
        "new_type": "SyngenServiceProvider_Syngen",
        "old_toolkit": "SyngenToolkit",
        "new_toolkit": "Syngen",
    },
    "ClaudeServiceProvider_ClaudeToolkit": {
        "new_type": "ClaudeServiceProvider_ClaudeCode",
        "old_toolkit": "ClaudeToolkit",
        "new_toolkit": "ClaudeCode",
    },
    "SlidevServiceProvider_SlidevToolkit": {
        "new_type": "SlidevServiceProvider_Slidev",
        "old_toolkit": "SlidevToolkit",
        "new_toolkit": "Slidev",
    },
    "CodexServiceProvider_CodexToolkit": {
        "new_type": "CodexServiceProvider_Codex",
        "old_toolkit": "CodexToolkit",
        "new_toolkit": "Codex",
    },
    "deepwiki_Deepwiki": {
        "new_type": "wikis_Wikis",
        "old_toolkit": "Deepwiki",
        "new_toolkit": "Wikis",
        "old_provider": "deepwiki",
        "new_provider": "wikis",
    },
}


REQUIRED_RENAME_FIELDS = ("new_type", "old_toolkit", "new_toolkit")
OPTIONAL_PROVIDER_FIELDS = ("old_provider", "new_provider")


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
        Migrate toolkit types when toolkits (and optionally their providers)
        are renamed.

        Updates per tool: type, settings.toolkit, settings.provider,
        meta.label, meta.interface.app_url, meta.interface.create_url.

        Re-running is safe: each rename matches both the old and new type,
        and only stale fields are touched, so partially-migrated rows from
        prior runs get repaired without disturbing rows that already match
        the new schema.

        Request body:
        {
            "project_id": 0,                  # 0 = all projects
            "dry_run": false,
            "renames": {                      # optional - overrides defaults
                "deepwiki_Deepwiki": {
                    "new_type": "wikis_Wikis",
                    "old_toolkit": "Deepwiki",
                    "new_toolkit": "Wikis",
                    "old_provider": "deepwiki",
                    "new_provider": "wikis"
                }
            }
        }
        """
        payload = dict(request.json) if request.json else {}

        project_id = payload.get('project_id')
        if project_id is not None:
            try:
                project_id = int(project_id)
                if project_id < 0:
                    return {"error": "Invalid project_id. Must be >= 0."}, 400
            except ValueError:
                return {"error": "Invalid project_id. Must be an integer."}, 400

        dry_run = payload.get('dry_run', False)

        custom_renames = payload.get('renames', {})
        if custom_renames:
            renames = {}
            for old_type, rename_config in custom_renames.items():
                if not old_type or not isinstance(old_type, str):
                    return {"error": "Invalid old_type key: must be a non-empty string"}, 400
                if not isinstance(rename_config, dict):
                    return {"error": f"Invalid rename config for '{old_type}': must be an object"}, 400

                missing = [f for f in REQUIRED_RENAME_FIELDS if f not in rename_config]
                if missing:
                    return {"error": f"Missing required fields for '{old_type}': {missing}"}, 400

                normalized = {}
                for field in REQUIRED_RENAME_FIELDS:
                    value = rename_config[field]
                    if not value or not isinstance(value, str):
                        return {"error": f"Invalid '{field}' for '{old_type}': must be a non-empty string"}, 400
                    normalized[field] = value

                supplied_provider_fields = [f for f in OPTIONAL_PROVIDER_FIELDS if f in rename_config]
                if supplied_provider_fields and len(supplied_provider_fields) != len(OPTIONAL_PROVIDER_FIELDS):
                    return {"error": f"For '{old_type}': both 'old_provider' and 'new_provider' must be supplied together"}, 400
                for field in supplied_provider_fields:
                    value = rename_config[field]
                    if not value or not isinstance(value, str):
                        return {"error": f"Invalid '{field}' for '{old_type}': must be a non-empty string"}, 400
                    normalized[field] = value

                renames[old_type] = normalized
        else:
            renames = TOOLKIT_TYPE_RENAMES

        if not renames:
            return {"error": "No renames configured. Provide 'renames' in request body or configure TOOLKIT_TYPE_RENAMES."}, 400

        project_ids = [project_id] if project_id else [
            i['id'] for i in self.module.context.rpc_manager.call.project_list(
                filter_={'create_success': True}
            )
        ]

        log.info(f"{'[DRY RUN] ' if dry_run else ''}Migrating toolkit types for projects {project_ids}")
        log.info(f"Renames to apply: {renames}")

        results = {
            "migrated_tools": [],
            "failed_projects": [],
            "summary_by_type": {},
        }
        for old_type, cfg in renames.items():
            results["summary_by_type"][old_type] = {
                "new_type": cfg["new_type"],
                "tools_found": 0,
                "tools_migrated": 0,
            }

        for pid in project_ids:
            try:
                with db.get_session(pid) as session:
                    for old_type, cfg in renames.items():
                        new_type = cfg["new_type"]
                        old_toolkit = cfg["old_toolkit"]
                        new_toolkit = cfg["new_toolkit"]
                        old_provider = cfg.get("old_provider")
                        new_provider = cfg.get("new_provider")

                        # Match both old and new type so partial migrations
                        # from prior runs get repaired idempotently.
                        tools = session.query(EliteATool).filter(
                            EliteATool.type.in_([old_type, new_type])
                        ).all()
                        results["summary_by_type"][old_type]["tools_found"] += len(tools)
                        if tools:
                            log.info(
                                f"Found {len(tools)} tool(s) matching rename "
                                f"'{old_type}' -> '{new_type}' in project {pid}"
                            )

                        for tool in tools:
                            settings = tool.settings or {}
                            meta = tool.meta or {}
                            interface = meta.get("interface") if isinstance(meta.get("interface"), dict) else None

                            type_changed = tool.type == old_type
                            toolkit_changed = settings.get("toolkit") == old_toolkit
                            provider_changed = bool(
                                old_provider and new_provider
                                and settings.get("provider") == old_provider
                            )
                            label_changed = meta.get("label") == old_toolkit

                            url_changes = {}
                            if (
                                old_provider and new_provider
                                and old_provider != new_provider
                                and interface is not None
                            ):
                                old_slug = f"/{old_provider}/"
                                new_slug = f"/{new_provider}/"
                                for url_key in ("app_url", "create_url"):
                                    url = interface.get(url_key)
                                    if isinstance(url, str) and old_slug in url:
                                        url_changes[url_key] = url.replace(old_slug, new_slug)

                            changes = []
                            if type_changed:
                                changes.append({"field": "type", "old": tool.type, "new": new_type})
                            if toolkit_changed:
                                changes.append({"field": "settings.toolkit", "old": settings["toolkit"], "new": new_toolkit})
                            if provider_changed:
                                changes.append({"field": "settings.provider", "old": settings["provider"], "new": new_provider})
                            if label_changed:
                                changes.append({"field": "meta.label", "old": meta["label"], "new": new_toolkit})
                            for url_key, new_url in url_changes.items():
                                changes.append({
                                    "field": f"meta.interface.{url_key}",
                                    "old": interface[url_key],
                                    "new": new_url,
                                })

                            tool_info = {
                                "project_id": pid,
                                "tool_id": tool.id,
                                "tool_name": tool.name,
                                "current_type": tool.type,
                                "rename": f"{old_type} -> {new_type}",
                                "changes": changes,
                                "will_update": bool(changes),
                            }

                            if changes and not dry_run:
                                if type_changed:
                                    tool.type = new_type
                                if toolkit_changed or provider_changed:
                                    new_settings = dict(settings)
                                    if toolkit_changed:
                                        new_settings["toolkit"] = new_toolkit
                                    if provider_changed:
                                        new_settings["provider"] = new_provider
                                    tool.settings = new_settings
                                    flag_modified(tool, "settings")
                                if label_changed or url_changes:
                                    new_meta = dict(meta)
                                    if label_changed:
                                        new_meta["label"] = new_toolkit
                                    if url_changes:
                                        new_meta["interface"] = {**interface, **url_changes}
                                    tool.meta = new_meta
                                    flag_modified(tool, "meta")
                                results["summary_by_type"][old_type]["tools_migrated"] += 1
                                tool_info["updated"] = True

                            results["migrated_tools"].append(tool_info)

                    if not dry_run:
                        session.commit()
                        log.info(f"Successfully migrated toolkit types for project {pid}")
            except Exception as e:
                log.error(f"Error migrating project {pid}: {str(e)}")
                results["failed_projects"].append({
                    "project_id": pid,
                    "error": str(e),
                })

        response = {
            "message": f"{'[DRY RUN] ' if dry_run else ''}Toolkit type rename migration completed",
            "dry_run": dry_run,
            "renames_applied": {k: v["new_type"] for k, v in renames.items()},
            "results": results,
            "summary": {
                "total_projects": len(project_ids),
                "failed_count": len(results["failed_projects"]),
                "total_tools_found": sum(r["tools_found"] for r in results["summary_by_type"].values()),
                "total_tools_migrated": sum(r["tools_migrated"] for r in results["summary_by_type"].values()),
            },
        }

        status_code = 200 if not results["failed_projects"] else 207
        return response, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
