from pylon.core.tools import log
from flask import request
from sqlalchemy import text
from tools import api_tools, auth, config as c, db

from ...models.elitea_tools import EliteATool


# Mapping of old toolkit types to new toolkit types
# Format: "OldProviderName_OldToolkitName": ("NewProviderName_NewToolkitName", "OldToolkitName", "NewToolkitName")
# The tuple contains: (new_type, old_settings_toolkit, new_settings_toolkit)
TOOLKIT_TYPE_RENAMES = {
    "SyngenServiceProvider_SyngenToolkit": ("SyngenServiceProvider_Syngen", "SyngenToolkit", "Syngen"),
    "ClaudeServiceProvider_ClaudeToolkit": ("ClaudeServiceProvider_ClaudeCode", "ClaudeToolkit", "ClaudeCode"),
    "SlidevServiceProvider_SlidevToolkit": ("SlidevServiceProvider_Slidev", "SlidevToolkit", "Slidev"),
    "CodexServiceProvider_CodexToolkit": ("CodexServiceProvider_Codex", "CodexToolkit", "Codex"),
    # Add more renames here as needed:
    # "ProviderName_OldToolkitName": ("ProviderName_NewToolkitName", "OldToolkitName", "NewToolkitName"),
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
        Migrate toolkit types when toolkits are renamed.
        
        Request body:
        {
            "project_id": 0,  // 0 for all projects, or specific project_id
            "dry_run": false,  // If true, only report what would be changed
            "renames": {  // Optional: override default TOOLKIT_TYPE_RENAMES
                "SyngenServiceProvider_SyngenToolkit": {
                    "new_type": "SyngenServiceProvider_Syngen",
                    "old_toolkit": "SyngenToolkit", 
                    "new_toolkit": "Syngen"
                }
            }
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
        
        # Get renames from payload or use defaults
        custom_renames = payload.get('renames', {})
        renames = {}
        
        # Use custom renames if provided, otherwise use defaults
        if custom_renames:
            required_fields = ('new_type', 'old_toolkit', 'new_toolkit')
            for old_type, rename_config in custom_renames.items():
                # Validate old_type is non-empty string
                if not old_type or not isinstance(old_type, str):
                    return {"error": f"Invalid old_type key: must be a non-empty string"}, 400
                
                # Validate rename_config is a dict
                if not isinstance(rename_config, dict):
                    return {"error": f"Invalid rename config for '{old_type}': must be an object"}, 400
                
                # Validate required fields exist
                missing_fields = [f for f in required_fields if f not in rename_config]
                if missing_fields:
                    return {"error": f"Missing required fields for '{old_type}': {missing_fields}"}, 400
                
                # Validate all values are non-empty strings
                for field in required_fields:
                    value = rename_config[field]
                    if not value or not isinstance(value, str):
                        return {"error": f"Invalid '{field}' for '{old_type}': must be a non-empty string"}, 400
                
                renames[old_type] = (
                    rename_config['new_type'],
                    rename_config['old_toolkit'],
                    rename_config['new_toolkit']
                )
        else:
            renames = TOOLKIT_TYPE_RENAMES
        
        if not renames:
            return {"error": "No renames configured. Provide 'renames' in request body or configure TOOLKIT_TYPE_RENAMES."}, 400
        
        # Get project IDs
        project_ids = [project_id] if project_id else [
            i['id'] for i in self.module.context.rpc_manager.call.project_list(
                filter_={'create_success': True}
            )
        ]
        
        log.info(f"{'[DRY RUN] ' if dry_run else ''}Migrating toolkit types for projects {project_ids}")
        log.info(f"Renames to apply: {renames}")
        
        # Track migration results
        results = {
            "migrated_tools": [],
            "failed_projects": [],
            "summary_by_type": {}
        }
        
        for old_type, (new_type, old_toolkit, new_toolkit) in renames.items():
            results["summary_by_type"][old_type] = {
                "new_type": new_type,
                "tools_found": 0,
                "tools_migrated": 0
            }
        
        for pid in project_ids:
            try:
                with db.get_session(pid) as session:
                    for old_type, (new_type, old_toolkit, new_toolkit) in renames.items():
                        # Find tools with the old type
                        tools = session.query(EliteATool).filter(
                            EliteATool.type == old_type
                        ).all()
                        
                        results["summary_by_type"][old_type]["tools_found"] += len(tools)
                        
                        if tools:
                            log.info(f"Found {len(tools)} tools with type '{old_type}' in project {pid}")
                        
                        for tool in tools:
                            # Check if settings['toolkit'] needs update
                            settings_needs_update = (
                                tool.settings and 
                                'toolkit' in tool.settings and 
                                tool.settings['toolkit'] == old_toolkit
                            )
                            current_settings_toolkit = tool.settings.get('toolkit') if tool.settings else None
                            
                            tool_info = {
                                "project_id": pid,
                                "tool_id": tool.id,
                                "tool_name": tool.name,
                                "old_type": old_type,
                                "new_type": new_type,
                                "old_settings_toolkit": current_settings_toolkit,
                                "new_settings_toolkit": new_toolkit if settings_needs_update else current_settings_toolkit,
                                "settings_toolkit_will_update": settings_needs_update,
                            }
                            
                            if not dry_run:
                                # Update the type
                                tool.type = new_type
                                
                                # Update settings['toolkit'] if it matches old toolkit name
                                if settings_needs_update:
                                    # Create new settings dict to trigger SQLAlchemy update
                                    new_settings = dict(tool.settings)
                                    new_settings['toolkit'] = new_toolkit
                                    tool.settings = new_settings
                                    tool_info["settings_toolkit_updated"] = True
                                
                                results["summary_by_type"][old_type]["tools_migrated"] += 1
                            
                            results["migrated_tools"].append(tool_info)
                    
                    if not dry_run:
                        session.commit()
                        log.info(f"Successfully migrated toolkit types for project {pid}")
                        
            except Exception as e:
                log.error(f"Error migrating project {pid}: {str(e)}")
                results["failed_projects"].append({
                    "project_id": pid,
                    "error": str(e)
                })
        
        # Build response
        response = {
            "message": f"{'[DRY RUN] ' if dry_run else ''}Toolkit type rename migration completed",
            "dry_run": dry_run,
            "renames_applied": {k: v[0] for k, v in renames.items()},
            "results": results,
            "summary": {
                "total_projects": len(project_ids),
                "failed_count": len(results["failed_projects"]),
                "total_tools_found": sum(r["tools_found"] for r in results["summary_by_type"].values()),
                "total_tools_migrated": sum(r["tools_migrated"] for r in results["summary_by_type"].values()),
            }
        }
        
        status_code = 200 if len(results["failed_projects"]) == 0 else 207
        
        return response, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
