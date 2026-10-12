"""Tool Spend features (#6980): the feature list derived from toolkit-schema metadata."""

from tools import api_tools, auth, config as c

from ...utils.tool_spend_features import feature_map


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.admin.project_budgets.view"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        features = feature_map(self.module.toolkit_schemas)
        if not features:
            return {"error": "Tool spend features are not loaded yet, retry shortly"}, 503
        return [
            {"feature": name, "label": info["label"], "backends": sorted(info["backends"])}
            for name, info in sorted(features.items())
        ], 200


class API(api_tools.APIBase):
    url_params = ["<string:mode>"]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
