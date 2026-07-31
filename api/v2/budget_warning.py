from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE

OPENAPI_TAG = "elitea_core/usage"

# Nothing to warn about: also what a caller gets when the feature is not installed
NO_WARNING = {
    "scope": None,
    "percent_used": None,
    "warning_pct": None,
    "should_warn": False,
}


def _warning_state(project_id: int, user_id: int):
    """Budget warning state for one member of one project.

    Deliberately narrow. The run pages ask on every open, so this returns a percentage and
    nothing else -- the Usage endpoint's per-day and per-model breakdown would page a whole
    month of spend to produce the same number.
    """
    try:
        return rpc_tools.RpcMixin().rpc.timeout(10).litellm_get_budget_warning_state(
            project_id=project_id, user_id=user_id,
        ) or dict(NO_WARNING)
    except Exception:  # pylint: disable=W0703
        # No cost-budgets plugin, or it is unreachable: show nothing rather than block the page
        return dict(NO_WARNING)


class PromptLibAPI(api_tools.APIModeHandler):
    """A project member checks whether their budget is nearing its limit."""

    @register_openapi(
        name="Get Budget Warning State",
        description=(
            "Whether to warn the caller that a budget is nearing its limit, for the banner "
            "above the message input on the chat, agent, pipeline and skill pages.\n\n"
            "Returns a single scope: the member budget takes priority over the project "
            "budget, because it is the one that stops this user specifically. scope is null "
            "and should_warn is false when nothing applies.\n\n"
            "should_warn is false for an unlimited budget, below the configured threshold, "
            "and at or above 100% -- at the limit the request is blocked and that error "
            "carries the message instead. It is also false unless budgets are enforcing, "
            "since observe mode tracks spend without ever blocking.\n\n"
            "The result is cached briefly, so the percentage may lag real spend by up to a "
            "minute. That is deliberate: resolving it reads a month of activity, and this "
            "endpoint is on the interactive path."
        ),
        tags=[OPENAPI_TAG],
        parameters=[
            {
                "name": "project_id",
                "in": "path",
                "required": True,
                "schema": {"type": "integer"},
                "description": "Project the caller is working in.",
                "example": 1,
            },
        ],
        responses={
            "200": {"description": "Warning state for the caller in this project."},
        },
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        # Same permission the Usage page uses, so a caller who can be warned can also
        # follow the link the banner offers
        user_id = auth.current_user().get("id")
        #
        return _warning_state(project_id, user_id), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/budget_warning",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
