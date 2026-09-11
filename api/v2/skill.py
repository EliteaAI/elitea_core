from flask import request
from pydantic import ValidationError
from pylon.core.tools import log

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.skill import (
    SkillMcpUpdateModel,
    SkillUpdateModel,
    SkillUpdateRelationModel,
)
from ...models.pd.skill_version import (
    SkillVersionCreateModel,
    SkillVersionUpdateModel,
)
from ...utils.skill_utils import (
    get_skill_details,
    update_skill,
    delete_skill,
    create_skill_version,
    update_skill_version,
    delete_skill_version,
    get_skill_version_by_id,
    attach_skill_to_agent,
    detach_skill_from_agent,
    SkillError,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.folder_access import require_folder_access
from ...utils.mcp_versioning import INTERNAL_MCP_ENVIRON_KEY


SKILL_PATH = '<string:mode>/<int:project_id>/<int:skill_id>'

# #6410: keys that name *where* the request goes rather than *what* it writes. The MCP executor
# routes every tool argument that is not a declared path/query parameter into the request body,
# and a direct HTTP client may echo its own URL params back, so they arrive in bodies that
# SkillVersionUpdateModel (extra="forbid") would otherwise reject.
TRANSPORT_OWNED_BODY_FIELDS = frozenset({'project_id', 'user_id', 'skill_id', 'version_id'})

# Fields of the nested skill shape that have no meaning once a single version is addressed.
SKILL_ONLY_BODY_FIELDS = frozenset({'description'})

MALFORMED_BODY_ERROR = 'request body must be present, sent as application/json, and a JSON object'


def _same_id(a, b) -> bool:
    """Compare an id supplied in a body with one taken from the URL ('8' == 8 == 8.0).

    JSON gives no integer type hint, so a model may spell the same id as a string, an int or a
    float. Only a genuinely different id should be treated as a mismatch.
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return str(a) == str(b)


def _render_keys(keys) -> str:
    return ', '.join(repr(key) for key in keys)


def normalize_version_update_body(raw, *, project_id: int, skill_id: int, version_id: int):
    """Reduce a version-targeted PUT body to the flat shape ``SkillVersionUpdateModel`` accepts.

    Returns ``(body, error, trace)``. Transport keys are dropped after being cross-checked
    against the URL - a *disagreeing* value is an explicit error rather than a silent write to
    the addressed version. A ``{"version": {...}}`` envelope is unwrapped onto the flat shape,
    which is the only body a schema-conforming MCP client can build for a content edit. Keys
    whose value is ``None`` were materialized from a published schema default by the SDK, not
    authored by the caller, so they never count as content.
    """
    if not isinstance(raw, dict):
        return None, MALFORMED_BODY_ERROR, {}

    body = dict(raw)
    trace = {'dropped': [], 'unwrapped': False, 'stray_id': None}

    url_owned = {'project_id': project_id, 'skill_id': skill_id, 'version_id': version_id}
    for key, url_value in url_owned.items():
        if key not in body:
            continue
        supplied = body.pop(key)
        trace['dropped'].append(key)
        if supplied is not None and not _same_id(supplied, url_value):
            return None, (
                f'body {key} {supplied!r} does not match {key} {url_value!r} in the URL'
            ), trace

    # The remaining transport keys are never compared: the server resolves the author from the
    # session, so whatever a caller echoes is ignored rather than trusted.
    for key in sorted(TRANSPORT_OWNED_BODY_FIELDS - url_owned.keys()):
        if key in body:
            body.pop(key)
            trace['dropped'].append(key)

    for key in SKILL_ONLY_BODY_FIELDS:
        if body.get(key) is None:
            body.pop(key, None)
    if skill_only := sorted(SKILL_ONLY_BODY_FIELDS & set(body)):
        return None, (
            f'{_render_keys(skill_only)} updates the skill, not a version; '
            'omit the version selector to edit skill metadata'
        ), trace

    if (stray_id := body.get('id')) is not None and not _same_id(stray_id, version_id):
        trace['stray_id'] = stray_id

    if 'version' in body:
        nested = body.pop('version')
        if nested is not None:
            if not isinstance(nested, dict):
                return None, (
                    'version must be a JSON object holding the version fields to write'
                ), trace
            if leftovers := sorted(key for key, value in body.items() if value is not None):
                return None, (
                    f'cannot combine top-level field(s) {_render_keys(leftovers)} with a '
                    f'version envelope - both would write the same version. Put them inside '
                    f'version: {{...}}, or omit them'
                ), trace
            nested_id = nested.get('id')
            if nested_id is not None and not _same_id(nested_id, version_id):
                return None, (
                    f'version.id {nested_id!r} does not match version_id {version_id!r} in the URL'
                ), trace
            trace['unwrapped'] = True
            return nested, None, trace

    return body, None, trace


def resolve_version_id(
    path_version_id: int | None,
    name: str = 'version_id',
    *,
    treat_null_as_absent: bool = False,
):
    """Return ``(version_id, error_response)`` for the version addressed by path or query.

    ``treat_null_as_absent`` reads a literal ``'None'``/``'null'`` selector as "no version
    selected" instead of rejecting it. It is OPT-IN per call site and must stay that way: for
    ``delete`` an absent selector means *delete the whole skill*, so tolerating a malformed
    selector there would turn a 400 into an irreversible deletion. Only ``put`` opts in, where
    the fallback is the skill-metadata branch.
    """
    if path_version_id is not None:
        return path_version_id, None

    raw = request.args.get(name)
    if not raw:
        return None, None

    # #6410: a caller (or a client holding a cached tool schema) that spells the absent selector
    # out as 'None'/'null' means "no version", not "a version named null".
    if treat_null_as_absent and raw.lower() in ('none', 'null'):
        return None, None

    try:
        return int(raw), None
    except ValueError:
        return None, ({"error": f"{name} must be an integer"}, 400)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Retrieve full metadata and version configuration of a specific skill",
        description=(
            "Returns the full details of the specified skill. If a version_id is provided, "
            "that version's details are included (404 if the version id does not exist); otherwise the "
            "default version is included. Response includes top-level default_version_id and version_id."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id to load details for"},
        ],
        path_suffix_override=SKILL_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id')
    def get(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        if ignored := sorted(set(request.args) - {'version_id'}):
            log.warning("Ignoring unsupported query parameter(s): %s", ignored)

        version_id, error = resolve_version_id(version_id)
        if error:
            return error

        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

        result = get_skill_details(
            project_id=project_id,
            skill_id=skill_id,
            version_id=version_id,
        )

        if not result.get('data'):
            return {"error": "Skill not found"}, 404

        return result['data'], 200

    @register_openapi(
        name="Create a new version for an existing skill",
        description="Creates a new (non-base) version for the skill. Version name must be unique within the skill and must not be 'base'.",
        request_body=SkillVersionCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        path_suffix_override=SKILL_PATH,
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def post(self, project_id: int, skill_id: int, **kwargs):
        raw = dict(request.json)
        raw['author_id'] = auth.current_user().get("id")

        try:
            version_data = SkillVersionCreateModel.model_validate(raw)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if version_data.name == 'base':
            return {"error": "Version name 'base' is reserved; use a different name"}, 400

        try:
            detail = create_skill_version(
                project_id=project_id,
                skill_id=skill_id,
                version_data=version_data,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return detail, 201

    @register_openapi(
        name="Update a skill's metadata or a specific skill version",
        description=(
            "Without a version selector the body edits the skill metadata (name, description, meta) "
            "and optionally version content in the same transaction — the nested version.id selects "
            "the target version (default version when omitted). "
            "With a version selector (the version_id query parameter or the /{version_id} path form) "
            "the request updates ONLY that version: send {\"version\": {...}} with every field "
            "you are writing inside it, or the flat version shape (name, instructions, tags, meta). "
            "A top-level name/meta is an ALTERNATIVE spelling that applies to that VERSION rather "
            "than to the skill — do not send both — and description is not accepted. "
            "Identity fields (project_id, user_id) are resolved by the server. "
            "Published or embedded versions cannot be updated."
        ),
        request_body=SkillUpdateModel,
        mcp_request_body=SkillMcpUpdateModel,
        mcp_description="""
        USE to change a skill's metadata, or to write content into one of its versions.

        DO NOT USE when:
        - Attaching or detaching a skill to an agent → use the skill relation tool
        - Creating a new version → use create_version

        REQUIRED path params: project_id, skill_id.

        Identity and author (project_id in the body, user_id) are resolved by the server — never
        send them. Only the fields you send are written; unset fields are NOT overwritten.

        WITHOUT version_id: the body edits skill metadata (name, description, meta) and may carry
        `version: {...}` to write version content in the same transaction; `version.id` selects
        which version (the default version when omitted).

        WITH version_id: ONLY that version is updated. Send { "version": { ... } } — that is the
        recommended shape; put every field you are writing inside it. A top-level `name`/`meta`
        is an ALTERNATIVE spelling that writes THAT VERSION (not the skill) — do not send both,
        and `description` is rejected. To rename the skill AND edit a version in one atomic call,
        omit version_id and send the skill fields alongside `version: {id: N, ...}`.

        Version content example:
        { 'project_id': 2, 'skill_id': 147, 'version_id': 204,
          'version': { 'instructions': 'new text', 'tags': [{'name': 'aqa'}] } }
        """,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id: the request then updates ONLY that version. Send {\"version\": {...}} (recommended) and put every field you are writing inside it. A top-level name/meta is an ALTERNATIVE spelling that writes that VERSION, not the skill - do not send both; description is not accepted. Omit this to edit skill metadata."},
        ],
        path_suffix_override=SKILL_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def put(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        if ignored := sorted(set(request.args) - {'version_id'}):
            log.warning("Ignoring unsupported query parameter(s): %s", ignored)

        version_id, error = resolve_version_id(version_id, treat_null_as_absent=True)
        if error:
            return error

        # Update a specific version addressed by id.
        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

            body, shape_error, trace = normalize_version_update_body(
                request.get_json(silent=True),
                project_id=project_id,
                skill_id=skill_id,
                version_id=version.id,
            )
            log.info(
                "skill PUT %s/%s: branch=version version_id=%s unwrapped=%s dropped=%s "
                "stray_id=%r mcp=%s",
                project_id, skill_id, version.id, trace.get('unwrapped', False),
                sorted(trace.get('dropped', ())), trace.get('stray_id'),
                request.environ.get(INTERNAL_MCP_ENVIRON_KEY, False),
            )
            if trace.get('stray_id') is not None:
                log.warning(
                    "skill PUT %s/%s: body id %r does not address version %s; the URL wins",
                    project_id, skill_id, trace['stray_id'], version.id,
                )
            if shape_error:
                return {"error": shape_error}, 400

            try:
                update_data = SkillVersionUpdateModel.model_validate(body)
            except ValidationError as e:
                return e.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                ), 400

            try:
                detail = update_skill_version(
                    project_id=project_id,
                    skill_id=skill_id,
                    version_id=version.id,
                    update_data=update_data,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status

            return detail, 200

        # Update skill metadata (and optionally the default version).
        raw = request.get_json(silent=True)
        if not isinstance(raw, dict):
            return {"error": MALFORMED_BODY_ERROR}, 400

        log.info(
            "skill PUT %s/%s: branch=metadata mcp=%s",
            project_id, skill_id, request.environ.get(INTERNAL_MCP_ENVIRON_KEY, False),
        )

        payload = dict(raw)
        # #6410: version_id is meaningless once we're in the nested branch (there was
        # none in the path); drop it so a caller that includes it alongside a nested
        # "version" body doesn't trip the extra="forbid" guard below.
        payload.pop('version_id', None)
        # #6410: the version branch cross-checks and drops the transport keys; do the same here
        # so a direct HTTP client echoing its own URL params is not rejected on one branch and
        # accepted on the other. A *disagreeing* value stays an explicit error.
        for key, url_value in (('project_id', project_id), ('skill_id', skill_id)):
            body_value = payload.pop(key, None)
            if body_value is not None and not _same_id(body_value, url_value):
                return {"error": (
                    f'{key} {body_value!r} in the body does not match {url_value!r} in the URL'
                )}, 400
        payload['project_id'] = project_id
        payload['user_id'] = auth.current_user().get("id")

        try:
            update_data = SkillUpdateModel.model_validate(payload)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        try:
            detail = update_skill(
                project_id=project_id,
                skill_id=skill_id,
                update_data=update_data,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return detail, 200

    @register_openapi(
        name="Link or unlink a skill to an agent version",
        description="Toggles the relation between a skill and an agent (application) version, mirroring the Link Agent to Toolkit flow. When has_relation is True the given skill version is attached (skill_version_id required); when False the skill is detached.",
        request_body=SkillUpdateRelationModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        path_suffix_override=SKILL_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def patch(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        if ignored := sorted(request.args):
            log.warning("Ignoring unsupported query parameter(s): %s", ignored)

        if version_id is not None:
            return {"error": "version_id is not supported for PATCH; the attached version is skill_version_id in the body"}, 400

        try:
            relation_data = SkillUpdateRelationModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if relation_data.has_relation:
            try:
                data = attach_skill_to_agent(
                    project_id=project_id,
                    entity_version_id=relation_data.entity_version_id,
                    skill_id=skill_id,
                    skill_version_id=relation_data.skill_version_id,
                    entity_type=relation_data.entity_type,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status
            return data, 201

        try:
            detach_skill_from_agent(
                project_id=project_id,
                entity_version_id=relation_data.entity_version_id,
                skill_id=skill_id,
                entity_type=relation_data.entity_type,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return {'ok': True}, 200

    @register_openapi(
        name="Delete a skill or a specific skill version",
        description="Without a version_id, permanently deletes the skill and all of its versions (agent attachments cascade-removed). With a version_id, deletes that specific version (cannot delete the only version or a version still attached to agents). Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id to delete a specific version"},
        ],
        path_suffix_override=SKILL_PATH,
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def delete(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        if unsupported := sorted(set(request.args) - {'version_id'}):
            return {"error": f"unsupported query parameter(s): {unsupported}"}, 400

        version_id, error = resolve_version_id(version_id)
        if error:
            return error

        # Delete a specific version addressed by id.
        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

            try:
                delete_skill_version(
                    project_id=project_id,
                    skill_id=skill_id,
                    version_id=version.id,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status

            return '', 204

        # Delete the entire skill.
        try:
            delete_skill(
                project_id=project_id,
                skill_id=skill_id,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
