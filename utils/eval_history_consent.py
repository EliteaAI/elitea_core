"""The project setting that lets the scheduled evaluation score agents from the project's chats.

One ``eval_history_settings`` record per project in the configurations plugin, section
``project_settings``, owned by elitea_core so only the evaluation settings PUT (a project admin)
changes it. Off until a project admin turns it on: history mode reads private chats, so without the
record an agent is never scored from them.

Each save also sets the project's row in the shared ``eval_history_consent_index``, which the admin
listing reads in one query; :func:`reindex_history_consents` rebuilds it from the records.
"""
from pydantic import BaseModel, ConfigDict, Field, StrictBool
from pylon.core.tools import log

SETTINGS_TYPE = "eval_history_settings"
RPC_TIMEOUT = 5
OWNER = "elitea_core"
MANAGE_URL = "/api/v2/elitea_core/eval_history_consent/{mode}/{project_id}"


class EvalHistoryProjectSettings(BaseModel):
    """The history evaluation consent of one project; the configurations plugin validates each write with this model."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"metadata": {
        "label": "Evaluation from chat history", "section": "project_settings", "type": SETTINGS_TYPE,
    }})

    history_evaluation_consent: StrictBool = Field(
        description="True lets the scheduled evaluation score the project's agents from their stored "
                    "chats, private ones included. Case text is shown only to the chat author and "
                    "project admins.",
        examples=[True],
    )


def _rpc():
    from tools import rpc_tools  # pylint: disable=E0401,C0415 -- late: the model loads without the platform
    return rpc_tools.RpcMixin().rpc.timeout(RPC_TIMEOUT)


def register_eval_history_settings_type() -> None:
    """Register the record type as owned by elitea_core; a failure is logged, the plugin still starts.

    Unregistered, the generic configurations REST API would let anyone with configuration write
    access give the consent, so a failure leaves the type unregistered rather than unowned.
    """
    try:
        _rpc().configurations_register(type_name=SETTINGS_TYPE, section="project_settings",
                                       model=EvalHistoryProjectSettings, replace=True,
                                       managed_by=OWNER, manage_url=MANAGE_URL)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the plugin still starts
        log.exception("Could not register the %s configuration type; it stays unregistered", SETTINGS_TYPE)


def _read(project_id: int):
    config = _rpc().configurations_get_first_filtered_project(
        project_id=project_id, filter_fields={"type": SETTINGS_TYPE, "elitea_title": SETTINGS_TYPE},
    )
    return None if config is None else EvalHistoryProjectSettings.model_validate(config["data"])


def has_history_consent(project_id: int) -> bool:
    """True when a project admin turned the consent on; a read error or a broken record is no (fail closed)."""
    try:
        settings = _read(project_id)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- no consent when the store fails
        log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
        return False
    return settings is not None and settings.history_evaluation_consent


def read_history_consent(project_id: int) -> bool:
    """The saved value for the settings form, False when never saved; a store error propagates."""
    settings = _read(project_id)
    return settings is not None and settings.history_evaluation_consent


def save_history_consent(project_id: int, consent: bool) -> dict:
    """Create or replace the project's record; a store error propagates."""
    data = EvalHistoryProjectSettings(history_evaluation_consent=consent).model_dump()
    rpc = _rpc()
    config, created = rpc.configurations_create_if_not_exists(payload={
        "project_id": project_id, "elitea_title": SETTINGS_TYPE, "label": "Evaluation from chat history",
        "type": SETTINGS_TYPE, "data": data,
    }, owner=OWNER)
    if not created:
        rpc.configurations_update(project_id=project_id, config_id=config["id"], payload={"data": data}, owner=OWNER)
    # After the record: a failed index write fails the save, and the admin can retry it.
    index_history_consent(project_id, consent)
    return data


def index_history_consent(project_id: int, consent: bool) -> None:
    from sqlalchemy import func  # pylint: disable=C0415
    from sqlalchemy.dialects.postgresql import insert  # pylint: disable=C0415
    from tools import db  # pylint: disable=E0401,C0415
    from ..models.eval_auto import EvalHistoryConsentIndex  # pylint: disable=C0415

    with db.with_project_schema_session(None) as session:
        session.execute(insert(EvalHistoryConsentIndex).values(project_id=project_id, consent=consent)
                        .on_conflict_do_update(index_elements=[EvalHistoryConsentIndex.project_id],
                                               set_={"consent": consent, "updated_at": func.now()}))
        session.commit()


def consenting_project_ids() -> set:
    """The projects whose index row says the consent is on."""
    from tools import db  # pylint: disable=E0401,C0415
    from ..models.eval_auto import EvalHistoryConsentIndex  # pylint: disable=C0415

    with db.with_project_schema_session(None) as session:
        return {row.project_id for row in session.query(EvalHistoryConsentIndex.project_id).filter(
            EvalHistoryConsentIndex.consent.is_(True))}


def reindex_history_consents(project_ids) -> dict:
    """Rebuild the index from each project's record (backfill, or repair after a failed save).

    A project whose record cannot be read keeps its row as it is.
    """
    indexed, failed = 0, []
    for project_id in project_ids:
        try:
            settings = _read(project_id)
        except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- one project must not stop the others
            log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
            failed.append(project_id)
            continue
        index_history_consent(project_id, settings is not None and settings.history_evaluation_consent)
        indexed += 1
    return {"indexed": indexed, "failed": failed}
