""" Public webhook delivery door (#7453) """
from datetime import UTC, datetime

from flask import Response, request  # pylint: disable=E0401
from pylon.core.tools import log  # pylint: disable=E0401,E0611
from tools import api_tools, db, register_openapi, this  # pylint: disable=E0401

from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.schedule_claim import fail_run
from ...utils.schedule_dispatch import START_FAILED, start_schedule_run
from ...utils.schedule_prerun import (
    ACCESS_CHECK_UNAVAILABLE, OWNER_LOST_ACCESS, PRINCIPAL_MISMATCH, check_owner_access, check_principal,
)
from ...utils import webhook_door as door

REFUSAL_CODES = {PRINCIPAL_MISMATCH: 'principal_mismatch', OWNER_LOST_ACCESS: 'owner_lost_access'}


def _busy(code: str):
    return {'code': code}, 503, {'Retry-After': str(door.RETRY_AFTER_SECONDS)}


def _record(project_id: int, row_id: int, outcome: str, now) -> None:
    with db.get_session(project_id) as session:
        door.record_delivery(session, row_id, outcome, now)
        session.commit()


class HooksAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    @register_openapi(
        name="Webhook Delivery",
        description="Deliver a signed event to a webhook trigger; the run starts as the webhook owner.",
        tags=["elitea_core/schedules"],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "uuid", "in": "path", "required": True, "schema": {"type": "string", "format": "uuid"}},
        ],
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, uuid: str):  # pylint: disable=R0911
        """ POST """
        try:
            body = door.read_capped_body(request.stream, request.content_length)
        except door.BodyTooLarge:
            return {'code': 'payload_too_large'}, 413
        headers = request.headers
        now = datetime.now(UTC)
        # The connection is held only for the row read; vault, Redis and auth run without it
        with db.get_session(project_id) as session:
            row = door.find_row(session, uuid)
            if row is not None:
                session.expunge(row)
        # Unknown, wrong type and bad signature look the same: nothing tells which part was wrong
        if row is None or not door.verify_delivery(row, headers, body, door.row_secrets(project_id, row, now)):
            door.log_rejected(row.webhook_type if row is not None else None)
            return Response(status=404)
        delivery = door.delivery_id(headers)
        log.info('Webhook %s delivery %s (type %s)', row.id, delivery, row.webhook_type)
        if not row.enabled:
            _record(project_id, row.id, 'disabled', now)
            return {'code': 'webhook_disabled', 'reason': row.disabled_reason}, 409
        if door.is_ping(row, headers):
            _record(project_id, row.id, 'ping', now)
            return {'ping': True}, 200
        redis = this.module.get_redis_client()
        if not door.claim_delivery(redis, row.uuid, delivery):
            _record(project_id, row.id, 'duplicate', now)
            return {'duplicate': True}, 200
        try:
            answer, outcome = self._admit_and_start(project_id, row, body, now)
        except Exception:
            door.release_delivery(redis, row.uuid, delivery)  # a 500 must leave the retry runnable
            raise
        if outcome != 'accepted':  # a retry with the same delivery id must not be a duplicate
            door.release_delivery(redis, row.uuid, delivery)
        return answer

    @staticmethod
    def _admit_and_start(project_id: int, row, body: bytes, now):
        refusal = check_principal(row) or check_owner_access(project_id, row.created_by)
        if refusal == ACCESS_CHECK_UNAVAILABLE:
            return _busy('access_check_unavailable'), 'unavailable'
        if refusal:
            _record(project_id, row.id, 'refused', now)
            return ({'code': REFUSAL_CODES.get(refusal, 'refused')}, 403), 'refused'
        with db.get_session(project_id) as session:
            run_id = door.admit_webhook_run(session, row.id)
            if run_id is None:
                session.rollback()
                return _busy('run_in_progress'), 'busy'
            session.commit()
        if not start_schedule_run(row.entity_type, project_id, row.id, run_id, payload=body):
            with db.get_session(project_id) as session:
                fail_run(session, run_id, START_FAILED)
                door.record_delivery(session, row.id, 'start_failed', now)
                session.commit()
            return _busy('no_free_slot'), 'busy'
        _record(project_id, row.id, 'accepted', now)
        return ({'run_id': run_id}, 202), 'accepted'


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    url_params = api_tools.with_modes(['<int:project_id>/<string:uuid>'])

    mode_handlers = {
        PROMPT_LIB_MODE: HooksAPI,
    }
