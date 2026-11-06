""" Public webhook delivery door (#7453) """
from datetime import UTC, datetime

from flask import Response, request  # pylint: disable=E0401
from pylon.core.tools import log  # pylint: disable=E0401,E0611
from tools import api_tools, db, register_openapi, this  # pylint: disable=E0401

from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.schedule_claim import fail_run
from ...utils.schedule_dispatch import START_FAILED, start_schedule_run
from ...utils.schedule_prerun import ACCESS_CHECK_UNAVAILABLE, check_owner_access, check_principal
from ...utils import webhook_door as door

# Run reasons of the pre-run check -> readable 403 codes
REFUSAL_CODES = {'principal does not match the owner': 'principal_mismatch', 'owner lost access': 'owner_lost_access'}


def _not_found():
    return Response(status=404)


def _busy(code: str):
    return {'code': code}, 503, {'Retry-After': str(door.RETRY_AFTER_SECONDS)}


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
        with db.get_session(project_id) as session:
            row = door.find_row(session, uuid)
            # Unknown, wrong type and bad signature look the same: nothing tells which part was wrong
            if row is None or not door.verify_delivery(row, headers, body, door.row_secrets(project_id, row, now)):
                door.log_rejected(row.webhook_type if row is not None else None)
                return _not_found()
            delivery = door.delivery_id(headers)
            log.info('Webhook %s delivery %s (type %s)', row.id, delivery, row.webhook_type)
            if not row.enabled:
                door.record_delivery(session, row, 'disabled', now)
                session.commit()
                return {'code': 'webhook_disabled', 'reason': row.disabled_reason}, 409
            if door.is_ping(row, headers):
                door.record_delivery(session, row, 'ping', now)
                session.commit()
                return {'ping': True}, 200
            if not door.claim_delivery(this.module.get_redis_client(), row.id, delivery):
                door.record_delivery(session, row, 'duplicate', now)
                session.commit()
                return {'duplicate': True}, 200
            refusal = check_principal(row) or check_owner_access(project_id, row.created_by)
            if refusal == ACCESS_CHECK_UNAVAILABLE:
                return _busy('access_check_unavailable')
            if refusal:
                door.record_delivery(session, row, 'refused', now)
                session.commit()
                return {'code': REFUSAL_CODES.get(refusal, 'refused')}, 403
            run_id = door.admit_webhook_run(session, row)
            if run_id is None:
                session.rollback()
                return _busy('run_in_progress')
            door.record_delivery(session, row, 'accepted', now)
            session.commit()
            if not start_schedule_run(row.entity_type, project_id, row.id, run_id, payload=body):
                fail_run(session, run_id, START_FAILED)
                session.commit()
                return _busy('no_free_slot')
        return {'run_id': run_id}, 202


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    url_params = api_tools.with_modes(['<int:project_id>/<string:uuid>'])

    mode_handlers = {
        PROMPT_LIB_MODE: HooksAPI,
    }
