"""The public webhook door: cheap checks in order, then the hand-over to the run starter (#7453)."""
import time
from datetime import UTC, datetime
from uuid import UUID

from pylon.core.tools import log
from sqlalchemy import select, update
from tools import VaultClient

from ..models.entity_schedule import EntitySchedule
from .pipeline_trigger import (
    WEBHOOK_TYPE_CONFIG, get_webhook_signature_header, normalize_secret_value, validate_webhook_secret,
)
from .schedule_claim import insert_run
from .webhook_secrets import current_key, is_signing_row, prev_key

MAX_BODY_BYTES = 512 * 1024
DEDUP_SECONDS = 24 * 3600
RETRY_AFTER_SECONDS = 30
DELIVERY_ID_HEADERS = ('X-GitHub-Delivery', 'X-Gitlab-Event-UUID', 'webhook-id')
REJECT_LOG_INTERVAL = 60  # seconds between two "rejected" log lines of one process
_last_reject_log = 0.0


class BodyTooLarge(Exception):
    pass


def read_capped_body(stream, content_length=None) -> bytes:
    """At most MAX_BODY_BYTES; a chunked body has no length to trust, so the read itself is bounded."""
    if isinstance(content_length, int) and content_length > MAX_BODY_BYTES:
        raise BodyTooLarge()
    raw = stream.read(MAX_BODY_BYTES + 1)
    if len(raw) > MAX_BODY_BYTES:
        raise BodyTooLarge()
    return raw


def find_row(session, uuid: str):
    try:
        key = UUID(uuid)
    except ValueError:  # a malformed uuid is an unknown webhook
        return None
    return session.scalar(select(EntitySchedule).where(EntitySchedule.uuid == key,
                                                       EntitySchedule.trigger_kind == 'webhook'))


def row_secrets(project_id: int, row, now: datetime) -> list[str]:
    """The current secret, plus the rotated-out one while its overlap window is open."""
    hidden = VaultClient(project_id).get_hidden_secrets() or {}
    keys = [current_key(row)]
    until = row.previous_secret_until
    if until is not None and (until if until.tzinfo else until.replace(tzinfo=UTC)) > now:
        keys.append(prev_key(row.uuid))
    return [hidden[key] for key in keys if hidden.get(key)]


def verify_delivery(row, headers, body: bytes, secrets: list[str]) -> bool:
    """True when any of the row's secrets signs the request; the reason of a failure is never returned."""
    config = WEBHOOK_TYPE_CONFIG.get(row.webhook_type)
    signature = headers.get(get_webhook_signature_header(row.webhook_type, row.gitlab_auth_method))
    if config is None or not signature:
        return False
    signing = is_signing_row(row)
    for secret in secrets:
        valid, _ = validate_webhook_secret(
            webhook_type=row.webhook_type,
            secret_value=secret if signing else normalize_secret_value(secret),
            signature=signature,
            raw_data=body,
            auth_method=row.gitlab_auth_method,
            webhook_id=headers.get(config['signing_id_header']) if signing else None,
            timestamp=headers.get(config['signing_timestamp_header']) if signing else None,
        )
        if valid:
            return True
    return False


def delivery_id(headers) -> str | None:
    for name in DELIVERY_ID_HEADERS:
        value = headers.get(name)
        if value:
            return value[:200]
    return None


def is_ping(row, headers) -> bool:
    return row.webhook_type == 'github' and headers.get('X-GitHub-Event') == 'ping'


def _dedup_key(row_uuid, delivery: str) -> str:
    # The uuid, not the id: row ids repeat across project schemas and Redis is shared
    return f'webhook:{row_uuid}:{delivery}'


def claim_delivery(redis, row_uuid, delivery: str | None) -> bool:
    """False for a delivery id seen in the last 24 h; a Redis failure lets the delivery through."""
    if delivery is None:
        return True
    try:
        return bool(redis.set(_dedup_key(row_uuid, delivery), '1', nx=True, ex=DEDUP_SECONDS))
    except Exception:  # noqa: BLE001
        log.warning('Webhook %s delivery %s: dedup unavailable, accepted', row_uuid, delivery)
        return True


def release_delivery(redis, row_uuid, delivery: str | None) -> None:
    """Forget a claimed id when the delivery was not run, so the sender's retry is not a duplicate."""
    if delivery is None:
        return
    try:
        redis.delete(_dedup_key(row_uuid, delivery))
    except Exception:  # noqa: BLE001 -- the key expires in 24 h; the retry is then refused as a duplicate
        log.warning('Webhook %s delivery %s: cannot release the dedup claim', row_uuid, delivery)


def admit_webhook_run(session, row_id: int, trigger: str = 'webhook', requested_by: int | None = None) -> int | None:
    """Insert the webhook run row (`test` for a sample body); the caps of #7400 go here."""
    return insert_run(session, row_id, trigger, requested_by)


def record_delivery(session, row_id: int, outcome: str, now: datetime) -> None:
    session.execute(update(EntitySchedule).where(EntitySchedule.id == row_id)
                    .values(last_delivery_at=now, last_delivery_outcome=outcome))


def log_rejected(webhook_type: str | None) -> None:
    """At most one line a minute: a flood of bad signatures must not flood the log."""
    global _last_reject_log  # pylint: disable=W0603
    moment = time.monotonic()
    if moment - _last_reject_log >= REJECT_LOG_INTERVAL:
        _last_reject_log = moment
        log.info('Webhook delivery rejected (type %s)', webhook_type)
