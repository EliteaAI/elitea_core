"""The identity an autonomous mandate's runs act with (#7131, C-1009): the owner's offline delegated consent.

A scheduled or webhook run has no signed-in person, so an Entra-delegated credential (a token minted at release on behalf
of a person) has nobody to act for. A mandate may name its identity (``identity_source``, part of its approved scope):

- ``owner_delegated``: its runs act with the mandate owner's Microsoft sign-in. The owner consents once, while signed
  in (``consent``): auth_oidc hands out one refresh token of their kept sign-in, bound to this mandate, which the secrets
  plugin keeps as the owner's offline material (by reference; never in a mandate row, an event or a log line). At
  release the material is read (``release_person``) for the mandate's own holder and the owner's frozen subject only,
  and ``entra_obo.acquire_offline`` redeems it. Withdrawing (``withdraw``) retires the material; signing out does not
  (it is offline by design).
- ``application``: named for the record; the runs use the application credentials they are granted (SharePoint
  App-only), exactly as a mandate without an identity.

The deployment switch ``mandate_identity_sources`` (plugin config) lists what may be named. Empty, the default: nothing
here reads anything, and a mandate run with a delegated credential is refused as before.

Every check fails closed with a fixed code; nothing here returns or logs a token.
"""
import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import SecretStr
from pylon.core.tools import log
from sqlalchemy import select, update
from tools import db, this

from ..models.agent_identity import LIVE_MANDATE_STATES, AutonomousMandate
from . import entra_obo

OWNER_DELEGATED = "owner_delegated"
APPLICATION = "application"
IDENTITY_SOURCES = (OWNER_DELEGATED, APPLICATION)
CONFIG_KEY = "mandate_identity_sources"
CONSENT_REQUIRED = entra_obo.CONSENT_REQUIRED
CONSENT_EXPIRED = entra_obo.CONSENT_EXPIRED
GRANT_RPC = "auth_oidc_obo_offline_grant"
RELEASE_ACTOR = "elitea_core:release"
SCHEDULE_TRIGGER = "schedule:entity:"


class IdentityRefused(Exception):
    """A consent or withdrawal that cannot be done; ``code`` says why (``MESSAGES``)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


MESSAGES = {
    "IDENTITY_SOURCE_NOT_ALLOWED": "This platform does not allow mandates to name this identity.",
    "IDENTITY_NOT_DELEGATED": "This mandate does not act with its owner's sign-in.",
    "CONSENT_OWNER_ONLY": "Only the mandate's owner can give this consent, signed in as themselves.",
    "MANDATE_NOT_FOUND": "No such mandate in this project.",
    "INVALID_STATE": "The mandate is not in a state that allows this action.",
    "CONSENT_NOT_GIVEN": "The mandate has no consent to withdraw.",
    "DENIED_SUBJECT_MISMATCH": "You are signed in with another identity than the one the mandate was made for.",
    "DENIED_ASSERTION_UNAVAILABLE": "Your Microsoft sign-in is not available. Sign in with Microsoft again, then retry.",
    "DENIED_DELEGATION_UNSUPPORTED_CALLER": "Consent needs your own signed-in session, not an API token.",
    "DEPENDENCY_UNAVAILABLE": "A service did not answer; nothing was changed. Try again.",
    "AUDIT_UNAVAILABLE": "The audit trail is unavailable; nothing was changed. Try again later.",
    "CONCURRENT_CHANGE": "The mandate changed while this ran; read it again and retry.",
}


def _plugin_config(module) -> dict:
    config = getattr(getattr(module, "descriptor", None), "config", None)
    return config if isinstance(config, dict) else {}


def allowed_sources(module) -> tuple:
    """The identities this deployment lets a mandate name (``mandate_identity_sources``); unknown names are ignored."""
    found = _plugin_config(module).get(CONFIG_KEY) or ()
    if not isinstance(found, (list, tuple)):   # a plain string would match by substring
        return ()
    return tuple(source for source in IDENTITY_SOURCES if source in found)


def owner_delegated_allowed(module) -> bool:
    return OWNER_DELEGATED in allowed_sources(module)


def holder_of(mandate_id) -> str:
    return f"mandate:{mandate_id}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(moment):
    return moment if moment is None or moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def _tracing():
    return this.for_module("tracing").module


def _secrets():
    return this.for_module("secrets").module


# --- admission ----------------------------------------------------------------------------------------------------

def _read_identity(project_id, mandate_id):
    with db.get_session(project_id) as session:
        return session.execute(select(
            AutonomousMandate.identity_source, AutonomousMandate.identity_material_ref,
            AutonomousMandate.identity_consented_at,
        ).where(AutonomousMandate.id == mandate_id)).one_or_none()


def identity_reader(module, project_id, mandate_id, trigger_ref=None, owner_user_id=None):
    """For the credential decision of a mandate run: a callable answering ``{"source", "consented"}`` of the mandate
    (read once, when a delegated credential is first decided), or ``None`` when the deployment allows no owner
    identity (nothing is ever read then). ``consented`` is False when the consent is missing, and also when the run is
    a schedule's that someone other than its owner changed after the consent was given (C9-SEC-46: the owner confirms
    by consenting again, or by saving the schedule themselves)."""
    if not owner_delegated_allowed(module):
        return None
    answers = []

    def read() -> Optional[dict]:
        if not answers:
            row = _read_identity(project_id, mandate_id)
            if row is None:
                answers.append(None)
            else:
                consented = row.identity_material_ref is not None and row.identity_consented_at is not None
                if consented and row.identity_source == OWNER_DELEGATED:
                    consented = _schedule_confirmed(project_id, trigger_ref, owner_user_id,
                                                    _aware(row.identity_consented_at))
                answers.append({"source": row.identity_source, "consented": consented})
        return answers[0]

    return read


def _schedule_confirmed(project_id, trigger_ref, owner_user_id, consented_at) -> bool:
    """A schedule run acts with the owner's sign-in only when the owner made its last change, or consented after it."""
    if not isinstance(trigger_ref, str) or not trigger_ref.startswith(SCHEDULE_TRIGGER):
        return True
    try:
        schedule_id = int(trigger_ref[len(SCHEDULE_TRIGGER):])
    except ValueError:
        return False
    change = _schedule_change(project_id, schedule_id)
    if change is None:
        return False
    updated_by, updated_at = change
    if updated_by is None or updated_by == owner_user_id:
        return True
    updated_at = _aware(updated_at)
    return updated_at is not None and consented_at >= updated_at


def _schedule_change(project_id, schedule_id):
    """``(updated_by, updated_at)`` of a schedule, or ``None``: one read, only for a delegated mandate run."""
    from ..models.entity_schedule import EntitySchedule  # pylint: disable=C0415  # only this path reads a schedule
    with db.get_session(project_id) as session:
        row = session.execute(select(EntitySchedule.updated_by, EntitySchedule.updated_at)
                              .where(EntitySchedule.id == schedule_id)).one_or_none()
    return None if row is None else (row.updated_by, row.updated_at)


# --- release ------------------------------------------------------------------------------------------------------

class OfflineMaterial:  # pylint: disable=R0903
    """The owner's consent as the release reads it: where it is, whose it is, the token (``SecretStr``) and how to keep
    a rotated one. Its repr names no value."""

    __slots__ = ("ref", "holder", "source", "generation", "refresh_token", "project_id")

    def __init__(self, ref, holder, source, generation, refresh_token, project_id):  # pylint: disable=R0913
        self.ref, self.holder, self.source, self.generation = ref, holder, source, generation
        self.refresh_token, self.project_id = refresh_token, project_id

    def __repr__(self):
        return f"OfflineMaterial(generation={self.generation})"

    def rotate(self, refresh_token) -> None:
        """Keep the refresh token the authority issued on a redemption (best effort: the old one still works)."""
        try:
            answer = _secrets().rotate_offline_material(
                self.project_id, self.ref, expected_generation=self.generation,
                refresh_token=SecretStr(refresh_token), caller_ref=RELEASE_ACTOR)
            if not isinstance(answer, dict) or answer.get("ok") is not True:
                log.warning("mandate identity: a rotated consent was not kept (%s)",
                            (answer or {}).get("reason_code") if isinstance(answer, dict) else "no answer")
        except Exception as exc:  # pylint: disable=W0703
            log.error("mandate identity: a rotated consent was not kept (%s)", type(exc).__name__)


def mandate_id_of(subject) -> Optional[uuid.UUID]:
    ref = str((subject or {}).get("ref") or "")
    try:
        return uuid.UUID(ref[len("mandate:"):]) if ref.startswith("mandate:") else None
    except ValueError:
        return None


def release_person(project_id, subject, tenant, caller) -> tuple:
    """``(person, OfflineMaterial)`` for an Entra-delegated slot of a mandate run: the owner's subject frozen in the
    mandate, which must be the one the run was admitted with, and their consent read for this mandate and this person
    only. Raises ``entra_obo.DelegationRefused``. Called after the release's own checks of the mandate (state, epoch,
    binding); reads the mandate's identity once and the material once."""
    mandate_id = mandate_id_of(subject)
    if mandate_id is None or not isinstance(caller, dict) or caller.get("kind") != "trigger":
        raise entra_obo.DelegationRefused(entra_obo.CALLER_UNSUPPORTED)
    with db.get_session(project_id) as session:
        row = session.execute(select(
            AutonomousMandate.owner_user_ref, AutonomousMandate.identity_source,
            AutonomousMandate.identity_material_ref,
        ).where(AutonomousMandate.id == mandate_id)).one_or_none()
    if row is None or row.identity_source != OWNER_DELEGATED:
        raise entra_obo.DelegationRefused(entra_obo.MANDATE_UNSUPPORTED)
    if row.identity_material_ref is None:
        raise entra_obo.DelegationRefused(CONSENT_REQUIRED)
    owner = row.owner_user_ref if isinstance(row.owner_user_ref, dict) else {}
    person = {"issuer": owner.get("issuer"), "tid": owner.get("tid"), "oid": owner.get("oid")}
    if not all(isinstance(value, str) and value for value in person.values()):
        raise entra_obo.DelegationRefused(entra_obo.SUBJECT_MISMATCH)
    admitted = (subject.get("issuer"), tenant, subject.get("oid"))
    if admitted != (person["issuer"], person["tid"], person["oid"]):
        # admitted before the identity was stored, or the owner's subject is not the one the run carries
        raise entra_obo.DelegationRefused(entra_obo.SUBJECT_MISMATCH if all(admitted) else entra_obo.STALE)
    holder = holder_of(mandate_id)
    try:
        answer = _secrets().release_offline_material(
            project_id, str(row.identity_material_ref), holder_ref=holder,
            custodian_subject_ref=f"idp:{person['tid']}:{person['oid']}", release_id=str(uuid.uuid4()),
            actor_ref=RELEASE_ACTOR)
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the consent could not be read (%s)", type(exc).__name__)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE) from None
    if not isinstance(answer, dict) or answer.get("ok") is not True:
        code = answer.get("reason_code") if isinstance(answer, dict) else None
        if code in ("OFFLINE_MATERIAL_INACTIVE", "OFFLINE_MATERIAL_MISMATCH", "STORE_NOT_FOUND"):
            raise entra_obo.DelegationRefused(CONSENT_REQUIRED)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE)
    return person, OfflineMaterial(str(row.identity_material_ref), holder, answer.get("source"),
                                   answer.get("generation"), answer.get("refresh_token"), project_id)


def consent_refused(project_id, subject, material: OfflineMaterial) -> None:
    """The authority refused the consent itself (expired, revoked at Microsoft): the mandate needs a new consent. Best
    effort: the run is refused either way, and the next one finds the same answer."""
    mandate_id = mandate_id_of(subject)
    try:
        _clear(project_id, mandate_id, material.ref, "MANDATE_IDENTITY_CONSENT_REFUSED", "elitea_core:release",
               retire_reason="CONSENT_REFUSED")
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: a refused consent was not withdrawn (%s)", type(exc).__name__)


# --- consent and withdrawal -----------------------------------------------------------------------------------------

def _event(event_type, reason_code, mandate, caller_ref, **fields) -> dict:
    owner = mandate.owner_user_ref if isinstance(mandate.owner_user_ref, dict) else {}
    event = {
        "event_type": event_type, "reason_code": reason_code, "outcome": "success",
        "project_id": mandate.project_id, "application_id": mandate.application_id,
        "agent_binding_ref": str(mandate.agent_binding_ref), "mandate_ref": str(mandate.id), "caller_ref": caller_ref,
        "user_ref": f"idp:{owner.get('tid')}:{owner.get('oid')}" if owner.get("tid") else None, **fields,
    }
    return {key: value for key, value in event.items() if value is not None}


def _read_mandate(session, project_id, mandate_id):
    return session.execute(select(
        AutonomousMandate.id, AutonomousMandate.project_id, AutonomousMandate.application_id,
        AutonomousMandate.agent_binding_ref, AutonomousMandate.owner_user_id, AutonomousMandate.owner_user_ref,
        AutonomousMandate.state, AutonomousMandate.valid_until, AutonomousMandate.identity_source,
        AutonomousMandate.identity_material_ref,
    ).where(AutonomousMandate.id == mandate_id, AutonomousMandate.project_id == project_id)).one_or_none()


def _retire(project_id, material_ref, reason, caller_ref, actor_user_id=None) -> None:
    entra_obo.forget_material(material_ref)
    try:
        answer = _secrets().retire_offline_material(
            project_id, str(material_ref), reason=reason, caller_ref=caller_ref, actor_user_id=actor_user_id)
        if not isinstance(answer, dict) or answer.get("ok") is not True:
            log.error("mandate identity: a consent was not retired (%s)",
                      answer.get("reason_code") if isinstance(answer, dict) else "no answer")
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: a consent was not retired (%s)", type(exc).__name__)


def _clear(project_id, mandate_id, material_ref, reason_code, caller_ref, *, retire_reason, actor_user_id=None):
    """Withdraw the consent ``material_ref`` from the mandate (if it is still the mandate's), with its event, then retire
    the material. True when this call withdrew it."""
    with db.get_session(project_id) as session:
        mandate = _read_mandate(session, project_id, mandate_id)
        if mandate is None:
            return False
        moved = session.execute(update(AutonomousMandate).where(
            AutonomousMandate.id == mandate_id,
            AutonomousMandate.identity_material_ref == uuid.UUID(str(material_ref)),
        ).values(identity_material_ref=None, identity_consented_at=None).execution_options(
            synchronize_session=False)).rowcount
        if moved:
            _tracing().security_audit_record(session, _event(
                "withdrawal", reason_code, mandate, caller_ref, credential_ref=str(material_ref)))
        session.commit()
    _retire(project_id, material_ref, retire_reason, caller_ref, actor_user_id)
    return bool(moved)


def _grant(module, person, holder) -> dict:
    """auth_oidc's one refresh token of the person's kept sign-in, for ``holder``: ``{refresh_token, assertion_source}``."""
    rpc = getattr(getattr(module, "context", None), "rpc_manager", None)
    key = entra_obo.custody_key(module)
    if rpc is None or key is None:
        log.error("mandate identity: the custody service or its request key is not configured")
        raise IdentityRefused("DEPENDENCY_UNAVAILABLE")
    iat, nonce = int(time.time()), secrets.token_urlsafe(16)
    fields = ("v1-offline-grant", person["tid"], person["oid"], holder)
    try:
        answer = getattr(rpc.timeout(entra_obo.RPC_TIMEOUT_S), GRANT_RPC)(
            tid=person["tid"], oid=person["oid"], holder=holder, iat=iat, nonce=nonce,
            signature=entra_obo.sign_fields(key, fields, iat, nonce))
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the custody service did not answer (%s)", type(exc).__name__)
        raise IdentityRefused("DEPENDENCY_UNAVAILABLE") from None
    if not isinstance(answer, dict) or answer.get("ok") is not True:
        code = answer.get("code") if isinstance(answer, dict) else None
        raise IdentityRefused(entra_obo.ASSERTION_UNAVAILABLE if code == "ASSERTION_UNAVAILABLE"
                              else "DEPENDENCY_UNAVAILABLE")
    token, source = answer.get("refresh_token"), answer.get("assertion_source")
    if not isinstance(token, str) or not token or source not in ("login", "broker"):
        raise IdentityRefused(entra_obo.ASSERTION_UNAVAILABLE)
    return {"refresh_token": token, "source": source}


def _verified_person(module, actor_user_id) -> dict:
    """``{issuer, tid, oid}`` of the signed-in session asking (verified as a run's admission verifies it), never a
    value of the request: the governed probe's own check (#7281)."""
    from . import credential_probe  # pylint: disable=C0415  # the probe owns the session verification
    from .exceptions import AdmissionDenied  # pylint: disable=C0415
    try:
        return credential_probe._probe_person(module, actor_user_id)  # pylint: disable=W0212
    except AdmissionDenied as denied:
        code = denied.reason_code
        raise IdentityRefused(code if code in MESSAGES else entra_obo.ASSERTION_UNAVAILABLE) from None


def consent(module, project_id, mandate_id, actor_user_id) -> dict:  # pylint: disable=R0914
    """The owner consents, while signed in, to the mandate's runs acting with their Microsoft sign-in: ``{"mandate_ref",
    "consented_at"}``, or raises ``IdentityRefused``. A consent given again replaces the earlier one (retired)."""
    if not owner_delegated_allowed(module):
        raise IdentityRefused("IDENTITY_SOURCE_NOT_ALLOWED")
    with db.get_session(project_id) as session:
        mandate = _read_mandate(session, project_id, mandate_id)
    if mandate is None:
        raise IdentityRefused("MANDATE_NOT_FOUND")
    if mandate.identity_source != OWNER_DELEGATED:
        raise IdentityRefused("IDENTITY_NOT_DELEGATED")
    if mandate.state not in LIVE_MANDATE_STATES or _aware(mandate.valid_until) <= _now():
        raise IdentityRefused("INVALID_STATE")
    if actor_user_id != mandate.owner_user_id:
        raise IdentityRefused("CONSENT_OWNER_ONLY")
    owner = mandate.owner_user_ref if isinstance(mandate.owner_user_ref, dict) else {}
    person = _verified_person(module, actor_user_id)
    if (person["issuer"], person["tid"], person["oid"]) != (owner.get("issuer"), owner.get("tid"), owner.get("oid")):
        raise IdentityRefused("DENIED_SUBJECT_MISMATCH")   # signed in as another identity than the mandate's owner
    holder = holder_of(mandate.id)
    granted = _grant(module, person, holder)
    try:
        kept = _secrets().keep_offline_material(
            project_id, holder_ref=holder, custodian_user_id=actor_user_id,
            custodian_subject_ref=f"idp:{person['tid']}:{person['oid']}", source=granted["source"],
            refresh_token=SecretStr(granted["refresh_token"]), actor_user_id=actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the consent could not be kept (%s)", type(exc).__name__)
        kept = None
    granted = None
    if not isinstance(kept, dict) or kept.get("ok") is not True:
        code = kept.get("reason_code") if isinstance(kept, dict) else None
        raise IdentityRefused("AUDIT_UNAVAILABLE" if code == "AUDIT_UNAVAILABLE" else "DEPENDENCY_UNAVAILABLE")
    material_ref, earlier, now = uuid.UUID(kept["material_ref"]), mandate.identity_material_ref, _now()
    caller_ref = f"user:{actor_user_id}"
    try:
        with db.get_session(project_id) as session:
            condition = (AutonomousMandate.identity_material_ref.is_(None) if earlier is None
                         else AutonomousMandate.identity_material_ref == earlier)
            moved = session.execute(update(AutonomousMandate).where(
                AutonomousMandate.id == mandate.id, AutonomousMandate.state.in_(LIVE_MANDATE_STATES),
                AutonomousMandate.identity_source == OWNER_DELEGATED, condition,
            ).values(identity_material_ref=material_ref, identity_consented_at=now).execution_options(
                synchronize_session=False)).rowcount
            if moved != 1:
                session.rollback()
                raise IdentityRefused("CONCURRENT_CHANGE")
            _tracing().security_audit_record(session, _event(
                "approval", "MANDATE_IDENTITY_CONSENTED", mandate, caller_ref, credential_ref=str(material_ref)))
            session.commit()
    except IdentityRefused:
        _retire(project_id, material_ref, "CONSENT_REPLACED", caller_ref, actor_user_id)
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the consent was not recorded (%s)", type(exc).__name__)
        _retire(project_id, material_ref, "CONSENT_REPLACED", caller_ref, actor_user_id)
        raise IdentityRefused("AUDIT_UNAVAILABLE") from None
    if earlier is not None:
        _retire(project_id, earlier, "CONSENT_REPLACED", caller_ref, actor_user_id)
    return {"mandate_ref": str(mandate.id), "consented_at": now.isoformat()}


def withdraw(module, project_id, mandate_id, actor_user_id) -> dict:
    """Withdraw the mandate's consent (its owner, or an operator the API let in): the next run is refused
    ``DENIED_MANDATE_CONSENT_REQUIRED``. ``{"mandate_ref"}`` or raises ``IdentityRefused``."""
    if not owner_delegated_allowed(module):
        raise IdentityRefused("IDENTITY_SOURCE_NOT_ALLOWED")
    with db.get_session(project_id) as session:
        mandate = _read_mandate(session, project_id, mandate_id)
    if mandate is None:
        raise IdentityRefused("MANDATE_NOT_FOUND")
    if mandate.identity_material_ref is None:
        raise IdentityRefused("CONSENT_NOT_GIVEN")
    try:
        withdrawn = _clear(project_id, mandate.id, mandate.identity_material_ref, "MANDATE_IDENTITY_WITHDRAWN",
                           f"operator:{actor_user_id}", retire_reason="CONSENT_WITHDRAWN",
                           actor_user_id=actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the consent was not withdrawn (%s)", type(exc).__name__)
        raise IdentityRefused("AUDIT_UNAVAILABLE") from None
    if not withdrawn:
        raise IdentityRefused("CONCURRENT_CHANGE")
    return {"mandate_ref": str(mandate.id)}


def retire_with_mandate(module, project_id, mandate_id, actor_user_id) -> None:
    """The mandate was revoked: its consent goes with it (best effort, after the revocation committed)."""
    if not owner_delegated_allowed(module):
        return
    try:
        with db.get_session(project_id) as session:
            mandate = _read_mandate(session, project_id, mandate_id)
        if mandate is not None and mandate.identity_material_ref is not None:
            _clear(project_id, mandate_id, mandate.identity_material_ref, "MANDATE_IDENTITY_WITHDRAWN",
                   f"operator:{actor_user_id}", retire_reason="HOLDER_REVOKED", actor_user_id=actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("mandate identity: the consent of a revoked mandate was not retired (%s)", type(exc).__name__)
