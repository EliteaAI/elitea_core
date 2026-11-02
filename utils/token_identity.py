"""A scoped API token acting with its owner's Microsoft sign-in (#7337, C-1009): the owner's offline consent, held by
the token.

A run started with a scoped API token (#7336) is its owner's run, but nobody is signed in behind it, so an
Entra-delegated credential has no live sign-in to act with. The owner may consent once, in their own signed-in session
(``consent``): exactly #7131's offline consent of a mandate owner, with ``token:<id>`` as the holder. auth_oidc hands
out one refresh token of their kept sign-in bound to that holder, the secrets plugin keeps it (by reference; never in
a row, an event or a log line), and ``api_token_identity_consent`` records where. At release the material is read for
this holder and the owner's linked subject only (``release_person``) and ``entra_obo.acquire_offline`` redeems it.
Withdrawing (``withdraw``), deleting the token or its expiry (``retire``) end it; signing out does not.

The deployment switch is the same list as the mandates' (``mandate_identity_sources``) with the source
``token_owner_delegated``. Without it: nothing here reads anything, a token run with a delegated credential is refused
at release as before (``DENIED_DELEGATION_UNSUPPORTED_CALLER``), and the table is not even created.

Every check fails closed with a fixed code; nothing here returns or logs a token.
"""
import uuid
from typing import Optional

from pydantic import SecretStr
from pylon.core.tools import log
from sqlalchemy import delete, select
from tools import auth, db

from ..models.api_token_identity import METADATA, ApiTokenIdentityConsent
from . import entra_obo
from .mandate_identity import (
    CONFIG_KEY, RELEASE_ACTOR, IdentityRefused, OfflineMaterial, _grant, _now, _plugin_config, _retire, _secrets,
    _tracing, _verified_person,
)

TOKEN_SOURCE = "token_owner_delegated"
CONSENT_REQUIRED = entra_obo.TOKEN_CONSENT_REQUIRED
CONSENT_EXPIRED = entra_obo.TOKEN_CONSENT_EXPIRED
HOLDER_PREFIX = "token:"

MESSAGES = {
    "IDENTITY_SOURCE_NOT_ALLOWED": "This platform does not let API tokens act with their owner's Microsoft sign-in.",
    "TOKEN_NOT_FOUND": "No such API token of yours.",
    "TOKEN_NOT_SCOPED": "Only a restricted token (projects, operations and an expiry) can act with your Microsoft "
                        "sign-in.",
    "INVALID_STATE": "The token has expired.",
    "CONSENT_NOT_GIVEN": "The token has no consent to withdraw.",
    "OWNER_NOT_LINKED": "Your account is not linked to a Microsoft sign-in; sign in with Microsoft first.",
    "DENIED_SUBJECT_MISMATCH": "You are signed in with another identity than the one your account is linked to.",
    "DENIED_ASSERTION_UNAVAILABLE": "Your Microsoft sign-in is not available. Sign in with Microsoft again, then retry.",
    "DENIED_DELEGATION_UNSUPPORTED_CALLER": "Consent needs your own signed-in session, not an API token.",
    "DEPENDENCY_UNAVAILABLE": "A service did not answer; nothing was changed. Try again.",
    "AUDIT_UNAVAILABLE": "The audit trail is unavailable; nothing was changed. Try again later.",
    "CONCURRENT_CHANGE": "The token's consent changed while this ran; read it again and retry.",
    "GOVERNANCE_NOT_ENFORCED": "Consent applies only while credential governance is enforced; nothing was changed.",
}


def allowed(module) -> bool:
    """Does the deployment let scoped API tokens act with their owner's consent (``mandate_identity_sources`` lists
    ``token_owner_delegated``)? A plain string is no list (it would match by substring)."""
    found = _plugin_config(module).get(CONFIG_KEY) or ()
    return isinstance(found, (list, tuple)) and TOKEN_SOURCE in found


def holder_of(token_id) -> str:
    return f"{HOLDER_PREFIX}{int(token_id)}"


def token_id_of(caller) -> Optional[int]:
    """The token id of a run's caller ``{"kind": "api_token", "ref": "token:<id>"}``, else None."""
    if not isinstance(caller, dict) or caller.get("kind") != "api_token":
        return None
    ref = str(caller.get("ref") or "")
    digits = ref[len(HOLDER_PREFIX):] if ref.startswith(HOLDER_PREFIX) else ""
    return int(digits) if digits.isdigit() else None


def ensure_table() -> None:
    """Create the consent table and its index where missing (the start of a deployment that lists the source).
    Idempotent: ``create_all`` and the index creation look at the catalog first (a plain ``CREATE`` then); a second
    replica that loses the race fails here, is logged by the caller, and finds the table in place."""
    with db.get_session(None) as session:
        connection = session.connection()
        METADATA.create_all(connection, checkfirst=True)
        for index in ApiTokenIdentityConsent.__table__.indexes:   # a table created before its index (#7337 review N-2)
            index.create(connection, checkfirst=True)
        session.commit()


def _read(token_id):
    with db.get_session(None) as session:
        return session.execute(select(
            ApiTokenIdentityConsent.owner_user_id, ApiTokenIdentityConsent.material_project_id,
            ApiTokenIdentityConsent.material_ref, ApiTokenIdentityConsent.consented_at,
        ).where(ApiTokenIdentityConsent.token_id == token_id)).one_or_none()


# --- admission ----------------------------------------------------------------------------------------------------

def identity_reader(module, token_id, owner_user_id):
    """For the credential decision of a scoped token's run: a callable answering whether the token's owner consented
    (read once, when a delegated credential first reaches check 4b), or ``None`` when the deployment does not allow the
    source (nothing is ever read then, and the decision is the one before #7337)."""
    if not allowed(module):
        return None
    answers = []

    def read() -> bool:
        if not answers:
            row = _read(token_id)
            answers.append(row is not None and row.owner_user_id == owner_user_id)
        return answers[0]

    return read


# --- release ------------------------------------------------------------------------------------------------------

def _token_live(token_id, owner_user_id) -> None:
    """The token still exists, has not expired and is its owner's (the auth plugin's admission verdict, epoch-cached):
    a deleted or expired token's consent (or one of a deleted or suspended owner) is retired here if neither an event
    nor the sweep did it yet, and nothing is released."""
    try:
        epochs = auth.read_identity_epochs("token", token_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the token could not be read (%s)", type(exc).__name__)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE) from None
    if not isinstance(epochs, dict):
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE)
    if epochs.get("active") is True and epochs.get("user_id") == owner_user_id:
        return
    ended = _ENDED.get(epochs.get("reason"))
    if ended is not None:
        retire(token_id, ended[0], RELEASE_ACTOR, retire_reason=ended[1])
    raise entra_obo.DelegationRefused(CONSENT_REQUIRED)


def release_person(subject, tenant, caller) -> tuple:
    """``(person, OfflineMaterial)`` for an Entra-delegated slot of a scoped token's run: the owner's linked subject
    the run was admitted with, the token still live and its owner's, and the consent kept for this token and this
    person only (one select, then the secrets plugin's own holder and custodian check). Raises
    ``entra_obo.DelegationRefused``. Called after the release's own checks of the run (context, binding, grants)."""
    token_id = token_id_of(caller)
    owner = caller.get("user_id") if isinstance(caller, dict) else None
    if token_id is None or isinstance(owner, bool) or not isinstance(owner, int):
        raise entra_obo.DelegationRefused(entra_obo.CALLER_UNSUPPORTED)
    subject = subject if isinstance(subject, dict) else {}
    person = {"issuer": subject.get("issuer"), "tid": tenant, "oid": subject.get("oid")}
    if subject.get("kind") != "api_token" or not all(isinstance(value, str) and value for value in person.values()):
        raise entra_obo.DelegationRefused(entra_obo.STALE)   # admitted before the consent was asked for
    _token_live(token_id, owner)
    try:
        row = _read(token_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent could not be read (%s)", type(exc).__name__)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE) from None
    if row is None or row.owner_user_id != owner:
        raise entra_obo.DelegationRefused(CONSENT_REQUIRED)
    holder = holder_of(token_id)
    try:
        answer = _secrets().release_offline_material(
            row.material_project_id, str(row.material_ref), holder_ref=holder,
            custodian_subject_ref=f"idp:{person['tid']}:{person['oid']}", release_id=str(uuid.uuid4()),
            actor_ref=RELEASE_ACTOR)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent could not be read (%s)", type(exc).__name__)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE) from None
    if not isinstance(answer, dict) or answer.get("ok") is not True:
        code = answer.get("reason_code") if isinstance(answer, dict) else None
        if code in ("OFFLINE_MATERIAL_INACTIVE", "OFFLINE_MATERIAL_MISMATCH", "STORE_NOT_FOUND"):
            # retired, another holder's (a mandate's), or another person's (the owner was linked again since)
            raise entra_obo.DelegationRefused(CONSENT_REQUIRED)
        raise entra_obo.DelegationRefused(entra_obo.UNAVAILABLE)
    return person, OfflineMaterial(str(row.material_ref), holder, answer.get("source"), answer.get("generation"),
                                   answer.get("refresh_token"), row.material_project_id)


def consent_refused(caller, material: OfflineMaterial) -> None:
    """Microsoft refused the consent itself (expired, revoked there): the token needs a new consent. Best effort."""
    token_id = token_id_of(caller)
    if token_id is None:
        return
    try:
        _clear(token_id, material.ref, "TOKEN_IDENTITY_CONSENT_REFUSED", RELEASE_ACTOR, retire_reason="CONSENT_REFUSED")
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: a refused consent was not withdrawn (%s)", type(exc).__name__)


# --- consent and withdrawal ----------------------------------------------------------------------------------------

def _event(event_type, reason_code, token_id, project_id, caller_ref, user_ref, material_ref) -> dict:
    event = {"event_type": event_type, "reason_code": reason_code, "outcome": "success", "project_id": project_id,
             "caller_ref": caller_ref, "user_ref": user_ref, "target_ref": holder_of(token_id),
             "credential_ref": str(material_ref)}
    return {key: value for key, value in event.items() if value is not None}


def _clear(token_id, material_ref, reason_code, caller_ref, *, retire_reason, actor_user_id=None) -> bool:
    """Remove the consent ``material_ref`` of the token (if it is still the token's), with its event, then retire the
    material. True when this call removed it."""
    with db.get_session(None) as session:
        row = session.execute(select(
            ApiTokenIdentityConsent.material_project_id, ApiTokenIdentityConsent.owner_user_id,
        ).where(ApiTokenIdentityConsent.token_id == token_id,
                ApiTokenIdentityConsent.material_ref == uuid.UUID(str(material_ref)))).one_or_none()
        if row is None:
            return False
        moved = session.execute(delete(ApiTokenIdentityConsent).where(
            ApiTokenIdentityConsent.token_id == token_id,
            ApiTokenIdentityConsent.material_ref == uuid.UUID(str(material_ref)),
        ).execution_options(synchronize_session=False)).rowcount
        if moved:
            _tracing().security_audit_record(session, _event(
                "withdrawal", reason_code, token_id, row.material_project_id, caller_ref, None, material_ref))
        session.commit()
    _retire(row.material_project_id, material_ref, retire_reason, caller_ref, actor_user_id)
    return bool(moved)


def _governed_or_refuse(module) -> None:
    from .governance import is_governed  # pylint: disable=C0415
    if not is_governed(module):
        raise IdentityRefused("GOVERNANCE_NOT_ENFORCED")


def _owned_token(module, token_id, actor_user_id) -> dict:
    """The token's scope (``{projects, operations, user_id}``) when it is the actor's, scoped and live."""
    _governed_or_refuse(module)
    if not allowed(module):
        raise IdentityRefused("IDENTITY_SOURCE_NOT_ALLOWED")
    try:
        scope = auth.token_scope(token_id)
        epochs = auth.read_identity_epochs("token", token_id)
    except Exception as exc:  # pylint: disable=W0703
        # a token that does not exist reads as an error of the token read: not one of the actor's
        log.info("token identity: the token could not be read (%s)", type(exc).__name__)
        raise IdentityRefused("TOKEN_NOT_FOUND") from None
    if not isinstance(epochs, dict) or epochs.get("user_id") != actor_user_id:
        raise IdentityRefused("TOKEN_NOT_FOUND")   # another user's token reads as none (no enumeration)
    if scope is None:
        raise IdentityRefused("TOKEN_NOT_SCOPED")
    if scope.get("user_id") != actor_user_id:
        raise IdentityRefused("TOKEN_NOT_FOUND")
    if epochs.get("active") is not True:
        raise IdentityRefused("INVALID_STATE")
    return scope


def _personal_project(module, user_id) -> int:
    """Where the owner's material is kept: their personal project, which every user has, whose member they always are,
    and which no token scope or project removal can take away (#7337 review S-1)."""
    rpc = getattr(getattr(module, "context", None), "rpc_manager", None)
    try:
        project_id = rpc.timeout(4).projects_get_personal_project_id(user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the owner's personal project could not be read (%s)", type(exc).__name__)
        raise IdentityRefused("DEPENDENCY_UNAVAILABLE") from None
    if isinstance(project_id, bool) or not isinstance(project_id, int) or project_id <= 0:
        raise IdentityRefused("DEPENDENCY_UNAVAILABLE")
    return project_id


def _linked_subject(actor_user_id) -> tuple:
    try:
        user = auth.get_user(actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the owner could not be read (%s)", type(exc).__name__)
        raise IdentityRefused("DEPENDENCY_UNAVAILABLE") from None
    user = user if isinstance(user, dict) else {}
    linked = (user.get("idp_issuer"), user.get("idp_tid"), user.get("idp_oid"))
    if user.get("id") != actor_user_id or not all(isinstance(value, str) and value for value in linked):
        raise IdentityRefused("OWNER_NOT_LINKED")
    return linked


def consent(module, token_id, actor_user_id) -> dict:  # pylint: disable=R0914
    """The token's owner consents, in their own signed-in session, to the token's runs acting with their Microsoft
    sign-in (the material kept in their personal project): ``{"token_id", "consented_at"}``, or raises
    ``IdentityRefused``. Given again, it replaces the earlier one
    (retired ``CONSENT_REPLACED``); two at once: one wins, the other's material is retired and it reads
    ``CONCURRENT_CHANGE``."""
    scope = _owned_token(module, token_id, actor_user_id)
    linked = _linked_subject(actor_user_id)
    person = _verified_person(module, actor_user_id)
    if (person["issuer"], person["tid"], person["oid"]) != linked:
        raise IdentityRefused("DENIED_SUBJECT_MISMATCH")   # signed in as another identity than the account's link
    holder, material_project = holder_of(token_id), _personal_project(module, actor_user_id)
    earlier = _read(token_id)
    granted = _grant(module, person, holder)
    user_ref = f"idp:{person['tid']}:{person['oid']}"
    try:
        kept = _secrets().keep_offline_material(
            material_project, holder_ref=holder, custodian_user_id=actor_user_id, custodian_subject_ref=user_ref,
            source=granted["source"], refresh_token=SecretStr(granted["refresh_token"]), actor_user_id=actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent could not be kept (%s)", type(exc).__name__)
        kept = None
    granted = None
    if not isinstance(kept, dict) or kept.get("ok") is not True:
        code = kept.get("reason_code") if isinstance(kept, dict) else None
        raise IdentityRefused("AUDIT_UNAVAILABLE" if code == "AUDIT_UNAVAILABLE" else "DEPENDENCY_UNAVAILABLE")
    material_ref, now, caller_ref = uuid.UUID(kept["material_ref"]), _now(), f"user:{actor_user_id}"
    try:
        with db.get_session(None) as session:
            if earlier is None:
                session.add(ApiTokenIdentityConsent(
                    token_id=token_id, owner_user_id=actor_user_id, material_project_id=material_project,
                    material_ref=material_ref, consented_at=now))
                session.flush()   # a consent given at the same moment holds the key: this one fails here
            else:
                moved = session.execute(delete(ApiTokenIdentityConsent).where(
                    ApiTokenIdentityConsent.token_id == token_id,
                    ApiTokenIdentityConsent.material_ref == earlier.material_ref,
                ).execution_options(synchronize_session=False)).rowcount
                if moved != 1:
                    session.rollback()
                    raise IdentityRefused("CONCURRENT_CHANGE")
                session.add(ApiTokenIdentityConsent(
                    token_id=token_id, owner_user_id=actor_user_id, material_project_id=material_project,
                    material_ref=material_ref, consented_at=now))
                session.flush()
            _tracing().security_audit_record(session, _event(
                "approval", "TOKEN_IDENTITY_CONSENTED", token_id, material_project, caller_ref, user_ref, material_ref))
            session.commit()
    except IdentityRefused:
        _retire(material_project, material_ref, "CONSENT_REPLACED", caller_ref, actor_user_id)
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent was not recorded (%s)", type(exc).__name__)
        _retire(material_project, material_ref, "CONSENT_REPLACED", caller_ref, actor_user_id)
        raise IdentityRefused("CONCURRENT_CHANGE" if type(exc).__name__ == "IntegrityError"
                              else "AUDIT_UNAVAILABLE") from None
    if earlier is not None:
        _retire(earlier.material_project_id, earlier.material_ref, "CONSENT_REPLACED", caller_ref, actor_user_id)
    return {"token_id": token_id, "consented_at": now.isoformat()}


def withdraw(module, token_id, actor_user_id) -> dict:
    """The token's owner withdraws its consent; works whatever the switch says (withdrawing is always safe), but only
    while governance is enforced (off and audit only read and write nothing). The next delegated release of the token's
    runs is refused ``DENIED_TOKEN_CONSENT_REQUIRED``."""
    _governed_or_refuse(module)
    try:
        row = _read(token_id)
    except Exception as exc:  # pylint: disable=W0703
        log.info("token identity: no consent could be read (%s)", type(exc).__name__)   # no table: none given
        row = None
    if row is None or row.owner_user_id != actor_user_id:
        raise IdentityRefused("CONSENT_NOT_GIVEN")
    try:
        withdrawn = _clear(token_id, row.material_ref, "TOKEN_IDENTITY_WITHDRAWN", f"user:{actor_user_id}",
                           retire_reason="CONSENT_WITHDRAWN", actor_user_id=actor_user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent was not withdrawn (%s)", type(exc).__name__)
        raise IdentityRefused("AUDIT_UNAVAILABLE") from None
    if not withdrawn:
        raise IdentityRefused("CONCURRENT_CHANGE")
    return {"token_id": token_id}


def retire(token_id, reason_code, caller_ref, retire_reason="HOLDER_REVOKED") -> bool:
    """The token was deleted or expired (``HOLDER_REVOKED``), or its owner is gone (``OWNER_RETIRED``): its consent
    goes with it. Best effort; one read."""
    try:
        row = _read(token_id)
        if row is None:
            return False
        return _clear(token_id, row.material_ref, reason_code, caller_ref, retire_reason=retire_reason)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token identity: the consent of an ended token was not retired (%s)", type(exc).__name__)
        return False


def on_auth_invalidate(module, payload) -> bool:
    """``auth_invalidate`` of a deleted token (auth_core fires ``{"kind": "token", "id", "epoch": None}`` after the
    delete): retire its consent. Nothing is read unless governed with the source allowed (a consent left behind is
    refused and retired at its next release: the token is gone)."""
    from .governance import is_governed  # pylint: disable=C0415
    if not isinstance(payload, dict) or payload.get("kind") != "token" or payload.get("epoch") is not None:
        return False
    token_id = payload.get("id")
    if isinstance(token_id, bool) or not isinstance(token_id, int) or not allowed(module) or not is_governed(module):
        return False
    return retire(token_id, "TOKEN_IDENTITY_RETIRED", "elitea_core:token_deleted")


def status(module, actor_user_id) -> dict:
    """What the owner's pages show: ``{"allowed", "linked", "consents": {token id: consented_at}, "mandates"}``
    (``mandates``: schedules and webhooks may act with the owner's sign-in through a mandate, #7131). Nothing is read
    but under the governed profile, and the consents only with the token source allowed."""
    from .governance import is_governed  # pylint: disable=C0415
    from .mandate_identity import owner_delegated_allowed  # pylint: disable=C0415
    governed = is_governed(module)
    mandates = governed and owner_delegated_allowed(module)
    if not governed or not allowed(module):
        return {"allowed": False, "linked": False, "consents": {}, "mandates": mandates}
    try:
        _linked_subject(actor_user_id)
        linked = True
    except IdentityRefused:
        linked = False
    with db.get_session(None) as session:
        rows = session.execute(select(ApiTokenIdentityConsent.token_id, ApiTokenIdentityConsent.consented_at).where(
            ApiTokenIdentityConsent.owner_user_id == actor_user_id)).all()
    return {"allowed": True, "linked": linked, "mandates": mandates,
            "consents": {str(row.token_id): row.consented_at.isoformat() for row in rows}}


# --- the hourly sweep (#7337 review S-2) --------------------------------------------------------------------------

SWEEP_LIMIT = 200   # consents looked at per run; the next run goes on after the last one seen
SWEEP_ACTOR = "elitea_core:token_consent_sweep"
_SWEEP_CURSOR = [0]
# the auth plugin's verdict reason -> (audit reason, material retire reason)
_ENDED = {"token_missing": ("TOKEN_IDENTITY_RETIRED", "HOLDER_REVOKED"),
          "token_expired": ("TOKEN_IDENTITY_RETIRED", "HOLDER_REVOKED"),
          "user_missing": ("TOKEN_IDENTITY_OWNER_RETIRED", "OWNER_RETIRED"),
          "user_suspended": ("TOKEN_IDENTITY_OWNER_RETIRED", "OWNER_RETIRED")}


def sweep(module, limit=SWEEP_LIMIT) -> dict:
    """Retire the consents whose token is gone (deleted, expired) or whose owner is (deleted, suspended), so no
    refresh token outlives its holder at rest. At most ``limit`` consents per run, in token order from where the last
    run stopped (round robin). Under off and audit only, or without the source, nothing is read. The release-time check
    (``_token_live``) stays the first line; this only cleans up. ``{"seen", "retired"}``."""
    from .governance import is_governed  # pylint: disable=C0415
    if not is_governed(module) or not allowed(module):
        return {"seen": 0, "retired": 0}
    with db.get_session(None) as session:
        rows = session.execute(select(ApiTokenIdentityConsent.token_id, ApiTokenIdentityConsent.owner_user_id).where(
            ApiTokenIdentityConsent.token_id > _SWEEP_CURSOR[0]).order_by(ApiTokenIdentityConsent.token_id)
            .limit(limit)).all()
    _SWEEP_CURSOR[0] = rows[-1].token_id if len(rows) == limit else 0   # wrap around after the last page
    retired = 0
    for row in rows:
        try:
            epochs = auth.read_identity_epochs("token", row.token_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("token identity sweep: a token could not be read (%s)", type(exc).__name__)
            continue
        if not isinstance(epochs, dict):
            continue
        ended = _ENDED.get(epochs.get("reason"))
        if ended is None and epochs.get("active") is True and epochs.get("user_id") not in (None, row.owner_user_id):
            ended = ("TOKEN_IDENTITY_RETIRED", "HOLDER_REVOKED")   # the token is not its owner's any more
        if ended is not None and retire(row.token_id, ended[0], SWEEP_ACTOR, retire_reason=ended[1]):
            retired += 1
    return {"seen": len(rows), "retired": retired}
