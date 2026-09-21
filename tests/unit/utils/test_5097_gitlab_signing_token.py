"""Issue #5097 - GitLab signing-token webhook authentication.

The legacy GitLab mode compares X-Gitlab-Token to a stored string, so a delivery can be
captured and replayed forever. Signing tokens replace that with an HMAC over
``{webhook-id}.{webhook-timestamp}.{body}`` plus a freshness window, which only helps if
every part of that triple is actually covered by the signature and a stale timestamp is
actually refused. These tests pin exactly that: tampering with the body, the id or the
timestamp must invalidate the signature, and an old timestamp must be rejected even when
the signature itself is genuine.

``derive_gitlab_signing_key`` is asserted separately because it is the one step taken from
GitLab's documentation rather than from an observed delivery.

Run via:
    python tests/run_tests.py unit/utils/test_5097_gitlab_signing_token.py -v
"""

import base64
import hashlib
import hmac
import os
import pathlib
import sys
import time
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope="module")
def pt():
    """Load utils/pipeline_trigger.py with its plugin-relative imports satisfied."""
    for pkg_name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.utils"):
        if pkg_name not in sys.modules:
            pkg = types.ModuleType(pkg_name)
            pkg.__path__ = []
            sys.modules[pkg_name] = pkg

    # Another suite may have left an empty placeholder under this name, so top up the
    # attribute on whatever object is registered rather than only filling in a fresh one.
    name = "plugins.elitea_core.utils.constants"
    constants = sys.modules.setdefault(name, types.ModuleType(name))
    if not hasattr(constants, "PROMPT_LIB_MODE"):
        constants.PROMPT_LIB_MODE = "prompt_lib"

    # Other suites in this run may have replaced the shared `tools` stub with a narrower
    # one, so top up only what this module imports rather than relying on load order.
    tools = sys.modules.setdefault("tools", types.ModuleType("tools"))
    if not hasattr(tools, "this"):
        tools.this = types.SimpleNamespace(module_name="elitea_core")
    if not hasattr(tools, "VaultClient"):
        tools.VaultClient = object

    load_utils_module(PLUGIN_ROOT / "utils", "exceptions")
    return load_utils_module(PLUGIN_ROOT / "utils", "pipeline_trigger")


@pytest.fixture
def signing():
    """A signing token plus the key GitLab would use to sign with it."""
    key = os.urandom(32)
    return types.SimpleNamespace(
        key=key,
        token="whsec_" + base64.b64encode(key).decode(),
    )


def sign(key: bytes, webhook_id: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(key, f"{webhook_id}.{timestamp}.".encode() + body, hashlib.sha256).digest()
    return f"v1,{base64.b64encode(digest).decode()}"


BODY = b'{"object_kind":"push","ref":"refs/heads/main"}'
WEBHOOK_ID = "msg_2abc"


class TestKeyDerivation:
    """The whsec_ prefix is stripped and the remainder is the base64-encoded HMAC key."""

    def test_key_is_the_base64_decoded_remainder(self, pt, signing):
        assert pt.derive_gitlab_signing_key(signing.token) == signing.key

    def test_token_without_prefix_still_derives(self, pt, signing):
        assert pt.derive_gitlab_signing_key(base64.b64encode(signing.key).decode()) == signing.key

    def test_empty_token_is_rejected(self, pt):
        with pytest.raises(ValueError):
            pt.derive_gitlab_signing_key("")

    def test_non_base64_token_is_rejected(self, pt):
        with pytest.raises(ValueError):
            pt.derive_gitlab_signing_key("whsec_not!valid!base64")

    # A partial paste can still be valid base64, so length is the only thing separating it from a
    # real key. Accepting one stores a key that decodes fine and then fails every signature check.
    def test_decodable_but_truncated_token_is_rejected(self, pt):
        with pytest.raises(ValueError):
            pt.derive_gitlab_signing_key("whsec_AA")
        short = base64.b64encode(os.urandom(pt.GITLAB_SIGNING_KEY_MIN_BYTES - 1)).decode()
        with pytest.raises(ValueError):
            pt.derive_gitlab_signing_key(f"whsec_{short}")

    def test_key_at_the_minimum_length_is_accepted(self, pt):
        key = os.urandom(pt.GITLAB_SIGNING_KEY_MIN_BYTES)
        token = "whsec_" + base64.b64encode(key).decode()
        assert pt.derive_gitlab_signing_key(token) == key


class TestSignatureCoversTheWholeTriple:
    """Every component GitLab signs must be covered, or the replay guard is decorative."""

    def test_genuine_signature_is_accepted(self, pt, signing):
        ts = str(int(time.time()))
        header = sign(signing.key, WEBHOOK_ID, ts, BODY)
        assert pt.verify_gitlab_signature(signing.token, WEBHOOK_ID, ts, header, BODY) is None

    def test_tampered_body_is_rejected(self, pt, signing):
        ts = str(int(time.time()))
        header = sign(signing.key, WEBHOOK_ID, ts, BODY)
        assert pt.verify_gitlab_signature(
            signing.token, WEBHOOK_ID, ts, header, BODY + b" "
        ) is not None

    def test_replayed_timestamp_is_rejected(self, pt, signing):
        """A captured signature must not verify once the timestamp is moved forward."""
        ts = str(int(time.time()))
        header = sign(signing.key, WEBHOOK_ID, ts, BODY)
        assert pt.verify_gitlab_signature(
            signing.token, WEBHOOK_ID, str(int(ts) + 1), header, BODY
        ) is not None

    def test_swapped_webhook_id_is_rejected(self, pt, signing):
        ts = str(int(time.time()))
        header = sign(signing.key, WEBHOOK_ID, ts, BODY)
        assert pt.verify_gitlab_signature(
            signing.token, "msg_other", ts, header, BODY
        ) is not None

    def test_signature_from_a_different_key_is_rejected(self, pt, signing):
        ts = str(int(time.time()))
        header = sign(os.urandom(32), WEBHOOK_ID, ts, BODY)
        assert pt.verify_gitlab_signature(signing.token, WEBHOOK_ID, ts, header, BODY) is not None

    def test_any_listed_signature_may_match_during_rotation(self, pt, signing):
        """GitLab sends space-separated signatures while a token is being rotated."""
        ts = str(int(time.time()))
        header = f"v1,{base64.b64encode(b'x' * 32).decode()} {sign(signing.key, WEBHOOK_ID, ts, BODY)}"
        assert pt.verify_gitlab_signature(signing.token, WEBHOOK_ID, ts, header, BODY) is None

    def test_unknown_signature_version_is_ignored(self, pt, signing):
        """A future scheme must not be accepted just because the bytes happen to match v1."""
        ts = str(int(time.time()))
        header = sign(signing.key, WEBHOOK_ID, ts, BODY).replace("v1,", "v2,")
        assert pt.verify_gitlab_signature(signing.token, WEBHOOK_ID, ts, header, BODY) is not None

    @pytest.mark.parametrize("webhook_id,header", [
        ("", "v1,abc"),
        (WEBHOOK_ID, ""),
    ])
    def test_missing_headers_are_rejected(self, pt, signing, webhook_id, header):
        assert pt.verify_gitlab_signature(
            signing.token, webhook_id, "1758100000", header, BODY
        ) is not None


class TestTimestampWindow:
    """Freshness is what bounds how long a captured delivery stays useful."""

    def test_current_timestamp_is_accepted(self, pt):
        assert pt.validate_gitlab_timestamp("1758100000", now=1758100000) is None

    @pytest.mark.parametrize("offset", [299, -299])
    def test_skew_inside_the_window_is_accepted(self, pt, offset):
        assert pt.validate_gitlab_timestamp("1758100000", now=1758100000 + offset) is None

    @pytest.mark.parametrize("offset", [301, -301])
    def test_skew_outside_the_window_is_rejected(self, pt, offset):
        assert pt.validate_gitlab_timestamp("1758100000", now=1758100000 + offset) is not None

    @pytest.mark.parametrize("value", ["", None, "not-a-number", "17581e5"])
    def test_unusable_timestamps_are_rejected(self, pt, value):
        assert pt.validate_gitlab_timestamp(value, now=1758100000) is not None


class TestValidationDispatch:
    """validate_webhook_secret must route on the stored auth method, not on the request."""

    def test_signing_mode_accepts_a_fresh_signed_delivery(self, pt, signing):
        ts = str(int(time.time()))
        ok, error = pt.validate_webhook_secret(
            webhook_type="gitlab",
            secret_value=signing.token,
            signature=sign(signing.key, WEBHOOK_ID, ts, BODY),
            raw_data=BODY,
            auth_method=pt.GITLAB_AUTH_SIGNING_TOKEN,
            webhook_id=WEBHOOK_ID,
            timestamp=ts,
        )
        assert ok, error

    def test_signing_mode_rejects_a_stale_delivery(self, pt, signing):
        """The signature is genuine here; only the timestamp is old."""
        ts = "1000000000"
        ok, error = pt.validate_webhook_secret(
            webhook_type="gitlab",
            secret_value=signing.token,
            signature=sign(signing.key, WEBHOOK_ID, ts, BODY),
            raw_data=BODY,
            auth_method=pt.GITLAB_AUTH_SIGNING_TOKEN,
            webhook_id=WEBHOOK_ID,
            timestamp=ts,
        )
        assert not ok
        assert "time window" in error

    def test_legacy_gitlab_trigger_still_uses_token_match(self, pt):
        """Triggers stored before #5097 have no gitlab_auth_method and must keep working."""
        assert pt.validate_webhook_secret("gitlab", "a-stored-token", "a-stored-token") == (True, None)

    def test_secret_token_mode_rejects_a_wrong_token(self, pt):
        ok, error = pt.validate_webhook_secret(
            "gitlab", "a-stored-token", "wrong", auth_method=pt.GITLAB_AUTH_SECRET_TOKEN
        )
        assert not ok
        assert "x-gitlab-token" in error

    def test_absent_token_does_not_pass_as_a_match(self, pt):
        ok, _ = pt.validate_webhook_secret("custom", "a-stored-token", None)
        assert not ok

    def test_signing_headers_are_only_consulted_in_signing_mode(self, pt, signing):
        """A signature-shaped header must not satisfy a trigger configured for secret tokens."""
        ts = str(int(time.time()))
        ok, _ = pt.validate_webhook_secret(
            webhook_type="gitlab",
            secret_value=signing.token,
            signature=sign(signing.key, WEBHOOK_ID, ts, BODY),
            raw_data=BODY,
            auth_method=pt.GITLAB_AUTH_SECRET_TOKEN,
            webhook_id=WEBHOOK_ID,
            timestamp=ts,
        )
        assert not ok


class TestHeaderSelection:
    def test_secret_token_mode_reads_x_gitlab_token(self, pt):
        assert pt.get_webhook_signature_header("gitlab") == "x-gitlab-token"
        assert pt.get_webhook_signature_header(
            "gitlab", pt.GITLAB_AUTH_SECRET_TOKEN) == "x-gitlab-token"

    def test_signing_mode_reads_webhook_signature(self, pt):
        assert pt.get_webhook_signature_header(
            "gitlab", pt.GITLAB_AUTH_SIGNING_TOKEN) == "webhook-signature"

    def test_auth_method_does_not_leak_into_other_webhook_types(self, pt):
        assert pt.get_webhook_signature_header(
            "github", pt.GITLAB_AUTH_SIGNING_TOKEN) == "x-hub-signature-256"


class TestSecretAcceptance:
    def test_signing_token_must_carry_the_gitlab_prefix(self, pt, signing):
        assert pt.validate_gitlab_signing_token_format(signing.token) is None
        assert pt.validate_gitlab_signing_token_format(
            base64.b64encode(signing.key).decode()) is not None

    def test_truncated_signing_token_is_refused_on_save(self, pt):
        assert pt.validate_gitlab_signing_token_format("whsec_AA") is not None

    # Verification must surface a truncated stored token as an error string, not a raised ValueError,
    # or a bad token turns every delivery into a 500 instead of a rejection.
    def test_truncated_stored_token_fails_verification_without_raising(self, pt):
        ts = str(int(time.time()))
        result = pt.verify_gitlab_signature("whsec_AA", WEBHOOK_ID, ts, "v1,AAAA", BODY)
        assert isinstance(result, str)

    def test_short_secret_tokens_are_refused(self, pt):
        assert pt.validate_webhook_secret_strength("short") is not None
        assert pt.validate_webhook_secret_strength("   ") is not None
        assert pt.validate_webhook_secret_strength("x" * 16) is None


class TestTriggerStorage:
    def test_gitlab_triggers_default_to_the_legacy_method(self, pt):
        update = types.SimpleNamespace(
            type="webhook", webhook_type="gitlab", gitlab_auth_method=None)
        assert pt.build_trigger_for_storage(update, 7)["gitlab_auth_method"] == "secret_token"

    def test_signing_method_is_persisted(self, pt):
        update = types.SimpleNamespace(
            type="webhook", webhook_type="gitlab", gitlab_auth_method="signing_token")
        assert pt.build_trigger_for_storage(update, 7)["gitlab_auth_method"] == "signing_token"

    def test_other_webhook_types_carry_no_auth_method(self, pt):
        update = types.SimpleNamespace(type="webhook", webhook_type="github")
        assert "gitlab_auth_method" not in pt.build_trigger_for_storage(update, 7)


class TestStoredSigningTokenIsReported:
    """
    A stored signing token is reused on save, so it has to be visible to the client even when
    secret-token mode is active — otherwise the UI demands a token GitLab only ever shows once.
    """

    def test_signing_ref_is_reported_while_secret_token_mode_is_active(self, pt):
        result = pt.get_webhook_secret_for_display(
            1, {"webhook_signing_secret": "{{secret.webhook_signing_secret_v9}}"},
            "gitlab", auth_method=pt.GITLAB_AUTH_SECRET_TOKEN,
        )
        assert result["signing_secret_configured"] is True
        # The active method has no secret of its own, so nothing is shown for it.
        assert result["secret_configured"] is False
        assert result["secret_value"] is None

    def test_absent_signing_ref_reports_false(self, pt):
        result = pt.get_webhook_secret_for_display(
            1, {}, "gitlab", auth_method=pt.GITLAB_AUTH_SECRET_TOKEN)
        assert result["signing_secret_configured"] is False

    def test_flag_is_present_on_the_populated_path(self, pt, signing, monkeypatch):
        monkeypatch.setattr(pt, "get_webhook_secret_from_vault", lambda *a, **k: signing.token)
        result = pt.get_webhook_secret_for_display(
            1, {"webhook_signing_secret": "{{secret.webhook_signing_secret_v9}}"},
            "gitlab", auth_method=pt.GITLAB_AUTH_SIGNING_TOKEN,
        )
        assert result["signing_secret_configured"] is True
        assert result["secret_configured"] is True
