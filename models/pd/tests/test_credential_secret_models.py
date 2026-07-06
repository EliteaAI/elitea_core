"""Unit tests for the CredentialCreateModel / SecretCreateModel validators.

Run standalone (no pylon runtime needed) with an env that has pydantic v2:

    pytest models/pd/tests/test_credential_secret_models.py -v

Both modules under test import only ``pydantic`` + stdlib, so they load in
isolation; we add the parent ``models/pd/`` dir to sys.path and import directly.
"""

import importlib
import pathlib
import sys

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

secret = importlib.import_module("secret")
credential = importlib.import_module("credential")

SecretCreateModel = secret.SecretCreateModel
SECRET_MAX_VALUE_LEN = secret.SECRET_MAX_VALUE_LEN
CredentialCreateModel = credential.CredentialCreateModel
CREDENTIAL_MAX_DATA_LEN = credential.CREDENTIAL_MAX_DATA_LEN


# --------------------------------------------------------------------------- #
# SecretCreateModel
# --------------------------------------------------------------------------- #

def test_secret_valid_defaults_overwrite_false():
    m = SecretCreateModel.model_validate({"key": "gh_token", "value": "abc123"})
    assert m.key == "gh_token"
    assert m.value == "abc123"
    assert m.overwrite is False


def test_secret_overwrite_can_be_set():
    m = SecretCreateModel.model_validate({"key": "k", "value": "v", "overwrite": True})
    assert m.overwrite is True


@pytest.mark.parametrize("bad_key", ["bad key", "has/slash", "a b", "{{x}}", ""])
def test_secret_rejects_invalid_key(bad_key):
    with pytest.raises(ValidationError):
        SecretCreateModel.model_validate({"key": bad_key, "value": "v"})


@pytest.mark.parametrize("good_key", ["a", "A_1", "my.key", "my-key", "Token.v2-1"])
def test_secret_accepts_safe_keys(good_key):
    assert SecretCreateModel.model_validate({"key": good_key, "value": "v"}).key == good_key


def test_secret_rejects_empty_value():
    with pytest.raises(ValidationError):
        SecretCreateModel.model_validate({"key": "k", "value": ""})


def test_secret_rejects_oversized_value():
    with pytest.raises(ValidationError):
        SecretCreateModel.model_validate({"key": "k", "value": "x" * (SECRET_MAX_VALUE_LEN + 1)})


def test_secret_accepts_max_value():
    m = SecretCreateModel.model_validate({"key": "k", "value": "x" * SECRET_MAX_VALUE_LEN})
    assert len(m.value) == SECRET_MAX_VALUE_LEN


# --------------------------------------------------------------------------- #
# CredentialCreateModel
# --------------------------------------------------------------------------- #

def test_credential_elitea_title_defaults_to_label():
    m = CredentialCreateModel.model_validate({"type": "github", "label": "My GH"})
    assert m.elitea_title == "My GH"


def test_credential_explicit_title_is_stripped():
    m = CredentialCreateModel.model_validate(
        {"type": "github", "label": "L", "elitea_title": "  spaced  "}
    )
    assert m.elitea_title == "spaced"


def test_credential_blank_explicit_title_falls_back_to_label():
    m = CredentialCreateModel.model_validate(
        {"type": "github", "label": "Fallback", "elitea_title": "   "}
    )
    assert m.elitea_title == "Fallback"


def test_credential_rejects_bad_title_chars():
    with pytest.raises(ValidationError):
        CredentialCreateModel.model_validate(
            {"type": "github", "label": "L", "elitea_title": "no/slash"}
        )


def test_credential_to_payload_minimal():
    m = CredentialCreateModel.model_validate(
        {"type": "github", "label": "GH", "data": {"token": "{{secret.gh}}"}}
    )
    payload = m.to_payload(42)
    assert payload == {
        "project_id": 42,
        "elitea_title": "GH",
        "label": "GH",
        "type": "github",
        "data": {"token": "{{secret.gh}}"},
    }
    assert "section" not in payload and "source" not in payload


def test_credential_to_payload_includes_optional_fields():
    m = CredentialCreateModel.model_validate(
        {"type": "t", "label": "L", "section": "sec", "source": "system"}
    )
    payload = m.to_payload(7)
    assert payload["section"] == "sec"
    assert payload["source"] == "system"


def test_credential_rejects_oversized_data():
    big = {"blob": "x" * (CREDENTIAL_MAX_DATA_LEN + 1)}
    with pytest.raises(ValidationError):
        CredentialCreateModel.model_validate({"type": "t", "label": "L", "data": big})


def test_credential_requires_type_and_label():
    with pytest.raises(ValidationError):
        CredentialCreateModel.model_validate({"label": "L"})
    with pytest.raises(ValidationError):
        CredentialCreateModel.model_validate({"type": "t"})
