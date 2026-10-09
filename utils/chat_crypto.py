""" Encryption envelope for chat export / import files

Exported chats are wrapped in the shared AES-256-GCM envelope keyed from
SECRETS_MASTER_KEY, so only files produced by this setup (and untouched since)
can be imported. The magic and HKDF info strings are part of the file format
and differ from project backups, so one can never be accepted as the other.
"""

from tools import artifact_crypto  # pylint: disable=E0401

CHAT_ENVELOPE_MAGIC = b"ELITEA-CHAT-ENC/1"
CHAT_ENVELOPE = artifact_crypto.Envelope(
    CHAT_ENVELOPE_MAGIC,
    b"elitea-chat-export/v1/aes-256-gcm",
    b"elitea-chat-export/v1/key-id",
    "chat export",
)
ENVELOPE_SUFFIX = artifact_crypto.ENVELOPE_SUFFIX
ENVELOPE_MIMETYPE = artifact_crypto.ENVELOPE_MIMETYPE
READ_CHUNK_SIZE = artifact_crypto.FRAME_SIZE

EnvelopeKeyMismatch = artifact_crypto.EnvelopeKeyMismatch
EnvelopeIntegrityError = artifact_crypto.EnvelopeIntegrityError


def chat_master_key():
    return artifact_crypto.configured_master_key()


def iter_file_chunks(file_obj, chunk_size=READ_CHUNK_SIZE):
    file_obj.seek(0)
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        yield chunk
