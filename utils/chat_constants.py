CONVERSATION_NAME_MAX_LENGTH: int = 50
CANVAS_CONTENT_TTL: int = 120
CANVAS_SHADOW_KEY_OFFSET_TTL: int = 10
SUMMARIZATION_LOCKING_TTL: int = 180
# Live text of a running chat answer, kept so a user who comes back mid-run sees it (EL-7385).
# Refreshed on every chunk, so it only has to outlive the gap between two chunks of one run.
RUN_SNAPSHOT_TTL: int = 6 * 60 * 60
# Per LLM step, in bytes (Lua string.len); past it the snapshot stops growing and is marked truncated
RUN_SNAPSHOT_MAX_CHARS: int = 256 * 1024
# Long enough for every replica to have processed the same event-node frame
RUN_SNAPSHOT_FRAME_TTL: int = 60
