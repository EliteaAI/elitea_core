"""Conversation turn extraction (EVAL-H7, §8.3 — verified 2026-08-11 against p_2).

Splits a stored ``Conversation`` into ``(input, output)`` turn pairs for promote-to-dataset
(§17.2, E2E-06) and on-demand eval evidence assembly (§14.4, E2E-11). The pure functions take
already-loaded group/item data (no DB, no ORM) so B3 promote and H5 assembly share exactly one
extraction contract that the E2E tests can assert against.

Verified model
--------------
``Conversation`` -> ``ConversationMessageGroup`` (table ``chat_message_group``) ->
polymorphic ``MessageItem`` (``chat_message_items``), text held by ``TextMessageItem``
(``chat_messages_text.content``).

**Turn discriminator** — ``group.author_participant.entity_name`` (``ParticipantTypes``):
    ``'user'``                                   -> user / **input** turn
    anything else (``application`` / ``prompt`` / ``llm`` / ``toolkit`` / ``dummy``)
                                                 -> agent / **output** turn
Proven on real conversation p_2/3781: a ``user`` group ``sent_to`` an ``application`` (input
``"[Scheduled execution triggered]"``) followed by an ``application`` group (output
``"Here are 3 random cars: ..."``).

**Output-text rule** — concatenate ``TextMessageItem.content`` for the group's text items only
(``item_type in {'text_message', 'text'}`` — ``'text'`` is a legacy identity for the same
subtable), ordered by ``order_index`` ASC. Non-text items (``canvas_message``,
``attachment_message``, ``context_message``) are excluded.

**Tool-call / trace exclusion** — LLM/tool-call/thinking steps live in the *separate*
``chat_message_trace_step`` table (``MessageTraceStep``, pylon_main, keyed by
``message_group_id``), **never** in ``message_items``. So reading only text items excludes them
structurally — no filtering needed.

Bracketed system markers (e.g. ``"[Scheduled execution triggered]"``,
``"[Pipeline execution completed]"``) are genuine text-item content and are returned verbatim;
relabelling/filtering them is a promote-UI concern (B3), not an extraction concern.
"""

from typing import Iterable, List, Optional, Tuple

# mirrors models.enums.all.ParticipantTypes / MessageGroupItemTypes, kept as literals so this
# module stays import-light (unit-testable without the ORM).
USER_ENTITY = 'user'
TEXT_ITEM_TYPES = ('text_message', 'text')

_DEFAULT_SEP = '\n\n'

# Read cap for `extract_conversation_turns`. Two groups make at most one case pair, so this is
# the import path's MAX_CASES (5000) doubled.
MAX_GROUPS = 10_000


def classify_role(entity_name: Optional[str]) -> str:
    """Map a group's author ``entity_name`` to ``'user'`` or ``'agent'`` (§8.3). Anything that is
    not the ``user`` participant is the agent side (application / prompt / llm / toolkit / dummy)."""
    return 'user' if entity_name == USER_ENTITY else 'agent'


def group_text(items: Iterable, separator: str = _DEFAULT_SEP) -> str:
    """Collapse one group's items to its output text (§8.3). Keeps only text items
    (``item_type in TEXT_ITEM_TYPES``), orders them by ``order_index`` ASC, and joins non-empty
    ``content``. Canvas/attachment/context items and (structurally) trace steps are excluded.

    ``items`` is an iterable of objects exposing ``item_type``, ``order_index`` and ``content``.
    """
    texts = [
        it for it in items
        if getattr(it, 'item_type', None) in TEXT_ITEM_TYPES
    ]
    texts.sort(key=lambda it: getattr(it, 'order_index', 0))
    parts = [
        (getattr(it, 'content', None) or '').strip()
        for it in texts
    ]
    return separator.join(p for p in parts if p)


def pair_turns(
    turns: Iterable[Tuple[str, str]],
) -> List[Tuple[str, Optional[str]]]:
    """Pair chronological ``(role, text)`` turns into ``(input, output)`` cases (§17.2).

    ``turns`` is an iterable of ``(role, text)`` in conversation order, where ``role`` is
    ``'user'`` or ``'agent'`` (as returned by :func:`classify_role`). Each ``user`` turn opens a
    case; the contiguous ``agent`` turns that follow (until the next ``user`` turn) are joined as
    its output. A user turn with no following agent turn yields ``(input, None)`` — a provisional
    case with no captured output. Leading agent turns (no preceding user turn) are skipped.
    """
    pairs: List[Tuple[str, Optional[str]]] = []
    pending_input: Optional[str] = None
    agent_buf: List[str] = []

    def _flush():
        if pending_input is not None:
            output = _DEFAULT_SEP.join(a for a in agent_buf if a) or None
            pairs.append((pending_input, output))

    for role, text in turns:
        if role == 'user':
            _flush()
            pending_input = text or ''
            agent_buf = []
        else:  # agent
            if pending_input is None:
                continue  # leading agent turn with no user prompt — skip
            if text:
                agent_buf.append(text)
    _flush()
    return pairs


def extract_conversation_turns(
    project_id: int,
    conversation_id: int,
    session=None,
) -> List[Tuple[str, Optional[str]]]:
    """Thin DB loader (prototype for B3 promote / H5 assembly): read a conversation's groups +
    text items from the **pylon_main** models and return ``(input, output)`` case pairs via the
    pure functions above. Groups are ordered by ``(created_at, id)``.

    Kept import-local so the pure contract above stays ORM-free and unit-testable.
    """
    from sqlalchemy.orm import selectinload
    from tools import db
    from ..models.conversation import Conversation
    from ..models.message_group import ConversationMessageGroup

    owns = session is None
    session = session or db.get_session(project_id)
    try:
        groups = (
            session.query(ConversationMessageGroup)
            .filter(ConversationMessageGroup.conversation_id == conversation_id)
            # Both relations are touched for every group below, and this runs synchronously
            # inside a pylon_main request — lazily they cost 2N round-trips per promote.
            .options(
                selectinload(ConversationMessageGroup.author_participant),
                selectinload(ConversationMessageGroup.message_items),
            )
            .order_by(ConversationMessageGroup.created_at.asc(),
                      ConversationMessageGroup.id.asc())
            # A conversation has no size bound of its own; the cap mirrors the import path's
            # (`evaluation_dataset_import.MAX_CASES`) since each pair becomes one case anyway.
            .limit(MAX_GROUPS)
            .all()
        )
        turns: List[Tuple[str, str]] = []
        for g in groups:
            entity_name = g.author_participant.entity_name if g.author_participant else None
            turns.append((classify_role(entity_name), group_text(g.message_items)))
        return pair_turns(turns)
    finally:
        if owns:
            session.close()
