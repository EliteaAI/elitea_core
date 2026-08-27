"""Durable chat segmentation for configured pipeline HITL nodes."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy.orm.attributes import flag_modified

from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem


PIPELINE_HITL_INTERACTION_TYPE = 'pipeline_hitl_node'
PIPELINE_HITL_HISTORY_CONTRACT_VERSION = 1
PIPELINE_HITL_PROMPT_KIND = 'pipeline_hitl_prompt'
PIPELINE_HITL_DECISION_KIND = 'pipeline_hitl_decision'


def get_pipeline_hitl_interrupt(response_metadata):
    """Return the versioned pipeline-node interrupt, otherwise ``None``."""
    metadata = response_metadata if isinstance(response_metadata, dict) else {}
    interrupt = metadata.get('hitl_interrupt')
    if not isinstance(interrupt, dict):
        plural = metadata.get('hitl_interrupts')
        interrupt = (
            plural[0]
            if isinstance(plural, list) and len(plural) == 1
            else metadata
        )
    interaction_type = (
        interrupt.get('interaction_type') or metadata.get('interaction_type')
    )
    raw_version = (
        interrupt.get('history_contract_version')
        or metadata.get('history_contract_version')
        or 0
    )
    try:
        version = int(raw_version)
    except (TypeError, ValueError):
        return None
    interrupt_id = interrupt.get('interrupt_id') or metadata.get('interrupt_id')
    if (
        interaction_type != PIPELINE_HITL_INTERACTION_TYPE
        or version < PIPELINE_HITL_HISTORY_CONTRACT_VERSION
        or not interrupt_id
    ):
        return None
    normalized = dict(interrupt)
    normalized['interaction_type'] = interaction_type
    normalized['history_contract_version'] = version
    normalized['interrupt_id'] = str(interrupt_id)
    return normalized


def get_direct_pipeline_hitl_interrupt(
    message_group, response_metadata, interrupt=None,
):
    """Return an interrupt eligible for durable root-chat segmentation.

    A HITL node reached through an agent or another pipeline belongs to that
    parent execution.  Splitting it into new chat turns would redirect the
    resume away from the parent checkpoint; for a parallel parent it would also
    break the aggregate decision batch.  Only a directly selected pipeline is
    therefore allowed to create prompt/decision/continuation chat segments.
    """
    metadata = response_metadata if isinstance(response_metadata, dict) else {}
    candidate = get_pipeline_hitl_interrupt(
        interrupt if isinstance(interrupt, dict) else metadata
    )
    if not candidate:
        return None

    author = getattr(message_group, 'author_participant', None)
    entity_name = getattr(author, 'entity_name', None)
    entity_name = getattr(entity_name, 'value', entity_name)
    participant_meta = getattr(author, 'meta', None)
    if (
        entity_name != 'application'
        or not isinstance(participant_meta, dict)
        or participant_meta.get('agent_type') != 'pipeline'
    ):
        return None

    raw_plural = metadata.get('hitl_interrupts')
    if isinstance(raw_plural, list) and len(raw_plural) > 1:
        return None

    sources = [candidate, metadata]
    lineage = metadata.get('metadata')
    if isinstance(lineage, dict):
        sources.append(lineage)
    for source in sources:
        if (
            source.get('parent_agent_name')
            or source.get('parent_agent_call_id')
            or source.get('child_thread_id')
            or source.get('parent_agent_path')
            or source.get('resume_strategy') in {
                'aggregate_child',
                'supervised_child',
            }
        ):
            return None
    return candidate


def pipeline_hitl_decision_text(action, value):
    """Return the canonical user-visible text for one pipeline HITL decision."""
    if action == 'edit':
        return str(value or '')
    if action == 'approve':
        return 'Approved'
    if action == 'reject':
        return 'Rejected'
    return str(value or action or '')


def persist_pipeline_hitl_prompt(session, message_group, response_metadata):
    """Persist one rendered prompt exactly once for one interrupt occurrence."""
    interrupt = get_direct_pipeline_hitl_interrupt(
        message_group, response_metadata,
    )
    if not interrupt:
        return None
    interrupt_id = interrupt['interrupt_id']
    for item in message_group.message_items or []:
        meta = item.meta if isinstance(item.meta, dict) else {}
        if (
            meta.get('kind') == PIPELINE_HITL_PROMPT_KIND
            and meta.get('interrupt_id') == interrupt_id
        ):
            return item

    existing_indexes = [
        item.order_index for item in (message_group.message_items or [])
        if isinstance(item.order_index, int)
    ]
    item = TextMessageItem(
        message_group=message_group,
        item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
        content=str(interrupt.get('message') or 'Awaiting human review...'),
        order_index=(max(existing_indexes) + 1) if existing_indexes else 0,
        meta={
            'kind': PIPELINE_HITL_PROMPT_KIND,
            'interrupt_id': interrupt_id,
            'node_name': str(interrupt.get('node_name') or ''),
            'status': 'pending',
            'available_actions': list(interrupt.get('available_actions') or []),
        },
    )
    session.add(item)
    return item


def create_pipeline_hitl_resume_segments(
    session,
    *,
    conversation,
    paused_response,
    author_participant,
    interrupt,
    action,
    value,
    execution_generation,
):
    """Resolve a prompt and create durable user/assistant resume segments."""
    interrupt_id = interrupt['interrupt_id']
    existing_resolution = (paused_response.meta or {}).get(
        'pipeline_hitl_resolution'
    )
    if (
        isinstance(existing_resolution, dict)
        and existing_resolution.get('interrupt_id') == interrupt_id
    ):
        return None, None

    prompt_item = persist_pipeline_hitl_prompt(
        session, paused_response, {'hitl_interrupt': interrupt},
    )
    if prompt_item is None:
        return None, None
    prompt_meta = dict(prompt_item.meta or {})
    prompt_meta.update({
        'status': 'resolved',
        'action': action,
        'selected_route': str((interrupt.get('routes') or {}).get(action) or ''),
        'resolved_at': datetime.now(tz=timezone.utc).isoformat(),
    })
    prompt_item.meta = prompt_meta
    flag_modified(prompt_item, 'meta')

    # Response placeholders are normally created one second after their input.
    # A fast Edit loop can reach the same HITL node again before that wall-clock
    # second elapses, so derive the next boundary from the paused segment too.
    # Microsecond spacing keeps ordering strict without introducing a visible
    # delay or depending on database insertion order.
    wall_time = datetime.now()
    paused_time = paused_response.created_at or wall_time
    if paused_time.tzinfo is not None:
        paused_time = paused_time.astimezone(timezone.utc).replace(tzinfo=None)
    decision_time = max(wall_time, paused_time + timedelta(microseconds=1))
    decision_uuid = uuid4()
    continuation_uuid = uuid4()
    decision_text = pipeline_hitl_decision_text(action, value)
    decision_group = ConversationMessageGroup(
        uuid=decision_uuid,
        conversation=conversation,
        author_participant=author_participant,
        sent_to_id=paused_response.author_participant_id,
        created_at=decision_time,
        meta={
            'pipeline_hitl_decision': {
                'interrupt_id': interrupt_id,
                'node_name': str(interrupt.get('node_name') or ''),
                'action': action,
                'prompt_message_id': str(paused_response.uuid),
            },
        },
    )
    decision_item = TextMessageItem(
        message_group=decision_group,
        item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
        content=decision_text,
        order_index=0,
        meta={
            'kind': PIPELINE_HITL_DECISION_KIND,
            'interrupt_id': interrupt_id,
            'node_name': str(interrupt.get('node_name') or ''),
            'action': action,
        },
    )
    # ``ConversationMessageGroup.reply_to`` is a self-referential relationship
    # without ``remote_side``.  Assigning it on two transient groups reverses
    # the persisted foreign key (the decision points to the continuation).
    # Flush the user decision first and use the explicit FK, as normal chat
    # response creation does in ``chat_all``.
    session.add_all([decision_group, decision_item])
    session.flush()
    continuation_group = ConversationMessageGroup(
        uuid=continuation_uuid,
        conversation=conversation,
        author_participant_id=paused_response.author_participant_id,
        reply_to_id=decision_group.id,
        is_streaming=True,
        created_at=decision_time + timedelta(microseconds=1),
        meta={
            'thread_id': (paused_response.meta or {}).get('thread_id'),
            'execution_generation': execution_generation,
            'pipeline_hitl_parent': {
                'interrupt_id': interrupt_id,
                'prompt_message_id': str(paused_response.uuid),
                'decision_message_id': str(decision_uuid),
            },
        },
    )

    paused_response.meta = {
        **(paused_response.meta or {}),
        'pipeline_hitl_resolution': {
            'interrupt_id': interrupt_id,
            'node_name': str(interrupt.get('node_name') or ''),
            'action': action,
            'decision_message_id': str(decision_uuid),
            'continuation_message_id': str(continuation_uuid),
        },
    }
    paused_response.is_streaming = False
    flag_modified(paused_response, 'meta')
    session.add_all([prompt_item, continuation_group])
    session.flush()
    return decision_group, continuation_group
