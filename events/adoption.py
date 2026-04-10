"""Event handler for adoption counter increments on published agents."""

from pylon.core.tools import log, web
from tools import db

from ..models.all import Application


class Event:
    @web.event('adoption_counter_increment')
    def handle_adoption_increment(self, context, event, payload: dict):
        """Increment the adoption counter on a public agent's meta.

        Expected payload keys:
          - public_project_id: int
          - agent_id: int (Application.id in the public project)
          - consumer_project_id: int (project where the conversation lives)
        """
        public_project_id = payload.get('public_project_id')
        agent_id = payload.get('agent_id')
        consumer_project_id = payload.get('consumer_project_id')

        if not all((public_project_id, agent_id, consumer_project_id)):
            log.warning("[ADOPTION] Missing fields in payload: %s", payload)
            return

        try:
            with db.get_session(public_project_id) as session:
                application = session.query(Application).get(agent_id)
                if application is None:
                    log.warning("[ADOPTION] Agent %d not found in public project %d", agent_id, public_project_id)
                    return

                adoption = (application.meta or {}).get('adoption', {
                    'conversation_count': 0,
                    'project_count': 0,
                    'project_ids': [],
                })

                adoption['conversation_count'] = adoption.get('conversation_count', 0) + 1

                project_ids = adoption.get('project_ids', [])
                if consumer_project_id not in project_ids:
                    project_ids.append(consumer_project_id)
                    adoption['project_ids'] = project_ids
                    adoption['project_count'] = len(project_ids)

                application.meta = application.meta or {}
                application.meta['adoption'] = adoption
                session.commit()

                log.info(
                    "[ADOPTION] Agent %d: conversations=%d projects=%d",
                    agent_id,
                    adoption['conversation_count'],
                    adoption['project_count'],
                )
        except Exception as e:
            log.error("[ADOPTION] Failed to increment counter for agent %d: %s", agent_id, e)
