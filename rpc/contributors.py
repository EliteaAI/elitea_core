from typing import List
from sqlalchemy import func

from pylon.core.tools import web, log
from tools import db

from ..models.all import ApplicationVersion, Collection
from ..models.elitea_tools import EliteATool


class RPC:
    @web.rpc("elitea_core_get_top_contributors", "get_top_contributors")
    def get_top_contributors(
        self, project_id: int, entity_type: str, limit: int = 5
    ) -> List[int]:
        """
        Get top N user IDs by entity count for a specific entity type.

        Args:
            project_id: The project ID to query
            entity_type: Type of entity to count ('application', 'toolkit', 'collection')
            limit: Maximum number of user IDs to return (default 5)

        Returns:
            List of user IDs sorted by entity count (descending)
        """
        with db.with_project_schema_session(project_id) as session:
            if entity_type == 'application':
                # Count applications per author
                query = (
                    session.query(
                        ApplicationVersion.author_id,
                        func.count(func.distinct(ApplicationVersion.application_id)).label('count')
                    )
                    .group_by(ApplicationVersion.author_id)
                    .order_by(func.count(func.distinct(ApplicationVersion.application_id)).desc())
                    .limit(limit)
                )
            elif entity_type == 'toolkit':
                # Count toolkits per author
                query = (
                    session.query(
                        EliteATool.author_id,
                        func.count(EliteATool.id).label('count')
                    )
                    .group_by(EliteATool.author_id)
                    .order_by(func.count(EliteATool.id).desc())
                    .limit(limit)
                )
            elif entity_type == 'collection':
                # Count collections per author
                query = (
                    session.query(
                        Collection.author_id,
                        func.count(Collection.id).label('count')
                    )
                    .group_by(Collection.author_id)
                    .order_by(func.count(Collection.id).desc())
                    .limit(limit)
                )
            else:
                log.warning(f"Unknown entity_type: {entity_type}")
                return []

            result = query.all()
            return [row[0] for row in result if row[0] is not None]
