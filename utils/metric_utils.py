from typing import Sequence

from sqlalchemy import text
from sqlalchemy.orm import Query, Session

from pylon.core.tools import log


def sql_explain_query(project_id: int, query: Query, session: Session) -> Sequence:
    """
    Execute EXPLAIN ANALYZE on the given SQLAlchemy query.
    :param project_id: project identifier to determine the schema
    :param session: sqlalchemy session
    :param query: query to analyze
    :return: execution time and query details
    """
    try:
        main_compiled_query = query.statement.compile(
            dialect=session.bind.dialect,
            compile_kwargs={"literal_binds": True}
        )
        main_explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {main_compiled_query}".replace(
            'tenant', f'p_{project_id}'
        )
        return session.execute(text(main_explain_sql)).fetchall()
    except Exception as e:
        log.error(f"Failed to execute EXPLAIN at : {str(e)}")
        return {}
