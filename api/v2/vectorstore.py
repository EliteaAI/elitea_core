from typing import Optional

from flask import request
from pydantic import BaseModel, ValidationError, ConfigDict

from tools import VaultClient
from tools import api_tools, auth, config as c, this, register_openapi
from ...rpc.vectorstore import VAULT_PGVECTOR_PASSWORD_KEY, VAULT_PGVECTOR_CONNSTR_KEY


class VectorStoreCreate(BaseModel):
    project_ids: list[int] | None = None
    concurrent_tasks: Optional[int] = 20
    public_pgvector_title: str = 'elitea-pgvector'
    force_recreate: Optional[bool] = False

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "project_ids": [1, 2, 3],
                    "concurrent_tasks": 20,
                    "public_pgvector_title": "elitea-pgvector",
                    "force_recreate": False,
                }
            ]
        }
    )


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create VectorStore Credentials",
        description="Create pgvector credentials for one or more projects and store connection strings in secrets.",
        tags=["elitea_core/runtime"],
        request_body=VectorStoreCreate,
    )
    @auth.decorators.check_api(["runtime.plugins"])
    def post(self, **kwargs):
        try:
            parsed = VectorStoreCreate.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400
        try:
            result: dict = self.module.create_pgvector_credentials(
                project_ids=parsed.project_ids,
                save_connstr_to_secrets=True,
                concurrent_tasks=parsed.concurrent_tasks,
                public_pgvector_title=parsed.public_pgvector_title,
                force_recreate=parsed.force_recreate,
            )
        except AssertionError as e:
            return {'errors': {'_': str(e)}, 'success': False}, 400

        errors = {}
        success = {}
        for k, v in result.items():
            if v.get('status') == 'error':
                errors[k] = v
            else:
                success[k] = v

        return {'errors': errors, 'success': success}, 200 if len(errors) == 0 else 207

    @register_openapi(
        name="Delete VectorStore Credentials",
        description="Remove pgvector password and connection string secrets from specified projects (or all projects if none specified).",
        tags=["elitea_core/runtime"],
        request_body=VectorStoreCreate,
    )
    @auth.decorators.check_api(["runtime.plugins"])
    def delete(self, **kwargs):
        try:
            parsed = VectorStoreCreate.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        if parsed.project_ids is None:
            parsed.project_ids = [
                i['id'] for i in self.module.context.rpc_manager.call.project_list(
                    filter_={'create_success': True}
                )
            ]

        for p in parsed.project_ids:
            vc = VaultClient(p)
            project_secrets: dict = vc.get_secrets()
            project_secrets.pop(VAULT_PGVECTOR_PASSWORD_KEY, None)
            project_secrets.pop(VAULT_PGVECTOR_CONNSTR_KEY, None)
            vc.set_secrets(project_secrets)

        return None, 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        ''
    ])

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
