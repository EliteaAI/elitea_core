"""Issue #7050: /predict_llm routes Auto on the skill's own instructions, like a skill Run."""
import ast
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from fixtures.helpers import load_module_with_stubs


@pytest.fixture
def post(models_path):
    predict_llm = load_module_with_stubs(models_path / 'pd' / 'predict_llm.py', 'test_7050_predict_llm')
    path = models_path.parent / 'api' / 'v2' / 'predict_llm.py'
    api_class = next(n for n in ast.parse(path.read_text()).body
                     if isinstance(n, ast.ClassDef) and n.name == 'PromptLibAPI')
    method = next(n for n in api_class.body if isinstance(n, ast.FunctionDef) and n.name == 'post')
    method.decorator_list = []
    request = NS(json=None)
    namespace = {
        'request': request,
        'LLMPredictRequest': predict_llm.LLMPredictRequest,
        'ValidationError': ValidationError,
        'prepare_project_context_delivery': lambda instructions, context: (f'{context}\n\n{instructions}', None),
        'get_project_context': lambda project_id: 'PROJECT CONTEXT',
        'auth': NS(current_user=lambda: {'id': 1}),
        'PredictPayloadError': type('PredictPayloadError', (Exception,), {}),
        'BudgetDoorClosedError': type('BudgetDoorClosedError', (Exception,), {}),
        'PermissionError': PermissionError,
        'PoolSaturationError': type('PoolSaturationError', (Exception,), {}),
        'log': NS(exception=Mock(), error=Mock()),
    }
    exec(compile(ast.Module([method], []), str(path), 'exec'), namespace)
    predict = Mock(return_value={'task_id': 'task-1'})
    api = NS(module=NS(not_starting_task_event=threading.Event(), predict_sio_llm=predict))

    def call(body):
        request.json = body
        namespace['post'](api, 2)
        return predict.call_args.kwargs

    return call


def test_project_context_stays_out_of_auto_routing(post):
    call = post({'user_input': 'hi', 'instructions': 'Review it.', 'include_project_context': True})

    assert call['data']['instructions'] == 'PROJECT CONTEXT\n\nReview it.'
    assert call['routing_projection'] == {'instructions': 'Review it.', 'task': 'hi'}


def test_requests_without_project_context_keep_routing_on_the_prompt(post):
    assert post({'user_input': 'hi', 'instructions': 'Review it.'})['routing_projection'] is None
