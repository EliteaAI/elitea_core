"""Auto reuses the existing trusted usage run identity; it does not mint a second one."""
import importlib.util
from pathlib import Path

source = Path(__file__).resolve().parents[3] / 'utils/run_id.py'
spec = importlib.util.spec_from_file_location('routing_run_identity', source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_resume_and_internal_input_keep_message_generation_identity():
    original = {'kwargs': {'message_id': 12, 'execution_generation': 3}}
    run_id = module.stamp_predict_run_id(original)
    for extra in [{'hitl_resume': True}, {'should_continue': True}, {'user_input': 'internally injected harder task'}]:
        resumed = {'kwargs': {'message_id': 12, 'execution_generation': 3, **extra}}
        assert module.stamp_predict_run_id(resumed) == run_id


def test_new_external_turn_or_explicit_regeneration_is_new_run():
    original = module.derived_run_id({'message_id': 12, 'execution_generation': 3})
    assert module.derived_run_id({'message_id': 13, 'execution_generation': 3}) != original
    assert module.derived_run_id({'message_id': 12, 'execution_generation': 4}) != original


def test_carried_child_and_reconcile_identity_wins_new_dispatch():
    carried = {'kwargs': {'message_id': 99, '_elitea_predict_run_id': 'trusted-parent-run'}}
    assert module.stamp_predict_run_id(carried) == 'trusted-parent-run'
    assert carried['meta']['platform_run_id'] == 'trusted-parent-run'


def test_actual_chat_dispatch_metadata_owns_run_identity_on_resume():
    import ast
    from types import SimpleNamespace
    from unittest.mock import Mock
    path = source.parents[1] / 'rpc/application.py'
    tree = ast.parse(path.read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == 'start_task'
             and n.args and isinstance(n.args[0], ast.Constant)
             and n.args[0].value in {'indexer_agent', 'indexer_predict_agent'}]
    assert len(calls) == 2
    for call in calls:
        observed = []
        for resume in [False, True]:
            start_task = Mock()
            payload = {'execution_generation':'persisted-generation', 'hitl_resume':resume}
            env = {'self':SimpleNamespace(task_node=SimpleNamespace(start_task=start_task)),
                   'parsed':SimpleNamespace(stream_id='thread',message_id=123,project_id=7,user_input='Hi'),
                   'payload':payload,'start_event_content':{},'sio_event':'chat_predict',
                   'chat_project_id':7,'user_id':2,'user_context':{},'non_interactive':False,
                   'add_trace_context_to_meta':lambda x:x, 'serialize':lambda x:x,
                   'user_input_preview':lambda x:x}
            eval(compile(ast.Expression(call),str(path),'eval'),env)
            envelope = start_task.call_args.kwargs
            assert 'message_id' not in envelope['kwargs']
            assert envelope['args'] == ['thread',123]
            observed.append(module.stamp_predict_run_id(envelope))
        assert observed[0] == observed[1]
        assert observed[0] == module.derived_run_id({'message_id':123,'execution_generation':'persisted-generation'})


def test_regenerate_forwards_server_projection_outside_public_payload():
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import Mock
    path=Path(__file__).resolve().parents[3]/'api/v2/regenerate.py'
    tree=ast.parse(path.read_text())
    call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call)
              and isinstance(n.func,ast.Call) and getattr(n.func.func,'id',None)=='getattr'
              and any(k.arg=='routing_projection' for k in n.keywords))
    projection={'task':[{'type':'text','text':'Hi'}],'instructions':''}
    payload={'user_input':'server context. Hi','_routing_projection':projection}
    rpc=Mock()
    namespace={'self':SimpleNamespace(module=SimpleNamespace(context=SimpleNamespace(rpc_manager=SimpleNamespace(call=rpc)))),
               'rpc_func':'applications_predict_sio_llm','parsed':SimpleNamespace(sid='socket',question_id=1),
               'regenerate_payload':payload,'SioEvents':SimpleNamespace(chat_predict=SimpleNamespace(value='chat_predict')),
               'msg_group':SimpleNamespace(author_participant_id=2),'project_id':7}
    eval(compile(ast.Expression(call),str(path),'eval'),namespace)
    sent=rpc.applications_predict_sio_llm.call_args
    assert sent.kwargs['routing_projection']==projection
    assert '_routing_projection' not in sent.args[1]
    assert sent.args[1]['user_input']=='server context. Hi'


def test_routing_instructions_include_legacy_inline_context_but_not_progressive_body():
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    root=Path(__file__).resolve().parents[3]
    utility={}
    exec((root/'utils/project_context_utils.py').read_text(),utility)
    tree=ast.parse((root/'rpc/chat_all.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='generate_payload')
    match=next(n for n in ast.walk(fn) if isinstance(n,ast.Match))
    # Execute each owning branch's real projection/helper statements only.
    chat=next(c for c in match.cases if 'ParticipantTypes.dummy' in ast.unparse(c.pattern))
    start=next(i for i,n in enumerate(chat.body) if isinstance(n,ast.Assign) and "result['_routing_projection']" in ast.unparse(n))
    chat_nodes=chat.body[start:start+3]
    agent=next(c for c in match.cases if 'ParticipantTypes.application' in ast.unparse(c.pattern))
    agent_guard=next(n for n in agent.body if isinstance(n,ast.If) and ast.unparse(n.test)=='not _is_pipeline')
    for surface,nodes in [('chat',chat_nodes),('agent',[agent_guard])]:
        for progressive in (False,True):
            context={'enabled':True,'content':'Always verify durability with fault injection.'}
            if progressive:context.update(activation_description='Project durability guidance',revision='r1')
            result={'instructions':'Authored requirements'}
            ns={'result':result,'_vd':{'instructions':'Authored requirements'},'_is_pipeline':False,
                'predict_payload':SimpleNamespace(project_id=7),'get_project_context':lambda _:context,
                'prepare_project_context_delivery':utility['prepare_project_context_delivery']}
            exec(compile(ast.Module(nodes,[]),str(root/'rpc/chat_all.py'),'exec'),ns)
            projected=result['_routing_projection']['instructions']
            assert ('fault injection' in projected) is not progressive, surface
            assert 'Authored requirements' in projected
            assert ('project_context' in result) is progressive
