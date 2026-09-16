"""A user with no usable PAT falls back to the system token (#5262).

Two call sites resolve the token a run executes under - the MCP injection path in
internal_tools and the predict payload path in predict_utils - and both must make
the same three choices: a real PAT still wins, no usable PAT falls back to the
system token, and an auth_core that cannot provide one degrades instead of
breaking the run.
"""
import importlib
import pathlib
import sys
import types
from datetime import datetime, timedelta

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

PACKAGE = "plugins.elitea_core"

PAST = datetime.now() - timedelta(days=1)
FUTURE = datetime.now() + timedelta(days=1)


class Auth:
    """The auth RPC facade, recording what the token resolution asked for.

    list_tokens never lists the system token - that is auth_core's doing, and it
    is why an existing PAT can still win here.
    """

    def __init__(self, tokens=None, system_token="system-token", failure=None):
        self.tokens = tokens if tokens is not None else []
        self.system_token = system_token
        self.failure = failure
        self.calls = []

    def list_tokens(self, user_id):
        self.calls.append(("list_tokens", user_id))
        return self.tokens

    def encode_token(self, token_id):
        self.calls.append(("encode_token", token_id))
        return f"encoded-{token_id}"

    def ensure_system_token(self, user_id):
        self.calls.append(("ensure_system_token", user_id))
        if self.failure is not None:
            raise self.failure
        return self.system_token

    def called(self, name):
        return [call for call in self.calls if call[0] == name]


class Log:
    """Pylon's log, recording what the module chose to say."""

    def __init__(self):
        self.warnings = []

    def warning(self, message, *args, **kwargs):
        self.warnings.append(message % args if args else message)

    def info(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


@pytest.fixture(scope="module")
def package_graph():
    """The package skeleton both modules' relative imports resolve against.

    Registered here rather than assumed, so this file passes on its own as well
    as inside the suite. Whatever was in sys.modules before is put back.
    """
    packages = {
        "plugins": PLUGIN_ROOT.parent,
        PACKAGE: PLUGIN_ROOT,
        f"{PACKAGE}.utils": PLUGIN_ROOT / "utils",
        f"{PACKAGE}.models": PLUGIN_ROOT / "models",
        f"{PACKAGE}.models.pd": PLUGIN_ROOT / "models" / "pd",
        f"{PACKAGE}.models.enums": PLUGIN_ROOT / "models" / "enums",
    }
    added = []
    widened = {}

    for name, path in packages.items():
        package = sys.modules.get(name)
        #
        if package is None:
            package = types.ModuleType(name)
            package.__path__ = [str(path)]
            sys.modules[name] = package
            added.append(name)
            continue
        #
        # Another test may have registered this package pointing somewhere else.
        # Widen its search path instead of replacing it, so its stubs keep
        # working and the real sibling modules become importable too.
        search_path = list(getattr(package, "__path__", []))
        if str(path) not in search_path:
            widened[name] = (package, getattr(package, "__path__", None))
            package.__path__ = search_path + [str(path)]

    try:
        yield
    finally:
        for name in added:
            sys.modules.pop(name, None)
        for package, original in widened.values():
            if original is None:
                del package.__path__
            else:
                package.__path__ = original


@pytest.fixture(scope="module")
def internal_tools(package_graph):
    """The real internal_tools, with mcp_config stubbed out."""
    import tools

    for attr, value in (
        ("config", types.SimpleNamespace()),
        ("VaultClient", object),
        ("rpc_tools", types.SimpleNamespace(RpcMixin=object)),
        ("this", types.SimpleNamespace()),
        ("auth", types.SimpleNamespace()),
    ):
        if not hasattr(tools, attr):
            setattr(tools, attr, value)
    tools.config.APP_HOST = "http://localhost"

    mcp_config = types.ModuleType("mcp_config")
    mcp_config.is_mcp_exposure_enabled = lambda: True

    names = (f"{PACKAGE}.utils.mcp_config", f"{PACKAGE}.utils.internal_tools")
    replaced = {name: sys.modules.get(name) for name in names}

    try:
        yield load_utils_module(
            PLUGIN_ROOT / "utils",
            "internal_tools",
            extra_stubs={names[0]: mcp_config},
        )
    finally:
        for name, previous in replaced.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


@pytest.fixture(scope="module")
def predict_utils(package_graph, internal_tools):
    """The real predict_utils, with only its import graph stubbed out.

    Its siblings are stubbed rather than loaded: get_user_token touches none of
    them, and the models package pulls in the ORM. Whatever was in sys.modules
    before is put back, so this stays independent of collection order.
    """
    import tools

    if not hasattr(tools, "serialize"):
        tools.serialize = lambda value: value

    modules = {
        "flask": {},
        f"{PACKAGE}.utils.llm_settings": {
            "normalize_runtime_max_tokens": lambda value: value,
        },
        f"{PACKAGE}.utils.next_input_suggestion_utils": {
            "next_input_suggestion_config": lambda project_id: {},
        },
        f"{PACKAGE}.utils.skill_utils": {
            "consume_invoked_skills": lambda message, skills: (message, []),
            "resolve_runtime_skills": lambda details: [],
        },
        f"{PACKAGE}.utils.application_tools": {
            "expand_toolkit_settings": lambda tools_, *args: tools_,
        },
        f"{PACKAGE}.models.elitea_tools": {"EliteATool": object},
        f"{PACKAGE}.models.enums.all": {"AgentTypes": types.SimpleNamespace()},
        f"{PACKAGE}.models.pd.chat": {
            "ApplicationChatRequest": object,
            "LLMChatRequest": object,
        },
        f"{PACKAGE}.models.pd.tool": {"ToolDetails": object},
    }

    installed = []

    for name, attributes in modules.items():
        if name in sys.modules:
            continue
        module = types.ModuleType(name)
        for key, value in attributes.items():
            setattr(module, key, value)
        sys.modules[name] = module
        installed.append(name)

    # Other tests register a stub under this name to break their own import
    # cycles, so a module already in sys.modules is only the real one if it
    # actually carries the function under test.
    name = f"{PACKAGE}.utils.predict_utils"
    existing = sys.modules.get(name)
    #
    if existing is not None and hasattr(existing, "get_user_token"):
        module = existing
    else:
        sys.modules.pop(name, None)
        module = importlib.import_module(name)

    try:
        yield module
    finally:
        for module_name in installed:
            sys.modules.pop(module_name, None)
        if existing is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = existing


@pytest.fixture
def auth():
    return Auth()


@pytest.fixture
def resolve(internal_tools, auth, monkeypatch):
    """internal_tools.resolve_user_token_state, bound to a recording auth."""
    log = Log()
    monkeypatch.setattr(internal_tools, "auth", auth)
    monkeypatch.setattr(internal_tools, "log", log)
    auth.log = log
    return internal_tools.resolve_user_token_state


@pytest.fixture
def get_user_token(predict_utils, auth, monkeypatch):
    """predict_utils.get_user_token, bound to the same recording auth."""
    log = Log()
    monkeypatch.setattr(predict_utils, "auth", auth)
    monkeypatch.setattr(predict_utils, "log", log)
    auth.log = log
    return predict_utils.get_user_token


# --- a real PAT still wins -------------------------------------------------


def test_a_valid_pat_wins(resolve, auth):
    auth.tokens = [{"id": 7, "expires": FUTURE}]

    assert resolve(1) == ("VALID", "encoded-7")
    assert auth.called("ensure_system_token") == []


def test_a_valid_pat_wins_for_predict(get_user_token, auth):
    auth.tokens = [{"id": 7, "expires": FUTURE}]

    assert get_user_token(1) == "encoded-7"
    assert auth.called("ensure_system_token") == []


def test_a_pat_without_an_expiry_wins(resolve, auth):
    """A user-created token with no expiry is still the user's own."""
    auth.tokens = [{"id": 7, "expires": None}]

    assert resolve(1) == ("VALID", "encoded-7")
    assert auth.called("ensure_system_token") == []


def test_the_first_usable_pat_wins(resolve, auth):
    auth.tokens = [
        {"id": 1, "expires": PAST},
        {"id": 2, "expires": FUTURE},
        {"id": 3, "expires": FUTURE},
    ]

    assert resolve(1) == ("VALID", "encoded-2")


# --- no usable PAT falls back ----------------------------------------------


def test_no_pat_falls_back_to_the_system_token(resolve, auth):
    assert resolve(1) == ("VALID", "system-token")
    assert auth.called("ensure_system_token") == [("ensure_system_token", 1)]


def test_no_pat_falls_back_for_predict(get_user_token, auth):
    assert get_user_token(1) == "system-token"
    assert auth.called("ensure_system_token") == [("ensure_system_token", 1)]


def test_an_expired_pat_falls_back(resolve, auth):
    auth.tokens = [{"id": 7, "expires": PAST}]

    assert resolve(1) == ("VALID", "system-token")


def test_all_expired_pats_fall_back(resolve, auth):
    auth.tokens = [{"id": 1, "expires": PAST}, {"id": 2, "expires": PAST}]

    assert resolve(1) == ("VALID", "system-token")
    assert auth.called("encode_token") == []


def test_all_expired_pats_fall_back_for_predict(get_user_token, auth):
    auth.tokens = [{"id": 1, "expires": PAST}, {"id": 2, "expires": PAST}]

    assert get_user_token(1) == "system-token"


def test_the_fallback_is_asked_for_once(resolve, auth):
    resolve(1)

    assert len(auth.called("ensure_system_token")) == 1


# --- an auth_core that cannot provide one degrades -------------------------


@pytest.mark.parametrize("failure", [
    AttributeError("ensure_system_token"),
    RuntimeError("User is suspended: 1"),
    TimeoutError("rpc timeout"),
])
def test_a_failed_fallback_degrades_to_missing(resolve, auth, failure):
    """Missing on an older auth_core, refused for a suspended user - a banner,
    not a traceback."""
    auth.failure = failure

    assert resolve(1) == ("MISSING", None)


@pytest.mark.parametrize("failure", [
    AttributeError("ensure_system_token"),
    RuntimeError("User is suspended: 1"),
    TimeoutError("rpc timeout"),
])
def test_a_failed_fallback_degrades_for_predict(get_user_token, auth, failure):
    auth.failure = failure

    assert get_user_token(1) is None


def test_a_failed_fallback_is_logged(resolve, auth):
    auth.failure = RuntimeError("db is down")

    resolve(1)

    assert auth.log.warnings == ["Could not ensure system token for user 1"]


def test_a_failed_fallback_is_logged_for_predict(get_user_token, auth):
    auth.failure = RuntimeError("db is down")

    get_user_token(1)

    assert auth.log.warnings == ["Could not ensure system token for user 1"]


# --- no user, no lookup ----------------------------------------------------


@pytest.mark.parametrize("user_id", [None, 0])
def test_without_a_user_nothing_is_looked_up(resolve, auth, user_id):
    """Anonymous requests must not provision a token for user None."""
    assert resolve(user_id) == ("MISSING", None)
    assert auth.calls == []


def test_without_a_user_nothing_is_looked_up_for_predict(get_user_token, auth):
    assert get_user_token(None) is None
    assert auth.calls == []
