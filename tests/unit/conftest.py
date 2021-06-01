import pytest

from pglift import pm
from pglift.ctx import Context
from pglift.settings import Settings


def pytest_addoption(parser, pluginmanager):
    parser.addoption(
        "--regen-test-data",
        action="store_true",
        default=False,
        help="Re-generate test data from actual results",
    )


@pytest.fixture
def regen_test_data(request):
    return request.config.getoption("--regen-test-data")


@pytest.fixture
def settings(tmp_path):
    return Settings.parse_obj({"prefix": str(tmp_path)})


@pytest.fixture
def ctx(settings):
    p = pm.PluginManager.get()
    return Context(plugin_manager=p, settings=settings)
