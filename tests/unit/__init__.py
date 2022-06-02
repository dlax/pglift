import traceback

from click.testing import Result


def click_result_traceback(result: Result) -> str:
    assert result.exc_info
    exc_type, exc, tb = result.exc_info
    return "".join(traceback.format_exception(exc_type, exc, tb))
