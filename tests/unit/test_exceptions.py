import pathlib

from pglift import exceptions


def test_error() -> None:
    err = exceptions.Error("oups")
    assert str(err) == "oups"


def test_notfound() -> None:
    err = exceptions.InstanceNotFound("12/main")
    assert str(err) == "instance '12/main' not found"


def test_configurationerror() -> None:
    err = exceptions.ConfigurationError(
        pathlib.Path("/etc/tool.conf"), "missing 'foo' option"
    )
    assert str(err) == "missing 'foo' option (path: /etc/tool.conf)"
