from pglift import exceptions


def test_error():
    err = exceptions.Error("oups")
    assert str(err) == "oups"


def test_notfound():
    err = exceptions.InstanceNotFound("12/main")
    assert str(err) == "instance '12/main' not found"
