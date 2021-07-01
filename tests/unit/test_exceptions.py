from pglift import exceptions


def test_notfound():
    err = exceptions.InstanceNotFound("12/main")
    assert err.show() == "instance '12/main' not found"
