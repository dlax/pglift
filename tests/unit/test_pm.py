from pglift import pm


def test_pluginmanager_get() -> None:
    p = pm.PluginManager.get(no_register=["prometheus"])
    assert {name for name, _ in p.list_name_plugin()} == {
        "pglift.backup",
        "pglift.instance",
        "pglift.passfile",
        "pglift.pgbackrest",
    }


def test_pluginmanager_unregister_all() -> None:
    p = pm.PluginManager.get()
    assert p.list_name_plugin()
    p.unregister_all()
    assert not p.list_name_plugin()


def test_eq() -> None:
    p1, p2 = pm.PluginManager.get(), pm.PluginManager.get()
    assert p1 is not p2
    assert p1 == p2

    p2.unregister_all()
    assert p1 != p2

    assert p1 != object()
    assert 42 != p2
