from pglift import pm


def test_pluginmanager_get():
    p = pm.PluginManager.get(no_register=["prometheus"])
    assert {name for name, _ in p.list_name_plugin()} == {
        "pglift.backup",
        "pglift.instance",
        "pglift.pgbackrest",
    }


def test_pluginmanager_unregister_all():
    p = pm.PluginManager.get()
    assert p.list_name_plugin()
    p.unregister_all()
    assert not p.list_name_plugin()
