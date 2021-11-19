import pytest

from pglift import exceptions
from pglift.models import system


def test_default_postgresql_version(pg_version, ctx, monkeypatch):
    major_version = pg_version[:2]
    assert system.default_postgresql_version(ctx) == major_version

    new_settings = ctx.settings.copy(
        update={
            "postgresql": ctx.settings.postgresql.copy(update={"default_version": "42"})
        }
    )
    with monkeypatch.context() as m:
        m.setattr(ctx, "settings", new_settings)
        assert system.default_postgresql_version(ctx) == "42"


def test_baseinstance_str(pg_version, instance):
    assert str(instance) == f"{pg_version}/test"


def test_baseinstance_qualname(pg_version, instance):
    assert instance.qualname == f"{pg_version}-test"


@pytest.mark.parametrize(
    ["attrname", "expected_suffix"],
    [
        ("path", "srv/pgsql/{version}/test"),
        ("datadir", "srv/pgsql/{version}/test/data"),
        ("waldir", "srv/pgsql/{version}/test/wal"),
    ],
)
def test_baseinstance_paths(pg_version, instance, attrname, expected_suffix):
    path = getattr(instance, attrname)
    assert path.match(expected_suffix.format(version=pg_version))


def test_baseinstance_get(ctx):
    i = system.BaseInstance.get("test", None, ctx=ctx)
    major_version = str(ctx.pg_ctl(None).version)[:2]
    assert i.version == major_version


def test_postgresqlinstance_system_lookup(ctx, instance):
    i = system.PostgreSQLInstance.system_lookup(ctx, instance)
    expected = system.PostgreSQLInstance(
        instance.name, instance.version, instance.settings
    )
    assert i == expected

    i = system.PostgreSQLInstance.system_lookup(ctx, (instance.name, instance.version))
    assert i == expected

    with pytest.raises(TypeError, match="expecting either a BaseInstance or"):
        system.PostgreSQLInstance.system_lookup(ctx, ("nameonly",))  # type: ignore[arg-type]


def test_instance_system_lookup(ctx, instance):
    i = system.Instance.system_lookup(ctx, instance)
    assert i == instance

    i = system.Instance.system_lookup(ctx, (instance.name, instance.version))
    assert i == instance


def test_instance_system_lookup_misconfigured(ctx, instance):
    (instance.datadir / "postgresql.conf").unlink()
    with pytest.raises(exceptions.InstanceNotFound, match=str(instance)):
        system.Instance.system_lookup(ctx, instance)


def test_postgresqlinstance_exists(pg_version, settings):
    instance = system.PostgreSQLInstance(
        name="exists", version=pg_version, settings=settings
    )
    with pytest.raises(exceptions.InstanceNotFound):
        instance.exists()
    instance.datadir.mkdir(parents=True)
    (instance.datadir / "PG_VERSION").write_text(pg_version)
    with pytest.raises(exceptions.InstanceNotFound):
        instance.exists()
    (instance.datadir / "postgresql.conf").touch()
    assert instance.exists()


def test_postgresqlinstance_port(instance):
    assert instance.port == 999


def test_postgresqlinstance_config(instance):
    config = instance.config()
    assert config.port == 999
    assert config.unix_socket_directories == "/socks"

    assert not instance.config(managed_only=True).as_dict()


def test_postgresqlinstance_standby_for(ctx, instance):
    (instance.datadir / "postgresql.auto.conf").write_text(
        "primary_conninfo = host=/tmp port=4242 user=pg\n"
    )
    assert not instance.standby
    standbyfile = "standby.signal" if int(instance.version) >= 12 else "recovery.conf"
    (instance.datadir / standbyfile).touch()
    assert instance.standby
    assert instance.standby.for_ == "host=/tmp port=4242 user=pg"
