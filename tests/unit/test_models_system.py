import pytest

from pglift import exceptions
from pglift.models import system


@pytest.fixture
def instance_spec(instance):
    return system.InstanceSpec(
        instance.name,
        instance.version,
        settings=instance.settings,
        prometheus=system.PrometheusService(instance.prometheus.port),
        standby=None,
    )


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


def test_postgresqlinstance_default_version(ctx):
    i = system.PostgreSQLInstance.default_version("test", ctx=ctx)
    major_version = str(ctx.pg_ctl(None).version)[:2]
    assert i.version == major_version


def test_postgresqlinstance_system_lookup(ctx, instance, instance_spec):
    i = system.PostgreSQLInstance.system_lookup(ctx, instance_spec)
    expected = system.PostgreSQLInstance(
        instance.name, instance.version, instance.settings
    )
    assert i == expected

    i = system.PostgreSQLInstance.system_lookup(
        ctx, (instance_spec.name, instance_spec.version)
    )
    assert i == expected

    with pytest.raises(TypeError, match="expecting either a BaseInstance or"):
        system.PostgreSQLInstance.system_lookup(ctx, ("nameonly",))  # type: ignore[arg-type]


def test_instance_system_lookup(ctx, instance, instance_spec):
    i = system.Instance.system_lookup(ctx, instance_spec)
    assert i == instance

    i = system.Instance.system_lookup(ctx, (instance_spec.name, instance_spec.version))
    assert i == instance


def test_instance_system_lookup_misconfigured(ctx, instance, instance_spec):
    (instance_spec.datadir / "postgresql.conf").unlink()
    with pytest.raises(exceptions.InstanceNotFound, match=str(instance_spec)):
        system.Instance.system_lookup(ctx, instance_spec)


def test_instance_as_spec(instance, instance_spec):
    assert instance.as_spec() == instance_spec


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


def test_postgresqlinstance_standby_for(ctx, instance):
    (instance.datadir / "postgresql.auto.conf").write_text(
        "primary_conninfo = host=/tmp port=4242 user=pg\n"
    )
    assert not instance.standby
    standbyfile = "standby.signal" if int(instance.version) >= 12 else "recovery.conf"
    (instance.datadir / standbyfile).touch()
    assert instance.standby
    assert instance.standby.for_ == "host=/tmp port=4242 user=pg"


def test_instancespec_default_version(pg_version, ctx):
    i = system.InstanceSpec.default_version(
        "default", ctx=ctx, prometheus=system.PrometheusService(), standby=None
    )
    assert i.version == pg_version
