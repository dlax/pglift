import pytest

from pglift import exceptions, model


def test_instance_default_version(ctx):
    i = model.InstanceSpec.default_version("test", ctx=ctx)
    major_version = str(ctx.pg_ctl(None).version)[:2]
    assert i.version == major_version


def test_instance_from_spec(instance):
    spec = model.InstanceSpec(
        instance.name, instance.version, settings=instance.settings
    )
    from_spec = model.Instance.from_spec(spec)
    assert from_spec == instance


def test_instance_from_spec_misconfigured(instance):
    spec = model.InstanceSpec(
        instance.name, instance.version, settings=instance.settings
    )
    (spec.datadir / "postgresql.conf").unlink()
    with pytest.raises(exceptions.InstanceNotFound, match=str(spec)):
        model.Instance.from_spec(spec)


def test_instance_as_spec(instance):
    assert instance.as_spec() == model.InstanceSpec(
        instance.name, instance.version, settings=instance.settings
    )


def test_instance_config(instance):
    datadir = instance.datadir
    (datadir / "postgresql.conf").write_text(
        "\n".join(["bonjour = hello", "port=1234"])
    )

    config = instance.config()
    config.bonjour == "hello"
    config.port == 1234

    user_conf = datadir / "conf.pglift.d" / "user.conf"
    with pytest.raises(FileNotFoundError, match=str(user_conf)):
        instance.config(True)
    user_conf.parent.mkdir(parents=True)
    user_conf.write_text("\n".join(["port=5555"]))
    mconf = instance.config(True)
    assert mconf is not None
    assert mconf.port == 5555
