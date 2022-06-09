import json
from pathlib import Path
from typing import Iterator, Optional

import pytest
import requests

from pglift import instances, systemd
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.settings import TemboardSettings
from pglift.temboard import impl as temboard
from pglift.temboard import models

from . import reconfigure_instance


@pytest.fixture(scope="session", autouse=True)
def temboard_available(temboard_execpath: Optional[Path]) -> None:
    if not temboard_execpath:
        pytest.skip("temboard not available")


def test_configure(
    ctx: Context,
    temboard_settings: TemboardSettings,
    temboard_password: str,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    tmp_port_factory: Iterator[int],
) -> None:
    configpath = Path(str(temboard_settings.configpath).format(name=instance.qualname))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert "user = temboardagent" in lines
    assert f"port = {instance.port}" in lines
    assert f"password = {temboard_password}" in lines

    home_dir = Path(str(temboard_settings.home).format(name=instance.qualname))
    assert home_dir.exists()

    users_path = Path(str(temboard_settings.users_path).format(name=instance.qualname))
    assert users_path.exists()

    assert temboard._ssl_cert_file(instance.qualname, temboard_settings).exists()
    assert temboard._ssl_key_file(instance.qualname, temboard_settings).exists()

    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance_manifest, port=new_port):
        lines = configpath.read_text().splitlines()
        assert f"port = {new_port}" in lines


def request_agent(port: int) -> requests.Response:
    return requests.get(f"https://0.0.0.0:{port}/discover", verify=False)


@pytest.mark.usefixtures("temboard_settings")
def test_start_stop(ctx: Context, instance: system.Instance) -> None:
    service = instance.service(models.Service)
    port = service.port
    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, temboard.systemd_unit(instance.qualname))

    with instances.running(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(ctx, temboard.systemd_unit(instance.qualname))
        try:
            r = request_agent(port)
        except requests.ConnectionError as e:
            raise AssertionError(f"HTTPS connection failed: {e}") from None
        r.raise_for_status()
        assert r.ok
        output = r.text
        output_json = json.loads(output)
        assert output_json["hostname"] == "localhost.localdomain"

    with instances.stopped(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert not systemd.is_active(ctx, temboard.systemd_unit(instance.qualname))
        with pytest.raises(requests.ConnectionError):
            request_agent(port)


def test_standby(
    ctx: Context,
    temboard_password: str,
    temboard_settings: TemboardSettings,
    standby_instance: system.Instance,
) -> None:
    service = standby_instance.service(models.Service)
    port = service.port
    assert service.password and service.password.get_secret_value() == temboard_password
    configpath = Path(
        str(temboard_settings.configpath).format(name=standby_instance.qualname)
    )
    assert configpath.exists()
    with instances.running(ctx, standby_instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(
                ctx, temboard.systemd_unit(standby_instance.qualname)
            )
        assert instances.status(ctx, standby_instance) == instances.Status.running
        try:
            r = request_agent(port)
        except requests.ConnectionError as e:
            raise AssertionError(f"HTTPS connection failed: {e}") from None
        r.raise_for_status()
        assert r.ok
        output = r.text
        output_json = json.loads(output)
        assert output_json["hostname"] == "localhost.localdomain"
