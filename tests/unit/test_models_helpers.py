import enum
import json
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Type

import click
import pytest
from click.testing import CliRunner
from pydantic import BaseModel, Field, SecretStr

from pglift import pm, prometheus
from pglift.models import helpers, interface
from pglift.types import AnsibleConfig, CLIConfig, Manifest


class Gender(enum.Enum):
    M = "M"
    F = "F"


class Country(enum.Enum):
    France = "fr"
    Belgium = "be"
    UnitedKindom = "gb"


class Address(BaseModel):
    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "building": {"hide": True},
        "country": {"choices": [Country.France.value, Country.Belgium.value]},
        "city": {"name": "town"},
    }
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
        "building": {"hide": True},
        "zip_code": {"hide": True},
        "city": {"spec": {"type": "str", "description": ["the city"]}},
        "country": {"choices": [Country.France.value, Country.UnitedKindom.value]},
    }

    street: List[str] = Field(description="street lines")
    building: Optional[str] = Field(ansible={"hide": True})
    zip_code: int = Field(default=0, description="ZIP code")
    city: str = Field(description="city")
    country: Country = Field()
    shared: bool = Field(description="is this a collocation?")
    primary: bool = Field(
        default=False, description="is this person's primary address?"
    )

    class Config:
        extra = "forbid"


class Person(BaseModel):
    name: str
    nickname: Optional[SecretStr]
    gender: Optional[Gender]
    age: Optional[int] = Field(description="age")
    address: Optional[Address]
    dob: Optional[datetime] = Field(alias="birthdate", description="date of birth")

    class Config:
        extra = "forbid"


def test_parameters_from_model_typeerror() -> None:
    with pytest.raises(TypeError, match="expecting a 'person: Person' parameter"):

        @click.command("add-person")
        @helpers.parameters_from_model(Person)
        @click.pass_context
        def cb1(ctx: click.core.Context, x: Person) -> None:
            pass

    with pytest.raises(TypeError, match="expecting a 'person: Person' parameter"):

        @click.command("add-person")
        @helpers.parameters_from_model(Person)
        @click.pass_context
        def cb2(ctx: click.core.Context, person: str) -> None:
            pass


def test_parameters_from_model() -> None:
    @click.command("add-person")
    @click.option("--sort-keys", is_flag=True, default=False)
    @helpers.parameters_from_model(Person)
    @click.option("--indent", type=int)
    @click.pass_context
    def add_person(
        ctx: click.core.Context, sort_keys: bool, person: Person, indent: int
    ) -> None:
        """Add a new person."""
        click.echo(person.json(indent=indent, sort_keys=sort_keys), err=True)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(add_person, ["--help"])
    assert result.exit_code == 0
    assert result.stdout == (
        "Usage: add-person [OPTIONS] NAME\n"
        "\n"
        "  Add a new person.\n"
        "\n"
        "Options:\n"
        "  --sort-keys\n"
        "  --nickname TEXT\n"
        "  --gender [M|F]\n"
        "  --age AGE                       Age.\n"
        "  --address-street STREET         Street lines.\n"
        "  --address-zip-code ZIP-CODE     ZIP code.\n"
        "  --address-town TOWN             City.\n"
        "  --address-country [fr|be]\n"
        "  --address-shared / --no-address-shared\n"
        "                                  Is this a collocation?\n"
        "  --address-primary               Is this person's primary address?\n"
        "  --birthdate BIRTHDATE           Date of birth.\n"
        "  --indent INTEGER\n"
        "  --help                          Show this message and exit.\n"
    )

    result = runner.invoke(
        add_person,
        [
            "alice",
            "--age=42",
            "--gender=F",
            "--address-street=bd montparnasse",
            "--address-street=far far away",
            "--address-town=paris",
            "--address-country=fr",
            "--address-primary",
            "--birthdate=1981-02-18T01:02",
            "--no-address-shared",
            "--indent=2",
            "--nickname",
        ],
        input="alc\nalc\n",
    )
    assert result.exit_code == 0, result
    assert json.loads(result.stderr) == {
        "address": {
            "building": None,
            "city": "paris",
            "country": "fr",
            "street": ["bd montparnasse", "far far away"],
            "zip_code": 0,
            "primary": True,
            "shared": False,
        },
        "age": 42,
        "dob": "1981-02-18T01:02:00",
        "gender": "F",
        "name": "alice",
        "nickname": "**********",
    }


def test_parameters_from_model_no_parse() -> None:
    @click.command("add-person")
    @helpers.parameters_from_model(Person, parse_model=False)
    @click.pass_context
    def add_person(ctx: click.core.Context, **values: Any) -> None:
        click.echo(json.dumps(values))

    runner = CliRunner()
    result = runner.invoke(
        add_person,
        [
            "alice",
            "--age=42",
            "--gender=F",
            "--address-street=bd montparnasse",
            "--address-town=paris",
            "--address-country=fr",
            "--address-primary",
            "--birthdate=1981-02-18T01:02",
        ],
    )
    assert result.exit_code == 0, result
    assert json.loads(result.stdout) == {
        "address_city": "paris",
        "address_country": "fr",
        "address_street": ["bd montparnasse"],
        "address_primary": True,
        "age": "42",
        "birthdate": "1981-02-18T01:02",
        "gender": "F",
        "name": "alice",
    }


def test_unnest() -> None:
    params = {
        "name": "alice",
        "age": 42,
        "gender": "F",
        "address_city": "paris",
        "address_country": "fr",
        "address_street": ["bd montparnasse"],
        "address_zip_code": 0,
        "address_shared": True,
    }
    assert helpers.unnest(Person, params) == {
        "name": "alice",
        "age": 42,
        "gender": "F",
        "address": {
            "city": "paris",
            "country": "fr",
            "street": ["bd montparnasse"],
            "zip_code": 0,
            "shared": True,
        },
    }

    with pytest.raises(ValueError, match="invalid"):
        helpers.unnest(Person, {"age": None, "invalid": "value"})
    with pytest.raises(ValueError, match="in_va_lid"):
        helpers.unnest(Person, {"age": None, "in_va_lid": "value"})


def test_parse_params_as() -> None:
    address_params = {
        "city": "paris",
        "country": "fr",
        "street": ["bd montparnasse"],
        "zip_code": 0,
        "shared": True,
    }
    address = Address(
        street=["bd montparnasse"],
        zip_code=0,
        city="paris",
        country=Country.France,
        shared=True,
    )
    assert helpers.parse_params_as(Address, address_params) == address

    params = {
        "name": "alice",
        "age": 42,
        "gender": "F",
        "address": address_params,
    }
    person = Person(
        name="alice",
        age=42,
        gender=Gender.F,
        address=address,
    )
    assert helpers.parse_params_as(Person, params) == person

    params_nested = {
        "name": "alice",
        "age": 42,
        "gender": "F",
    }
    params_nested.update({f"address_{k}": v for k, v in address_params.items()})
    assert helpers.parse_params_as(Person, params_nested) == person


def test_argspec_from_model() -> None:
    argspec = helpers.argspec_from_model(Person)
    assert argspec == {
        "name": {"required": True, "type": "str"},
        "nickname": {"no_log": True, "type": "str"},
        "gender": {"choices": ["M", "F"]},
        "age": {"type": "int", "description": ["age"]},
        "birthdate": {"description": ["date of birth"]},
        "address_street": {
            "type": "list",
            "description": ["street lines"],
        },
        "address_city": {"type": "str", "description": ["the city"]},
        "address_country": {"choices": ["fr", "gb"]},
        "address_shared": {
            "type": "bool",
            "description": ["is this a collocation?"],
        },
        "address_primary": {
            "type": "bool",
            "description": ["is this person's primary address?"],
        },
    }


def test_argspec_from_model_nested_optional() -> None:
    """An optional nested model should propagate non-required on all nested models."""

    class Sub(BaseModel):
        f: int

    class Nested(BaseModel):
        s: Sub

    assert helpers.argspec_from_model(Nested) == {
        "s_f": {"required": True, "type": "int"},
    }

    class Model(BaseModel):
        n: Optional[Nested]

    assert helpers.argspec_from_model(Model) == {
        "n_s_f": {"type": "int"},
    }


def test_argspec_from_model_nested_default() -> None:
    """A default value on a optional nested model should not be set as "default" in ansible"""

    class Nested(BaseModel):
        r: int
        d: int = 42

    class Model(BaseModel):
        n: Optional[Nested]

    assert helpers.argspec_from_model(Model) == {
        "n_r": {"type": "int"},
        "n_d": {"type": "int"},
    }


def test_argspec_from_model_keep_default() -> None:
    """A non-required field with a default value should keep the "default" in ansible"""

    class Nested(BaseModel):
        f: int = 42

    class Model(BaseModel):
        n: Nested = Nested()

    assert helpers.argspec_from_model(Model) == {
        "n_f": {"default": 42, "type": "int"},
    }


# Composite instance model will all plugins enabled, for Ansible argspec tests.
composite_instance_model = interface.Instance.composite(pm.PluginManager.get())


@pytest.mark.parametrize(
    "manifest_type",
    [
        composite_instance_model,
        prometheus.PostgresExporter,
        interface.Role,
        interface.Database,
    ],
)
def test_argspec_from_model_manifest(
    datadir: Path, regen_test_data: bool, manifest_type: Type[Manifest]
) -> None:
    actual = helpers.argspec_from_model(manifest_type)
    fpath = datadir / f"ansible-argspec-{manifest_type.__name__.lower()}.json"
    if regen_test_data:
        fpath.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
    expected = json.loads(fpath.read_text())
    assert actual == expected
