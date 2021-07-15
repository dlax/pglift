import enum
import json
from typing import Optional

import click
import pytest
from click.testing import CliRunner
from pydantic import BaseModel, Field

from pglift.models import helpers, interface


class Gender(enum.Enum):
    M = "M"
    F = "F"


class Country(enum.Enum):
    France = "fr"
    Belgium = "be"
    UnitedKindom = "gb"


class Address(BaseModel):
    street: str = Field(description="the street")
    zipcode: int = Field(
        default=0,
        description="ZIP code",
        cli={"hide": True},
        ansible={"hide": True},
    )
    city: str = Field(
        description="city",
        ansible={"spec": {"type": "str", "description": "the city"}},
    )
    country: Country = Field(
        cli={"choices": [Country.France.value, Country.Belgium.value]},
        ansible={"choices": [Country.France.value, Country.UnitedKindom.value]},
    )
    shared: bool = Field(description="is this a collocation?")
    primary: bool = Field(
        default=False, description="is this person's primary address?"
    )

    class Config:
        extra = "forbid"


class Person(BaseModel):
    name: str
    gender: Optional[Gender]
    age: Optional[int] = Field(description="age")
    address: Optional[Address]

    class Config:
        extra = "forbid"


def test_parameters_from_model_typeerror():
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


def test_parameters_from_model():
    @click.command("add-person")
    @click.option("--sort-keys", is_flag=True, default=False)
    @helpers.parameters_from_model(Person)
    @click.option("--indent", type=int)
    @click.pass_context
    def add_person(
        ctx: click.core.Context, sort_keys: bool, person: Person, indent: int
    ) -> None:
        """Add a new person."""
        click.echo(person.json(indent=indent, sort_keys=sort_keys))

    runner = CliRunner()
    result = runner.invoke(add_person, ["--help"])
    assert result.exit_code == 0
    assert result.stdout == (
        "Usage: add-person [OPTIONS] NAME\n"
        "\n"
        "  Add a new person.\n"
        "\n"
        "Options:\n"
        "  --sort-keys\n"
        "  --gender [M|F]\n"
        "  --age AGE                       Age.\n"
        "  --address-street STREET         The street.\n"
        "  --address-city CITY             City.\n"
        "  --address-country [fr|be]\n"
        "  --address-shared / --no-address-shared\n"
        "                                  Is this a collocation?\n"
        "  --address-primary               Is this person's primary address?\n"
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
            "--address-city=paris",
            "--address-country=fr",
            "--address-primary",
            "--no-address-shared",
            "--indent=2",
        ],
    )
    assert result.exit_code == 0, result
    assert json.loads(result.stdout) == {
        "address": {
            "city": "paris",
            "country": "fr",
            "street": "bd montparnasse",
            "zipcode": 0,
            "primary": True,
            "shared": False,
        },
        "age": 42,
        "gender": "F",
        "name": "alice",
    }


def test_parse_params_as():
    params = {
        "name": "alice",
        "age": 42,
        "gender": "F",
        "address": {
            "city": "paris",
            "country": "fr",
            "street": "bd montparnasse",
            "zipcode": 0,
            "shared": True,
        },
    }
    assert helpers.parse_params_as(Person, params) == Person(
        name="alice",
        age=42,
        gender=Gender.F,
        address=Address(
            street="bd montparnasse",
            zipcode=0,
            city="paris",
            country=Country.France,
            shared=True,
        ),
    )


def test_argspec_from_model():
    argspec = helpers.argspec_from_model(Person)
    assert argspec == {
        "name": {"required": True, "type": "str"},
        "gender": {"choices": ["M", "F"]},
        "age": {"type": "int", "description": ["age"]},
        "address_street": {
            "required": True,
            "type": "str",
            "description": ["the street"],
        },
        "address_city": {"type": "str", "description": "the city"},
        "address_country": {"choices": ["fr", "gb"], "required": True},
        "address_shared": {
            "type": "bool",
            "required": True,
            "description": ["is this a collocation?"],
        },
        "address_primary": {
            "type": "bool",
            "default": False,
            "description": ["is this person's primary address?"],
        },
    }


@pytest.mark.parametrize("manifest_type", [interface.Instance, interface.Role])
def test_argspec_from_model_manifest(datadir, regen_test_data, manifest_type):
    actual = helpers.argspec_from_model(manifest_type)
    fpath = datadir / f"ansible-argspec-{manifest_type.__name__.lower()}.json"
    if regen_test_data:
        fpath.write_text(json.dumps(actual, indent=2, sort_keys=True))
    expected = json.loads(fpath.read_text())
    assert actual == expected
