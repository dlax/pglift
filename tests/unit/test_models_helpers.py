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

    class Config:
        extra = "forbid"


class Person(BaseModel):
    name: str
    gender: Optional[Gender]
    age: Optional[int] = Field(description="age")
    address: Optional[Address]

    class Config:
        extra = "forbid"


def test_parameters_from_model():
    @click.command("add-person")
    @helpers.parameters_from_model(Person)
    @click.pass_context
    def add_person(ctx: click.core.Context, person: Person) -> None:
        """Add a new person."""
        click.echo(person.json(indent=2, sort_keys=True))

    runner = CliRunner()
    result = runner.invoke(add_person, ["--help"])
    assert result.exit_code == 0
    assert result.stdout == (
        "Usage: add-person [OPTIONS] NAME\n"
        "\n"
        "  Add a new person.\n"
        "\n"
        "Options:\n"
        "  --gender [M|F]\n"
        "  --age AGE                  age\n"
        "  --address-street STREET    the street\n"
        "  --address-city CITY        city\n"
        "  --address-country [fr|be]\n"
        "  --help                     Show this message and exit.\n"
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
        ],
    )
    assert result.exit_code == 0, result
    assert json.loads(result.stdout) == {
        "address": {
            "city": "paris",
            "country": "fr",
            "street": "bd montparnasse",
            "zipcode": 0,
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
        ),
    )


def test_argspec_from_model():
    argspec = helpers.argspec_from_model(Person)
    assert argspec == {
        "name": {"required": True, "type": "str"},
        "gender": {"choices": ["M", "F"]},
        "age": {"type": "int"},
        "address_street": {"required": True, "type": "str"},
        "address_city": {"type": "str", "description": "the city"},
        "address_country": {"choices": ["fr", "gb"], "required": True},
    }


@pytest.mark.parametrize("manifest_type", [interface.Instance, interface.Role])
def test_argspec_from_model_manifest(datadir, regen_test_data, manifest_type):
    actual = helpers.argspec_from_model(manifest_type)
    fpath = datadir / f"ansible-argspec-{manifest_type.__name__.lower()}.json"
    if regen_test_data:
        fpath.write_text(json.dumps(actual, indent=2, sort_keys=True))
    expected = json.loads(fpath.read_text())
    assert actual == expected
