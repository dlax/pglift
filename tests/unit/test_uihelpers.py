import enum
import json
from typing import Optional

import click
from click.testing import CliRunner
from pydantic import BaseModel, Field

from pglift import uihelpers


class Gender(enum.Enum):
    M = "M"
    F = "F"


class Country(enum.Enum):
    France = "fr"
    Belgium = "be"
    UnitedKindom = "gb"


class Address(BaseModel):
    street: str = Field(description="the street")
    zipcode: int = Field(default=0, description="ZIP code", cli={"hide": True})
    city: str = Field(description="city")
    country: Country = Field(
        cli={"choices": [Country.France.value, Country.Belgium.value]}
    )


class Person(BaseModel):
    name: str
    gender: Optional[Gender]
    age: Optional[int] = Field(description="age")
    address: Optional[Address]


def test_parameters_from_model():
    @click.command("add-person")
    @uihelpers.parameters_from_model(Person)
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
