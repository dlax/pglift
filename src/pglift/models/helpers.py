import enum
import functools
from typing import Any, Callable, Dict, Iterator, List, Mapping, Type, TypeVar, Union

import click
import pydantic
from pydantic.utils import lenient_issubclass
from typing_extensions import TypedDict

Callback = Callable[..., None]
ModelType = Type[pydantic.BaseModel]
T = TypeVar("T", bound=pydantic.BaseModel)


def parse_params_as(model_type: Type[T], params: Dict[str, Any]) -> T:
    obj: Dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        if "_" in k:
            k, kk = k.split("_", 1)
            obj.setdefault(k, {})[kk] = v
        else:
            obj[k] = v
    return model_type.parse_obj(obj)


def _decorators_from_model(
    model_type: ModelType, *, _prefix: str = ""
) -> Iterator[Callable[[Callback], Callback]]:
    """Yield click.{argument,option} decorators corresponding to fields of
    a pydantic model type.
    """
    for field in model_type.__fields__.values():
        cli_config = field.field_info.extra.get("cli", {})
        if cli_config.get("hide", False):
            continue
        if not _prefix and field.required:
            yield click.argument(field.name, type=field.type_)
        else:
            fname = f"--{_prefix}-{field.name}" if _prefix else f"--{field.name}"
            param_decls = (fname,)
            attrs: Dict[str, Any] = {}
            if lenient_issubclass(field.type_, enum.Enum):
                try:
                    choices = cli_config["choices"]
                except KeyError:
                    choices = [v.name for v in field.type_]
                attrs["type"] = click.Choice(choices)
            elif lenient_issubclass(field.type_, pydantic.BaseModel):
                yield from _decorators_from_model(field.type_, _prefix=field.name)
                continue
            else:
                attrs["metavar"] = field.name.upper()
            if field.field_info.description:
                attrs["help"] = field.field_info.description
            yield click.option(*param_decls, **attrs)


def parameters_from_model(
    model_type: ModelType,
) -> Callable[[Callback], Callback]:
    """Attach click parameters (arguments or options) built from a pydantic
    model to the command.
    """

    def decorator(f: Callback) -> Callback:
        @functools.wraps(f)
        def callback(**kwargs: Any) -> None:
            model = parse_params_as(model_type, kwargs)
            return f(model)

        cb = callback
        for param_decorator in reversed(list(_decorators_from_model(model_type))):
            cb = param_decorator(cb)
        return cb

    return decorator


PYDANTIC2ANSIBLE_TYPES: Mapping[Union[Type[Any], str], str] = {
    bool: "bool",
    int: "int",
    str: "str",
    pydantic.SecretStr: "str",
}


class ArgSpec(TypedDict, total=False):
    required: bool
    type: str
    default: Any
    choices: List[str]
    description: List[str]


def argspec_from_model(model_type: ModelType) -> Dict[str, ArgSpec]:
    """Return the Ansible module argument spec object a pydantic model class."""
    spec = {}
    for field in model_type.__fields__.values():
        ansible_config = field.field_info.extra.get("ansible", {})
        if ansible_config.get("hide", False):
            continue
        try:
            arg_spec: ArgSpec = ansible_config["spec"]
        except KeyError:
            arg_spec = ArgSpec()
            ftype = field.type_
            try:
                arg_spec["type"] = PYDANTIC2ANSIBLE_TYPES[ftype]
            except KeyError:
                if lenient_issubclass(ftype, enum.Enum):
                    try:
                        choices = ansible_config["choices"]
                    except KeyError:
                        choices = [f.name for f in ftype]
                    arg_spec["choices"] = choices
                elif lenient_issubclass(ftype, pydantic.BaseModel):
                    for subname, subspec in argspec_from_model(ftype).items():
                        spec[f"{field.name}_{subname}"] = subspec
                    continue
                else:
                    raise ValueError(f"unhandled field type {ftype}")

            if field.required:
                arg_spec["required"] = True

            if field.default is not None:
                default = field.default
                if lenient_issubclass(ftype, enum.Enum):
                    default = default.name
                arg_spec["default"] = default

            if field.field_info.description:
                arg_spec["description"] = [
                    s.strip() for s in field.field_info.description.split(".")
                ]
        spec[field.name] = arg_spec

    return spec
