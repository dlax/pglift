import enum
import functools
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Tuple,
    Type,
    TypeVar,
    Union,
)

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
) -> Iterator[Tuple[str, Callable[[Callback], Callback]]]:
    """Yield click.{argument,option} decorators corresponding to fields of
    a pydantic model type along with respective callback argument name.
    """
    for field in model_type.__fields__.values():
        cli_config = field.field_info.extra.get("cli", {})
        if cli_config.get("hide", False):
            continue
        if not _prefix and field.required:
            yield field.name, click.argument(field.name, type=field.type_)
        else:
            if _prefix:
                fname = f"--{_prefix}-{field.name}"
                argname = f"{_prefix}_{field.name}"
            else:
                fname = f"--{field.name}"
                argname = field.name
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
            elif lenient_issubclass(field.type_, bool):
                if field.default is False:
                    attrs["is_flag"] = True
                else:
                    fname = f"{fname}/--no-{fname[2:]}"
            else:
                attrs["metavar"] = field.name.upper()
            if field.field_info.description:
                description = field.field_info.description.capitalize()
                if description[-1] not in ".?":
                    description += "."
                attrs["help"] = description
            yield argname, click.option(fname, **attrs)


def parameters_from_model(
    model_type: ModelType,
) -> Callable[[Callback], Callback]:
    """Attach click parameters (arguments or options) built from a pydantic
    model to the command.

    >>> class Obj(pydantic.BaseModel):
    ...     message: str

    >>> @click.command("echo")
    ... @parameters_from_model(Obj)
    ... @click.option("--caps", is_flag=True, default=False)
    ... @click.pass_context
    ... def cmd(ctx, obj, caps):
    ...     output = obj.message
    ...     if caps:
    ...         output = output.upper()
    ...     click.echo(output)

    The argument in callback function must match the base name (lower-case) of
    the pydantic model class. In the example above, this is named "obj".
    Otherwise, a TypeError is raised.

    >>> from click.testing import CliRunner
    >>> runner = CliRunner()
    >>> r = runner.invoke(cmd, ["hello, world"])
    >>> print(r.stdout.strip())
    hello, world
    >>> r = runner.invoke(cmd, ["hello, world", "--caps"])
    >>> print(r.stdout.strip())
    HELLO, WORLD
    """

    def decorator(f: Callback) -> Callback:

        argnames, param_decorators = zip(
            *reversed(list(_decorators_from_model(model_type)))
        )

        s = inspect.signature(f)
        model_argname = model_type.__name__.lower()
        type_error = TypeError(
            f"expecting a '{model_argname}: {model_type.__name__}' parameter in '{f.__name__}{s}'"
        )
        try:
            model_param = s.parameters[model_argname]
        except KeyError:
            raise type_error
        if model_param.annotation not in (model_type, inspect.Signature.empty):
            raise type_error

        @functools.wraps(f)
        def callback(**kwargs: Any) -> None:
            params = {n: kwargs.pop(n) for n in argnames}
            model = parse_params_as(model_type, params)
            kwargs[model_argname] = model
            return f(**kwargs)

        cb = callback
        for param_decorator in param_decorators:
            cb = param_decorator(cb)
        return cb

    return decorator


class ArgSpec(TypedDict, total=False):
    required: bool
    type: str
    default: Any
    choices: List[str]
    description: List[str]
    no_log: bool


PYDANTIC2ANSIBLE: Mapping[Union[Type[Any], str], ArgSpec] = {
    bool: {"type": "bool"},
    int: {"type": "int"},
    str: {"type": "str"},
    pydantic.SecretStr: {"type": "str", "no_log": True},
}


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
                arg_spec.update(PYDANTIC2ANSIBLE[ftype])
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
