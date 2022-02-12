import enum
import functools
import inspect
import typing
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Mapping,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pydantic
from pydantic.utils import lenient_issubclass

from ..types import AnsibleArgSpec

Callback = Callable[..., Any]
ModelType = Type[pydantic.BaseModel]
T = TypeVar("T", bound=pydantic.BaseModel)

try:
    get_origin = getattr(typing, "get_origin")
except AttributeError:  # Python < 3.8

    def get_origin(tp: Any) -> Any:
        # Works only for GenericAlias, which should be enough for us.
        return getattr(tp, "__origin__", None)


def unnest(model_type: Type[T], params: Dict[str, Any]) -> Dict[str, Any]:
    obj: Dict[str, Any] = {}
    known_fields = {f.alias or f.name for f in model_type.__fields__.values()}
    for k, v in params.items():
        if v is None:
            continue
        if k in known_fields:
            obj[k] = v
        elif "_" in k:
            p, subk = k.split("_", 1)
            if p not in known_fields:
                raise ValueError(k)
            obj.setdefault(p, {})[subk] = v
        else:
            raise ValueError(k)
    return obj


def parse_params_as(model_type: Type[T], params: Dict[str, Any]) -> T:
    obj = unnest(model_type, params)
    return model_type.parse_obj(obj)


DEFAULT = object()


def _decorators_from_model(
    model_type: ModelType, *, exclude: Sequence[str] = (), _prefix: str = ""
) -> Iterator[Tuple[Tuple[str, str], Callable[[Callback], Callback]]]:
    """Yield click.{argument,option} decorators corresponding to fields of
    a pydantic model type along with respective callback argument name and
    model name.
    """
    import click

    def default(ctx: click.Context, param: click.Argument, value: Any) -> Any:
        if (param.multiple and value == ()) or (value == param.default):
            return DEFAULT
        return value

    model_cli_config = getattr(model_type, "_cli_config", {})

    for field in model_type.__fields__.values():
        cli_config = model_cli_config.get(field.name, {})
        if cli_config.get("hide", False):
            continue
        argname = cli_config.get("name", field.alias)
        if argname in exclude:
            continue
        argname = argname.replace("_", "-")
        modelname = field.alias
        ftype = field.outer_type_
        if not _prefix and field.required:
            yield (modelname, argname.replace("-", "_")), click.argument(
                argname, type=ftype
            )
        else:
            metavar = argname.upper()
            if _prefix:
                fname = f"--{_prefix}-{argname}"
                modelname, argname = (
                    f"{_prefix}_{modelname}",
                    f"{_prefix}_{argname.replace('-', '_')}",
                )
            else:
                fname = f"--{argname}"
                argname = argname.replace("-", "_")
            description = None
            if field.field_info.description:
                description = field.field_info.description
                description = description[0].upper() + description[1:]
            attrs: Dict[str, Any] = {}
            origin_type = get_origin(field.outer_type_)
            if lenient_issubclass(ftype, enum.Enum):
                try:
                    choices = cli_config["choices"]
                except KeyError:
                    choices = [v.name for v in ftype]
                attrs["type"] = click.Choice(choices)
            elif lenient_issubclass(ftype, pydantic.BaseModel):
                assert not _prefix, "only one nesting level is supported"
                yield from _decorators_from_model(ftype, _prefix=argname)
                continue
            elif origin_type is not None and issubclass(origin_type, list):
                attrs["multiple"] = True
                attrs["metavar"] = metavar
            elif lenient_issubclass(ftype, pydantic.SecretStr):
                attrs["prompt"] = description if description is not None else True
                attrs["prompt_required"] = False
                attrs["confirmation_prompt"] = True
                attrs["hide_input"] = True
            elif lenient_issubclass(ftype, bool):
                if field.default is False:
                    attrs["is_flag"] = True
                else:
                    fname = f"{fname}/--no-{fname[2:]}"
                # Use None to distinguish unspecified option from the default value.
                attrs["default"] = None
            else:
                attrs["metavar"] = metavar
            if description is not None:
                if description[-1] not in ".?":
                    description += "."
                attrs["help"] = description
            yield (modelname, argname), click.option(fname, callback=default, **attrs)


def parameters_from_model(
    model_type: ModelType, *, exclude: Sequence[str] = (), parse_model: bool = True
) -> Callable[[Callback], Callback]:
    """Attach click parameters (arguments or options) built from a pydantic
    model to the command.

    >>> class Obj(pydantic.BaseModel):
    ...     message: str
    ...     ignored: int = 0

    >>> import click

    >>> @click.command("echo")
    ... @parameters_from_model(Obj, exclude=['ignored'])
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

        modelnames_and_argnames, param_decorators = zip(
            *reversed(list(_decorators_from_model(model_type, exclude=exclude)))
        )

        def params_to_modelargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
            args = {}
            for modelname, argname in modelnames_and_argnames:
                value = kwargs.pop(argname)
                if value is DEFAULT:
                    continue
                args[modelname] = value
            return args

        if parse_model:
            s = inspect.signature(f)
            model_argname = model_type.__name__.lower()
            try:
                model_param = s.parameters[model_argname]
            except KeyError:
                raise TypeError(
                    f"expecting a '{model_argname}: {model_type.__name__}' parameter in '{f.__name__}{s}'"
                )
            if model_param.annotation not in (
                model_type,
                inspect.Signature.empty,
            ) and not issubclass(model_type, model_param.annotation):
                raise TypeError(
                    f"expecting a '{model_argname}: {model_type.__name__}' parameter in '{f.__name__}{s}'; got {model_param.annotation}"
                )

            @functools.wraps(f)
            def callback(**kwargs: Any) -> Any:
                args = params_to_modelargs(kwargs)
                model = parse_params_as(model_type, args)
                kwargs[model_argname] = model
                return f(**kwargs)

        else:

            @functools.wraps(f)
            def callback(**kwargs: Any) -> Any:
                args = params_to_modelargs(kwargs)
                kwargs.update(args)
                return f(**kwargs)

        cb = callback
        for param_decorator in param_decorators:
            cb = param_decorator(cb)
        return cb

    return decorator


PYDANTIC2ANSIBLE: Mapping[Union[Type[Any], str], AnsibleArgSpec] = {
    bool: {"type": "bool"},
    int: {"type": "int"},
    str: {"type": "str"},
    pydantic.SecretStr: {"type": "str", "no_log": True},
}


def argspec_from_model(
    model_type: ModelType,
    force_non_required: bool = False,
) -> Dict[str, AnsibleArgSpec]:
    """Return the Ansible module argument spec object corresponding to a
    pydantic model class.

    When `force_non_required` is True, force all field to be non-required,
    this is useful when sub-models are optionals.
    """
    spec = {}
    model_config = getattr(model_type, "_ansible_config", {})
    for field in model_type.__fields__.values():
        ftype = field.outer_type_
        if lenient_issubclass(ftype, pydantic.BaseModel):
            for subname, subspec in argspec_from_model(
                ftype,
                force_non_required or (not field.required and field.default is None),
            ).items():
                spec[f"{field.alias}_{subname}"] = subspec
            continue

        ansible_config = model_config.get(field.name, {})
        if ansible_config.get("hide", False):
            continue
        try:
            arg_spec: AnsibleArgSpec = ansible_config["spec"]
        except KeyError:
            arg_spec = AnsibleArgSpec()
            try:
                arg_spec.update(PYDANTIC2ANSIBLE[ftype])
            except KeyError:
                origin_type = get_origin(ftype)
                if lenient_issubclass(ftype, enum.Enum):
                    try:
                        choices = ansible_config["choices"]
                    except KeyError:
                        choices = [f.name for f in ftype]
                    arg_spec["choices"] = choices
                elif origin_type is not None and issubclass(origin_type, list):
                    arg_spec["type"] = "list"

            if field.required and not force_non_required:
                arg_spec["required"] = True

            if not force_non_required and field.default is not None:
                default = field.default
                if lenient_issubclass(ftype, enum.Enum):
                    default = default.name
                arg_spec["default"] = default

            if field.field_info.description:
                arg_spec["description"] = [
                    s.strip() for s in field.field_info.description.split(".")
                ]
        spec[field.alias] = arg_spec

    return spec
