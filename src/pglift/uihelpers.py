import enum
import functools
from typing import Any, Callable, Dict, Iterator, Type

import click
import pydantic
from pydantic.utils import lenient_issubclass

Callback = Callable[..., None]


def _decorators_from_model(
    model_type: Type[pydantic.BaseModel], *, _prefix: str = ""
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
                    choices = [v.name for v in field.type_.__members__.values()]
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
    model_type: Type[pydantic.BaseModel],
) -> Callable[[Callback], Callback]:
    """Attach click parameters (arguments or options) built from a pydantic
    model to the command.
    """

    def decorator(f: Callback) -> Callback:
        @functools.wraps(f)
        def callback(**kwargs: Any) -> None:
            obj = {}  # type: ignore[var-annotated]
            for k, v in kwargs.items():
                if v is None:
                    continue
                if "_" in k:
                    k, kk = k.split("_", 1)
                    obj.setdefault(k, {})[kk] = v
                else:
                    obj[k] = v
            model = model_type.parse_obj(obj)
            return f(model)

        cb = callback
        for param_decorator in reversed(list(_decorators_from_model(model_type))):
            cb = param_decorator(cb)
        return cb

    return decorator
