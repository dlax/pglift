from typing import Any, Mapping, Optional, Sequence, Tuple

from typing_extensions import Protocol

from .ctx import BaseContext
from .types import CompletedProcess


class _AnsibleModule(Protocol):
    def debug(self, msg: str) -> None:
        ...

    def log(self, msg: str, log_args: Optional[Mapping[str, Any]] = None) -> None:
        ...

    def run_command(
        self, args: Sequence[str], *, check_rc: bool = False, **kwargs: Any
    ) -> Tuple[int, str, str]:
        ...


class AnsibleContext(BaseContext):
    """Execution context that uses an Ansible module."""

    def __init__(self, module: _AnsibleModule, **kwargs: Any) -> None:
        self.module = module
        super().__init__(**kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return self.module.debug(msg % args)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return self.module.log(f"[info] {msg % args}")

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return self.module.log(f"[warn] {msg % args}")

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return self.module.log(f"[error] {msg % args}")

    exception = error

    def run(
        self, args: Sequence[str], log_command: bool = True, **kwargs: Any
    ) -> CompletedProcess:
        """Run a command through the Ansible module."""
        try:
            kwargs["check_rc"] = kwargs.pop("check")
        except KeyError:
            pass
        kwargs.pop("capture_output", None)  # default on Ansible
        returncode, stdout, stderr = self.module.run_command(args, **kwargs)
        return CompletedProcess(args, returncode, stdout, stderr)
