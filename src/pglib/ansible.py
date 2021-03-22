from typing import Any, Sequence, Tuple

from typing_extensions import Protocol

from .ctx import BaseContext
from .types import CompletedProcess


class _AnsibleModule(Protocol):
    def run_command(
        self, args: Sequence[str], *, check_rc: bool = False, **kwargs: Any
    ) -> Tuple[int, str, str]:
        ...


class AnsibleContext(BaseContext):
    """Execution context that uses an Ansible module."""

    def __init__(self, module: _AnsibleModule, **kwargs: Any) -> None:
        self.module = module
        super().__init__(**kwargs)

    def run(self, args: Sequence[str], **kwargs: Any) -> CompletedProcess:
        """Run a command through the Ansible module."""
        try:
            kwargs["check_rc"] = kwargs.pop("check")
        except KeyError:
            pass
        returncode, stdout, stderr = self.module.run_command(args, **kwargs)
        return CompletedProcess(args, returncode, stdout, stderr)
