import logging
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Tuple

from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    from pglift._compat import Protocol
    from pglift.ctx import BaseContext, SiteMixin
    from pglift.types import CompletedProcess

    if TYPE_CHECKING:
        from pglift.settings import Settings


class _AnsibleModule(Protocol):
    def debug(self, msg: str) -> None:
        ...

    def log(self, msg: str, log_args: Optional[Mapping[str, Any]] = None) -> None:
        ...

    def run_command(
        self, args: Sequence[str], *, check_rc: bool = False, **kwargs: Any
    ) -> Tuple[int, str, str]:
        ...


class AnsibleLoggingHandler(logging.Handler):
    def __init__(self, module: _AnsibleModule, *args: Any, **kwargs: Any) -> None:
        self._ansible_module = module
        super().__init__(*args, **kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if record.levelno == logging.DEBUG:
            self._ansible_module.debug(message)
        else:
            self._ansible_module.log(f"[record.levelname.lower()] {message}")


class AnsibleContext(SiteMixin, BaseContext):
    """Execution context that uses an Ansible module."""

    def __init__(self, module: _AnsibleModule, *, settings: "Settings") -> None:
        self.module = module
        logger = logging.getLogger("pglift")
        logger.addHandler(AnsibleLoggingHandler(module))
        super().__init__(settings=settings)

    def run(
        self,
        args: Sequence[str],
        log_command: bool = True,
        log_output: bool = True,
        **kwargs: Any,
    ) -> CompletedProcess:
        """Run a command through the Ansible module."""
        try:
            kwargs["check_rc"] = kwargs.pop("check")
        except KeyError:
            pass
        kwargs.pop("capture_output", None)  # default on Ansible
        returncode, stdout, stderr = self.module.run_command(args, **kwargs)
        return CompletedProcess(args, returncode, stdout, stderr)