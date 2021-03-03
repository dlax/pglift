from typing import Any, Sequence, Tuple

from typing_extensions import Protocol

from .types import CommandRunner, CompletedProcess


class _AnsibleModule(Protocol):
    def run_command(
        self, args: Sequence[str], *, check_rc: bool = False, **kwargs: Any
    ) -> Tuple[int, str, str]:
        ...


def ansible_runner(module: _AnsibleModule) -> CommandRunner:
    def run(args: Sequence[str], **kwargs: Any) -> CompletedProcess:
        try:
            kwargs["check_rc"] = kwargs.pop("check")
        except KeyError:
            pass
        returncode, stdout, stderr = module.run_command(args, **kwargs)
        return CompletedProcess(args, returncode, stdout, stderr)

    return run
