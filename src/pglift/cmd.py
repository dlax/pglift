import asyncio
import asyncio.subprocess
import os
import signal
import subprocess
import sys
from pathlib import Path
from subprocess import DEVNULL, PIPE
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from . import exceptions
from .types import CompletedProcess, Logger


async def process_stream_with(
    stream: Optional[asyncio.StreamReader], process_fn: Callable[[str], None]
) -> str:
    """Process 'stream' by passing each read and decoded line to 'process_fn'
    and return the complete output.

    >>> class MyStream:
    ...     def __init__(self, content):
    ...         self.content = content.split(b" ")
    ...         self.pos = -1
    ...
    ...     def __aiter__(self):
    ...         return self
    ...
    ...     async def __anext__(self):
    ...         self.pos += 1
    ...         try:
    ...             return self.content[self.pos]
    ...         except IndexError:
    ...             raise StopAsyncIteration

    >>> loop = asyncio.get_event_loop()
    >>> logs = []

    >>> async def main(coro):
    ...     return await coro

    >>> coro = process_stream_with(MyStream(b"a b c"), logs.append)
    >>> loop.run_until_complete(main(coro))
    'abc'
    >>> logs
    ['a', 'b', 'c']

    If 'stream' is None, no processing is done:

    >>> def fail(v):
    ...     raise RuntimeError(f"oops, got {v}")
    >>> coro_with_none = process_stream_with(None, fail)
    >>> loop.run_until_complete(main(coro_with_none))
    ''
    """

    if stream is None:
        return ""

    lines = []
    try:
        async for lineb in stream:
            line = lineb.decode("utf-8")
            process_fn(line)
            lines.append(line)
    except asyncio.CancelledError:
        # In case of cancellation, we still return what's been processed.
        pass
    return "".join(lines)


async def communicate_with(
    child: asyncio.subprocess.Process,
    input: Optional[str],
    process_stdout: Callable[[str], None],
    process_stderr: Callable[[str], None],
    min_poll_delay: float = 0.1,
) -> Tuple[str, str]:
    """Interact with 'child' process:

        1. send data to *stdin* if 'input' is not `None`
        2. read data from *stdout* (resp. *stderr*) line by line and process
           each line with 'process_stdout' (resp. 'process_stderr')
        3. wait for the process to terminate

    Return (out, err) tuple.
    """
    if input:
        assert child.stdin is not None
        child.stdin.write(input.encode("utf-8"))
        try:
            await child.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            # Like in communicate() and _feed_stdin() from
            # asyncio.subprocess.Process, we ignore these errors.
            pass
        child.stdin.close()

    stdout = asyncio.ensure_future(process_stream_with(child.stdout, process_stdout))
    stderr = asyncio.ensure_future(process_stream_with(child.stderr, process_stderr))

    pending = {stdout, stderr}

    while True:
        done, pending = await asyncio.wait(pending, timeout=min_poll_delay)

        if not pending:
            break
        elif child.returncode is not None:
            for task in pending:
                task.cancel()

    await child.wait()

    return stdout.result(), stderr.result()


def run(
    args: Sequence[str],
    *,
    input: Optional[str] = None,
    redirect_output: bool = False,
    check: bool = False,
    logger: Optional[Logger] = None,
    **kwargs: Any,
) -> CompletedProcess:
    """Run a command as a subprocess.

    Standard output and errors of child subprocess are captured by default.

    >>> run(["true"], input="a", capture_output=False)
    CompletedProcess(args=['true'], returncode=0, stdout='', stderr='')

    Files can also be used with ``stdout`` and ``stderr`` arguments:

    >>> import tempfile
    >>> with tempfile.NamedTemporaryFile() as f:
    ...     _ = run(["echo", "ahah"], stdout=f, stderr=None)
    ...     with open(f.name) as f:
    ...         print(f.read(), end="")
    ahah

    >>> r = run(["cat", "doesnotexist"], stdout=PIPE, stderr=PIPE, env={"LANG": "C"})
    >>> print(r.stderr, end="")
    cat: doesnotexist: No such file or directory

    With ``check=True``, :class:`~pglift.exceptions.CommandError` is raised
    in case of non-zero return code:

    >>> run(["cat", "doesnotexist"], check=True)
    Traceback (most recent call last):
        ...
    pglift.exceptions.CommandError: Command '['cat', 'doesnotexist']' returned non-zero exit status 1.
    """
    stdin = DEVNULL if input is None else PIPE

    if not args:
        raise ValueError("empty arguments sequence")

    try:
        capture_output = kwargs.pop("capture_output")
    except KeyError:
        kwargs.setdefault("stdout", subprocess.PIPE)
        kwargs.setdefault("stderr", subprocess.PIPE)
    else:
        if capture_output:
            if "stdout" in kwargs or "stderr" in kwargs:
                raise ValueError(
                    "stdout and stderr arguments may not be used with capture_output"
                )
            kwargs["stdout"] = kwargs["stderr"] = subprocess.PIPE

    prog = args[0]

    def process_stdout(out: str, prog: str = prog) -> None:
        if logger:
            logger.debug("%s: %s", prog, out.rstrip())
        if redirect_output:
            sys.stdout.write(out)

    def process_stderr(err: str, prog: str = prog) -> None:
        if logger:
            logger.debug("%s: %s", prog, err.rstrip())
        if redirect_output:
            sys.stderr.write(err)

    async def run() -> Tuple[asyncio.subprocess.Process, str, str]:
        proc = await asyncio.create_subprocess_exec(*args, stdin=stdin, **kwargs)
        out, err = await communicate_with(proc, input, process_stdout, process_stderr)
        assert proc.returncode is not None
        return proc, out, err

    loop = asyncio.get_event_loop()
    proc, out, err = loop.run_until_complete(run())

    if check and proc.returncode:
        raise exceptions.CommandError(proc.returncode, args, out, err)

    assert proc.returncode is not None
    return CompletedProcess(args, proc.returncode, out, err)


def run_expect(
    *args: Sequence[str],
    codes: Tuple[int, ...] = (0,),
    **kwargs: Any,
) -> CompletedProcess:
    """Check that return code command execution with :func:`run` matches
    expected ``codes``.

    :raises ~exceptions.CommandError: if return code does not match.

    >>> run_expect(["false"], codes=(0, 1))
    CompletedProcess(args=['false'], returncode=1, stdout='', stderr='')
    >>> run_expect(["false"])
    Traceback (most recent call last):
      ...
    pglift.exceptions.CommandError: Command '['false']' returned non-zero exit status 1.
    """
    result = run(*args, **kwargs)
    retcode = result.returncode
    if retcode not in codes:
        raise exceptions.CommandError(
            retcode, result.args, result.stdout, result.stderr
        )
    return result


def execute_program(
    cmd: Sequence[str],
    pidfile: Path,
    *,
    timeout: float = 1,
    env: Optional[Mapping[str, str]] = None,
    capture_output: bool = True,
    logger: Optional[Logger] = None,
) -> None:
    """Execute program described by 'cmd' and store its PID in 'pidfile'.

    This is aimed at starting daemon programs.

    :raises ~exceptions.SystemError: if the program is already running.
    :raises ~exceptions.CommandError: in case program execution terminates
        after `timeout`.
    """
    prog = cmd[0]
    if pidfile.exists():
        pid = pidfile.read_text()
        if (Path("/proc") / pid).exists():
            raise exceptions.SystemError(
                f"program {prog} seems to be running already with PID {pid}"
            )
        else:
            if logger:
                logger.warning(
                    "program %s is supposed to be running with PID %s but "
                    "it's apparently not; starting anyway",
                    prog,
                    pid,
                )
            pidfile.unlink()
    if capture_output:
        stderr = subprocess.PIPE
    else:
        stderr = subprocess.DEVNULL
    stdout = subprocess.DEVNULL
    proc = subprocess.Popen(  # nosec
        cmd, stdout=stdout, stderr=stderr, env=env, universal_newlines=True
    )
    try:
        __, errs = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(proc.pid))
        return None
    else:
        if logger:
            for errline in errs.splitlines():
                logger.error("%s: %s", prog, errline)
        raise exceptions.CommandError(proc.returncode, cmd, stderr=errs)


def terminate_program(pidfile: Path, *, logger: Optional[Logger] = None) -> None:
    """Terminate program matching PID in 'pidfile'.

    Upon successful termination, the 'pidfile' is removed.

    :raises ~exceptions.FileNotFoundError: if pidfile path does not exist.
    """
    try:
        pid = int(pidfile.read_text())
    except FileNotFoundError as e:
        raise exceptions.FileNotFoundError(str(e)) from e
    if logger:
        logger.info("terminating process %d", pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError as e:
        if logger:
            logger.warning("failed to kill process %d: %s", pid, e)
    pidfile.unlink()
