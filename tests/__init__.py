import contextlib
import time

from pglift import instance as instance_mod


@contextlib.contextmanager
def instance_running(ctx, instance, timeout=10):
    instance_mod.start(ctx, instance)
    for _ in range(timeout):
        time.sleep(1)
        if instance_mod.status(ctx, instance) == instance_mod.Status.running:
            break
    else:
        raise AssertionError(f"{instance} not started after {timeout}s")
    try:
        yield
    finally:
        instance_mod.stop(ctx, instance)
