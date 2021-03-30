import contextlib

from pglib import instance as instance_mod


@contextlib.contextmanager
def instance_running(ctx, instance):
    instance_mod.start(ctx, instance)
    try:
        yield
    finally:
        instance_mod.stop(ctx, instance)
