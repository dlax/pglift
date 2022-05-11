import sys
from contextlib import contextmanager
from typing import Iterator

from ansible.module_utils.basic import missing_required_lib


@contextmanager
def check_required_libs() -> Iterator[None]:
    try:
        yield None
    except ImportError as e:
        print(missing_required_lib(e.name), file=sys.stderr)
        sys.exit(1)
