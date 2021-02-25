from typing import List, Optional


def command(*args: str, user: Optional[str] = None) -> List[str]:
    """Build a command list, possibly with 'sudo -u <user>'.

    >>> command("ls", "-a")
    ['ls', '-a']
    >>> command("psql", "-l", user="postgres")
    ['sudo', '-u', 'postgres', 'psql', '-l']
    """
    cmd = []
    if user is not None:
        cmd = ["sudo", "-u", user]
    return cmd + list(args)
