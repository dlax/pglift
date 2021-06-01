This directory holds functional tests, i.e. tests relying on the existence of
a real PostgreSQL instance on the system as well as on the availability of
various satellite components (backup, monitoring). Running them, when systemd
is available on target system, will modify the *user* systemd configuration,
though only for what pglift is concerned.
