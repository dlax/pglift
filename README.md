# What is pglift?

pglift is a solution aiming at deploying production-ready instances of
PostgreSQL, set up, backed up and monitored.

Here, the term "instance" means a plain PostgreSQL instance (aka a cluster)
complemented with satellite services which are essential to ensure proper
operation in a production context. These satellite components typically
concern backup, monitoring, high-availability or migration.

The project aims at supporting several tools for each category. At the moment,
supported tools are:

* Backup: pgBackRest
* Monitoring: Prometheus postgres\_exporter

# Status

The project is under active development; it is not yet ready for production
use. Refer to the [issue tracker](https://gitlab.com/dalibo/pglift/-/issues/)
for more details.

# Getting Started

* walk through the [documentation][] for installation and tutorials
* see also the [development and contributing guide][]

[documentation]: https://pglift.readthedocs.io/
[development and contributing guide]: https://pglift.readthedocs.io/en/latest/dev.html

# Documentation

The pglift documentation can be found at <https://pglift.readthedocs.io>.

# License

The code in this repository is developed and distributed under the GNU General
Public License version 3. See [LICENSE](LICENSE) for details.
