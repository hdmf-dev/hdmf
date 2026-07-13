..  _software_process:

================
Software Process
================

----------------------
Continuous Integration
----------------------

HDMF is tested against Ubuntu, macOS, and Windows operating systems.
The project has both unit and integration tests.
Tests run on `GitHub Actions`_.

Each time a PR is created or updated, the project is built, packaged, and tested on all supported operating systems
and python distributions. That way, as a contributor, you know if you introduced regressions or coding style
inconsistencies.

There are badges in the README_ file which shows the current condition of the dev branch.

Additionally, HDMF runs tests against known NWB extensions to ensure that changes to core HDMF functionality
don't break existing extensions.

.. _GitHub Actions: https://github.com/hdmf-dev/hdmf/actions
.. _README: https://github.com/hdmf-dev/hdmf/blob/dev/README.rst


--------
Coverage
--------

Code coverage is computed and reported using the coverage_ tool. There are two coverage-related badges in the README_
file. One shows the status of the `GitHub Action workflow`_ which runs the coverage_ tool and uploads the report to
codecov_, and the other badge shows the percentage coverage reported from codecov_. A detailed report can be found on
codecov_, which shows line by line which lines are covered by the tests.

.. _coverage: https://coverage.readthedocs.io
.. _GitHub Action workflow: https://github.com/hdmf-dev/hdmf/actions?query=workflow%3A%22Run+coverage%22
.. _codecov: https://app.codecov.io/gh/hdmf-dev/hdmf/tree/dev/src/hdmf

..  _software_process_requirement_specifications:

-------------------------
Installation Requirements
-------------------------

pyproject.toml_ contains a list of package dependencies and their version ranges allowed for
running HDMF. As a library, upper bound version constraints create more harm than good in the long term (see this
`blog post`_) so we avoid setting upper bounds on requirements.

When setting lower bounds, make sure to specify the lower bounds in the ``[project] dependencies`` key and
``[project.optional-dependencies] min-reqs`` key in pyproject.toml_.
The latter is used in automated testing to ensure that the package runs
correctly using the minimum versions of dependencies.

Minimum requirements should be updated manually if a new feature or bug fix is added in a dependency that is required
for proper running of HDMF. Minimum requirements should also be updated if a user requests that HDMF be installable
with an older version of a dependency, all tests pass using the older version, and there is no valid reason for the
minimum version to be as high as it is.

.. _pyproject.toml: https://github.com/hdmf-dev/hdmf/blob/dev/pyproject.toml
.. _blog post: https://iscinumpy.dev/post/bound-version-constraints/

--------------------
Testing Requirements
--------------------

pyproject.toml_ contains the optional dependency group "test" with testing requirements.

See tox.ini_ and the GitHub Actions workflows for how different testing environments are
defined using the optional dependency groups.

environment-ros3.yml_ lists the dependencies used to test ROS3 streaming in HDMF which
can only be done in a Conda environment.

.. _tox.ini: https://github.com/hdmf-dev/hdmf/blob/dev/tox.ini
.. _environment-ros3.yml: https://github.com/hdmf-dev/hdmf/blob/dev/environment-ros3.yml

--------------------------
Documentation Requirements
--------------------------

pyproject.toml_ contains the optional dependency group "docs" with documentation requirements.
This dependency group is used by ReadTheDocs_ to initialize the local environment for Sphinx to run
(see .readthedocs.yaml_).

.. _ReadTheDocs: https://readthedocs.org/projects/hdmf/
.. _.readthedocs.yaml: https://github.com/hdmf-dev/hdmf/blob/dev/.readthedocs.yaml

-------------------------
Versioning and Releasing
-------------------------

HDMF uses setuptools_scm_ for versioning source and wheel distributions. `setuptools_scm` creates a semi-unique release
name for the wheels that are created based on git tags.
After all the tests pass, the "Deploy release" GitHub Actions workflow
creates both a wheel (``\*.whl``) and source distribution (``\*.tar.gz``) for Python 3
and uploads them back to GitHub as a release_.

It is important to note that GitHub automatically generates source code archives in ``.zip`` and ``.tar.gz`` formats and
attaches those files to all releases as an asset. These files currently do not contain the submodules within HDMF and
thus do not serve as a complete installation. For a complete source code archive, use the source distribution generated
by GitHub Actions, typically named ``hdmf-{version}.tar.gz``.

.. _setuptools_scm: https://github.com/pypa/setuptools_scm
.. _release: https://github.com/hdmf-dev/hdmf/releases
