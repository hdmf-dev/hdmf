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

If some of the packages are outdated, see :ref:`update_requirements_files`.

.. _pyproject.toml: https://github.com/hdmf-dev/hdmf/blob/dev/pyproject.toml
.. _blog post: https://iscinumpy.dev/post/bound-version-constraints/

--------------------
Testing Requirements
--------------------

There are several kinds of requirements files used for testing PyNWB.

The first one is requirements-min.txt_, which lists the package dependencies and their minimum versions for
installing HDMF.

The second one is requirements.txt_, which lists the pinned (concrete) dependencies to reproduce
an entire development environment to use HDMF.

The third one is requirements-dev.txt_, which list the pinned (concrete) dependencies to reproduce
an entire development environment to use HDMF, run HDMF tests, check code style, compute coverage, and create test
environments.

The fourth one is requirements-opt.txt_, which lists the pinned (concrete) optional dependencies to use all
available features in HDMF.

The final one is environment-ros3.yml_, which lists the dependencies used to
test ROS3 streaming in HDMF.

.. _requirements-min.txt: https://github.com/hdmf-dev/hdmf/blob/dev/requirements-min.txt
.. _requirements.txt: https://github.com/hdmf-dev/hdmf/blob/dev/requirements.txt
.. _requirements-dev.txt: https://github.com/hdmf-dev/hdmf/blob/dev/requirements-dev.txt
.. _requirements-opt.txt: https://github.com/hdmf-dev/hdmf/blob/dev/requirements-opt.txt
.. _environment-ros3.yml: https://github.com/hdmf-dev/hdmf/blob/dev/environment-ros3.yml

--------------------------
Documentation Requirements
--------------------------

requirements-doc.txt_ lists the dependencies to generate the documentation for HDMF.
Both this file and `requirements.txt` are used by ReadTheDocs_ to initialize the local environment for Sphinx to run.

.. _requirements-doc.txt: https://github.com/hdmf-dev/hdmf/blob/dev/requirements-doc.txt
.. _ReadTheDocs: https://readthedocs.org/projects/hdmf/

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
