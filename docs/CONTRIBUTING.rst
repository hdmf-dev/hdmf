How to contribute to HDMF software and documents
=================================================

.. _sec-code-of-conduct:

Code of Conduct
---------------

This project and everyone participating in it is governed by our `code of conduct guidelines <https://github.com/hdmf-dev/hdmf/blob/dev/.github/CODE_OF_CONDUCT.md>`_. By participating, you are expected to uphold this code. Please report unacceptable behavior.

.. _sec-contribution-types:

Types of Contributions
----------------------

Did you find a bug? or Do you intend to add a new feature or change an existing one?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Submit issues and requests using out `issue tracker <https://github.com/hdmf-dev/hdmf/issues>`_**

* **Ensure the feature or change was not already reported** by searching on GitHub under `HDMF Issues <https://github.com/hdmf-dev/hdmf/issues>`_

* If you are unable to find an open issue addressing the problem then open a new issue on the respective repository. Be sure to use our issue templates and include:

    * **brief and descriptive title**
    * **clear description of the problem you are trying to solve***. Describing the use case is often more important than proposing a specific solution. By describing the use case and problem you are trying to solve gives the development team community a better understanding for the reasons of changes and enables others to suggest solutions.
    * **context** providing as much relevant information as possible and if available a **code sample** or an **executable test case** demonstrating the expected behavior and/or problem.

* Be sure to select the appropriate labels (see :ref:`sec-issue-labels`) for your tickets so that they can be processed accordingly.

* HDMF is currently being developed primarily by staff at scientific research institutions and industry, most of which work on many different research projects. Please be patient, if our development team is not able to respond immediately to your issues. In particular issues that belong to later project milestones may not be reviewed or processed until work on that milestone begins.

Did you write a patch that fixes a bug or implements a new feature?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
See the ``Contributing Patches and Changes`` section below for details.

Did you fix whitespace, format code, or make a purely cosmetic patch in source code?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Source code changes that are purely cosmetic in nature and do not add anything substantial to the stability, functionality, or testability will generally not be accepted unless they have been approved beforehand. One of the main reasons is that there are a lot of hidden costs in addition to writing the code itself, and with the limited resources of the project, we need to optimize developer time. E.g,. someone needs to test and review PRs, backporting of bug fixes gets harder, it creates noise and pollutes the git repo and many other cost factors.

Do you have questions about HDMF?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See our `hdmf-dev.github.io <https://hdmf-dev.github.io/>`_ website for details.

Informal discussions between developers and users?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The https://nwb-users.slack.com slack is currently used mainly for informal discussions between developers and users.

.. _sec-contributing:

Contributing Patches and Changes
--------------------------------

To contribute to HDMF you must submit your changes to the ``dev`` branch via a `Pull Request <https://help.github.com/articles/creating-a-pull-request>`_.

From your local copy directory, use the following commands.

1) First create a new branch to work on

.. code-block:: bash

    $ git checkout -b <new_branch>

2) Make your changes.

3) Push your feature branch to origin (i.e. github)

.. code-block:: bash

    $ git push origin <new_branch>

4) Once you have tested and finalized your changes, create a pull request targeting ``dev`` as the base branch. Be sure to use our `pull request template <https://github.com/hdmf-dev/hdmf/blob/dev/.github/pull_request_template.md>`_ and:

    * Ensure the PR description clearly describes the problem and solution.
    * Include the relevant issue number if applicable.
    * Before submitting, please ensure that the code follows the standard coding style of the respective repository.
    * **NOTE:** Contributed branches will be removed by the development team after the merge is complete and should, hence, not be used after the pull request is complete.


.. _sec-issue-labels:

Issue Labels, Projects, and Milestones
--------------------------------------

Labels
^^^^^^

Labels are used to describe the general scope of an issue, e.g., whether it describes a bug or feature request etc. Please review and select the appropriate labels for the respective Git repository:

    * `HDMF issue labels  <https://github.com/hdmf-dev/hdmf/labels>`_

Milestones
^^^^^^^^^^

Milestones are used to define the scope and general timeline for issues. Please review and select the appropriate milestones for the respective Git repository:

    * `HDMF milestones <https://github.com/hdmf-dev/hdmf/milestones>`_

Projects
^^^^^^^^

Projects are currently used mainly on the HDMF organization level and are only accessible to members of the organization. Projects are used to plan and organize developments across repositories. We currently do not use projects on the individual repository level, although that might change in the future.

.. _sec-styleguides:

Styleguides
-----------

Git Commit Message Styleguide
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Use the present tense ("Add feature" not "Added feature")
* The first should be short and descriptive.
* Additional details may be included in further paragraphs.
* If a commit fixes an issues, then include "Fix #X" where X is the number of the issue.
* Reference relevant issues and pull requests liberally after the first line.

Documentation Styleguide
^^^^^^^^^^^^^^^^^^^^^^^^

All documentations is written in reStructuredText (RST) using Sphinx.

Python Code Styleguide
^^^^^^^^^^^^^^^^^^^^^^

Python coding style is checked via ``flake8`` for automatic checking of PEP8 style during pull requests.

Endorsement
-----------

Please don’t take the fact that working with an organization (e.g., during a hackathon or via GitHub) as an endorsement of your work or your organization. It’s okay to say  e.g., “We worked with XXXXX to advance science” but not e.g., “XXXXX supports our work on HDMF”.”

License and Copyright
=======================

See the `license <https://raw.githubusercontent.com/hdmf-dev/hdmf/dev/license.txt>`_ files for details about the copyright and license.

As indicated in the HDMF license: *“You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the features, functionality or performance of the source code ("Enhancements") to anyone; however, if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley National Laboratory, without imposing a separate written license agreement for such Enhancements, then you hereby grant the following license: a non-exclusive, royalty-free perpetual license to install, use, modify, prepare derivative works, incorporate into other computer software, distribute, and sublicense such enhancements or derivative works thereof, in binary and source code form.”*

Contributors to the HDMF code base are expected to use a permissive, non-copyleft open source license. Typically 3-clause BSD is used, but any compatible license is allowed, the MIT and Apache 2.0 licenses being good alternative choices. The GPL and other copyleft licenses are not allowed due to the consternation it generates across many organizations.

Also, make sure that you are permitted to contribute code. Some organizations, even academic organizations, have agreements in place that discuss IP ownership in detail (i.e., address IP rights and ownership that you create while under the employ of the organization). These are typically signed documents that you looked at on your first day of work and then promptly forgot. We don’t want contributed code to be yanked later due to IP issues.
