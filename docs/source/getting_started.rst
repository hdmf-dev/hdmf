..  _getting_started:

------------
Dependencies
------------

HDMF has the following minimum requirements, which must be installed before you can get started using HDMF.

#. Python 3.5, 3.6, 3.7, or 3.8
#. pip

------------
Installation
------------

Install release from PyPI
-------------------------

The `Python Package Index (PyPI) <https://pypi.org>`_ is a repository of software for the Python programming language.

To install or update the HDMF distribution from PyPI, simply run:

.. code::

   $ pip install -U hdmf

This will automatically install the following required dependencies:

 #. h5py
 #. numpy
 #. scipy
 #. pandas
 #. ruamel.yaml

Install release from conda-forge
--------------------------------

conda-forge_ is a community-led collection of recipes, build infrastructure,
and distributions for the conda_ package manager.

.. _conda-forge: https://conda-forge.org/#about
.. _conda: https://conda.io/docs/

To install or update the HDMF distribution from conda-forge using conda, simply run:

.. code::

   $ conda install -c conda-forge hdmf


Install latest pre-release
--------------------------

To try out the latest features and also set up continuous integration of your own project against the
latest version of HDMF, install the latest release from GitHub.

.. code::

   $ pip install -U hdmf --find-links https://github.com/hdmf-dev/hdmf/releases/tag/latest --no-index


--------------
For developers
--------------

Install from Git repository
---------------------------

For development, an editable install in a virtual environment is recommended. First, create a new virtual environment
located at `~/hdmf` using the virtualenv_ tool.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/

.. code::

   $ pip install -U virtualenv
   $ virtualenv ~/hdmf
   $ source ~/hdmf/bin/activate

Alternatively, you can use the conda_ environment and package manager to create your virtual environment. This may
work better on Windows.

.. code::

    $ conda create --name hdmf-dev python=3.8
    $ conda activate hdmf-dev

Then clone the git repository for HDMF, install the HDMF package requirements using the pip_ Python package manager, and
install HDMF.

.. _pip: https://pip.pypa.io/en/stable/

.. code::

   $ git clone --recurse-submodules git@github.com:hdmf-dev/hdmf.git
   $ cd hdmf
   $ pip install -r requirements.txt
   $ pip install -e .

Run tests
---------

For running the tests, it is required to install the development requirements. Within a virtual environment, run the
following code, which will clone the git repository for HDMF, install the HDMF package requirements using pip,
install HDMF, and run tests using the tox_ automated testing tool.

.. _tox: https://tox.readthedocs.io/en/latest/

.. code::

   $ cd hdmf
   $ pip install -r requirements.txt -r requirements-dev.txt
   $ pip install -e .
   $ tox


Following the HDMF Style Guide
------------------------------

Before you create a Pull Request, make sure you are following the HDMF style guide (PEP8_).
To check whether your code conforms to the HDMF style guide, make sure you have the development requirements installed
(see above) and then simply run the flake8_ tool in the project's root directory.

.. _flake8: http://flake8.pycqa.org/en/latest/
.. _PEP8: https://www.python.org/dev/peps/pep-0008/

.. code::

   $ flake8
