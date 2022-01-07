..  _install_developers:

------------------------------
Installing HDMF for Developers
------------------------------

Set up a virtual environment
----------------------------

For development, we recommend installing HDMF in a virtual environment in editable mode. You can use
the virtualenv_ tool to create a new virtual environment. Or you can use the
`conda package and environment management system`_ for managing virtual environments.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/
.. _conda package and environment management system: https://conda.io/projects/conda/en/latest/index.html

Option 1: Using virtualenv
^^^^^^^^^^^^^^^^^^^^^^^^^^

First, install the latest version of the ``virtualenv`` tool and use it to create a new virtual environment. This
virtual environment will be stored in the ``venv`` directory in the current directory.

.. code:: bash

    pip install -U virtualenv
    virtualenv venv

On macOS or Linux, run the following to activate your new virtual environment:

.. code:: bash

    source venv/bin/activate

On Windows, run the following to activate your new virtual environment:

.. code:: batch

    venv\Scripts\activate

This virtual environment is a space where you can install Python packages that are isolated from other virtual
environments. This is especially useful when working on multiple Python projects that have different package
requirements and for testing Python code with different sets of installed packages or versions of Python.

Activate your newly created virtual environment using the above command whenever you want to work on HDMF. You can also
deactivate it using the ``deactivate`` command to return to the base environment.

Option 2: Using conda
^^^^^^^^^^^^^^^^^^^^^

First, install Anaconda_ to install the ``conda`` tool. Then create and
activate a new virtual environment called "venv" with Python 3.8 installed.

.. code:: bash

    conda create --name venv python=3.8
    conda activate venv

Similar to a virtual environment created with ``virtualenv``, a conda environment
is a space where you can install Python packages that are isolated from other virtual
environments. In general, you should use ``conda install`` instead of ``pip install`` to install packages
in a conda environment.

Activate your newly created virtual environment using the above command whenever you want to work on HDMF. You can also
deactivate it using the ``conda deactivate`` command to return to the base environment.

.. _Anaconda: https://www.anaconda.com/distribution

Install from GitHub
-------------------

After you have created and activated a virtual environment, clone the HDMF git repository from GitHub, install the
package requirements using the pip_ Python package manager, and install HDMF in editable mode.

.. _pip: https://pip.pypa.io/en/stable/

.. code:: bash

    git clone --recurse-submodules https://github.com/hdmf-dev/hdmf.git
    cd hdmf
    pip install -r requirements.txt -r requirements-dev.txt -r requirements-doc.txt -r requirements-opt.txt
    pip install -e .

.. note::

   When using ``conda``, you may use ``pip install`` to install dependencies as shown above; however, it is generally
   recommended that dependencies should be installed via ``conda install``, e.g.,

   .. code:: bash

      conda install --file=requirements.txt --file=requirements-dev.txt --file=requirements-doc.txt \
      --file=requirements-opt.txt

Run tests
---------

You can run the full test suite with the following command:

.. code:: bash

    python test.py

You could also run the full test suite by installing and running the ``pytest`` tool,
a popular testing tool that provides more options for configuring test runs.

First, install ``pytest``:

.. code:: bash

    pip install pytest

Then run the full test suite:

.. code:: bash

    pytest

You can also run a specific test module or class, or you can configure ``pytest`` to start the
Python debugger (PDB) prompt on an error, e.g.,

.. code:: bash

    pytest tests/unit/test_container.py                                   # run all tests in the module
    pytest tests/unit/test_container.py::TestContainer                    # run all tests in this class
    pytest tests/unit/test_container.py::TestContainer::test_constructor  # run this test method
    pytest --pdb tests/unit/test_container.py                             # start pdb on error


Finally, you can run tests across multiple Python versions using the tox_ automated testing tool. Running ``tox`` will
create a virtual environment, install dependencies, and run the test suite for Python 3.7, 3.8, 3.9, and 3.10.
This can take some time to run.

.. _pytest: https://docs.pytest.org/
.. _tox: https://tox.readthedocs.io/en/latest/

.. code:: bash

    tox

Install latest pre-release
--------------------------

To try out the latest features or set up continuous integration of your own project against the
latest version of HDMF, install the latest release from GitHub.

.. code:: bash

    pip install -U hdmf --find-links https://github.com/hdmf-dev/hdmf/releases/tag/latest --no-index
