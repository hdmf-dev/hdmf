..  _install_developers:

------------------------------
Installing HDMF for Developers
------------------------------


Set up a virtual environment
----------------------------

For development, we recommend installing HDMF in a virtual environment in editable mode. You can use
the venv_ tool that comes packaged with Python to create a new virtual environment. Or you can use the
`conda package and environment management system`_ for managing virtual environments.

.. _venv: https://docs.python.org/3/library/venv.html
.. _conda package and environment management system: https://conda.io/projects/conda/en/latest/index.html


Option 1: Using venv
^^^^^^^^^^^^^^^^^^^^

First, create a new virtual environment using the ``venv`` tool. This
virtual environment will be stored in a new directory called ``"hdmf-env"`` in the current directory.

.. code:: bash

    venv hdmf-env

On macOS or Linux, run the following to activate your new virtual environment:

.. code:: bash

    source hdmf-env/bin/activate

On Windows, run the following to activate your new virtual environment:

.. code:: batch

    hdmf-env\Scripts\activate

This virtual environment is a space where you can install Python packages that are isolated from other virtual
environments. This is especially useful when working on multiple Python projects that have different package
requirements and for testing Python code with different sets of installed packages or versions of Python.

Activate your newly created virtual environment using the above command whenever you want to work on HDMF. You can also
deactivate it using the ``deactivate`` command to return to the base environment. And you can delete the virtual
environment by deleting the directory that was created.


Option 2: Using conda
^^^^^^^^^^^^^^^^^^^^^

The `conda package and environment management system`_ is an alternate way of managing virtual environments.
First, install Anaconda_ to install the ``conda`` tool. Then create and
activate a new virtual environment called ``"hdmf-env"`` with Python 3.12 installed.

.. code:: bash

    conda create --name hdmf-env python=3.12
    conda activate hdmf-env

Similar to a virtual environment created with ``venv``, a conda environment
is a space where you can install Python packages that are isolated from other virtual
environments. In general, you should use ``conda install`` instead of ``pip install`` to install packages
in a conda environment.

Activate your newly created virtual environment using the above command whenever you want to work on HDMF. You can also
deactivate it using the ``conda deactivate`` command to return to the base environment. And you can delete the virtual
environment by using the ``conda remove --name hdmf-venv --all`` command.

.. note::

    For advanced users, we recommend using Mambaforge_, a faster version of the conda package manager
    that includes conda-forge as a default channel.

.. _Anaconda: https://www.anaconda.com/products/distribution
.. _Mambaforge: https://github.com/conda-forge/miniforge

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
   recommended that dependencies should be installed via ``conda install``.


Run tests
---------

You can run the full test suite by running:

.. code:: bash

    pytest

This will run all the tests and compute the test coverage. The coverage report can be found in ``/htmlcov``.
You can also run a specific test module or class, or you can configure ``pytest`` to start the
Python debugger (PDB) prompt on an error, e.g.,

.. code:: bash

    pytest tests/unit/test_container.py                                   # run all tests in the module
    pytest tests/unit/test_container.py::TestContainer                    # run all tests in this class
    pytest tests/unit/test_container.py::TestContainer::test_constructor  # run this test method
    pytest --pdb tests/unit/test_container.py                             # start pdb on error


You can run tests across multiple Python versions using the tox_ automated testing tool. Running ``tox`` will
create a virtual environment, install dependencies, and run the test suite for different versions of Python.
This can take some time to run.

.. _pytest: https://docs.pytest.org/
.. _tox: https://tox.wiki/en/latest/

.. code:: bash

    tox

You can also test that the Sphinx Gallery files run without warnings or errors by running:

.. code:: bash

    python test_gallery.py


Install latest pre-release
--------------------------

To try out the latest features or set up continuous integration of your own project against the
latest version of HDMF, install the latest release from GitHub.

.. code:: bash

    pip install -U hdmf --find-links https://github.com/hdmf-dev/hdmf/releases/tag/latest --no-index
