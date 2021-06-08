..  _install_developers:

------------------------------
Installing HDMF for Developers
------------------------------

Set up a virtual environment
----------------------------

For development, we recommend installing HDMF in a virtual environment in editable mode. For beginners and for Windows
developers, we recommend using the `conda package and environment management system`_ for managing local virtual
environments. Install [Anaconda](https://www.anaconda.com/distribution) to install the ``conda`` tool. You can also use
the virtualenv_ tool to create a new virtual environment.

.. _conda package and environment management system: https://conda.io/projects/conda/en/latest/index.html
.. _virtualenv: https://virtualenv.pypa.io/en/stable/

Option 1: Using conda
^^^^^^^^^^^^^^^^^^^^^^^

Create and activate a new virtual environment called "hdmf-dev" with Python 3.8 installed.

.. code::

    $ conda create --name hdmf-dev python=3.8
    $ conda activate hdmf-dev

Option 2: Using virtualenv
^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, create a new virtual environment located at `~/hdmf`.

.. code::

   pip install -U virtualenv
   virtualenv ~/hdmf
   source ~/hdmf/bin/activate

Install from GitHub
-------------------

After you have created and activated a virtual environment, clone the HDMF git repository from GitHub, install the
package requirements using the pip_ Python package manager, and install HDMF in editable mode.

.. _pip: https://pip.pypa.io/en/stable/

.. code::

   git clone --recurse-submodules https://github.com/hdmf-dev/hdmf.git
   cd hdmf
   pip install -r requirements.txt -r requirements-dev.txt -r requirements-doc.txt
   pip install -e .

Run tests
---------

You can run the full test suite with the following command:

.. code::

   python test.py

You could also run the full test suite by installing and running the ``pytest`` tool,
a popular testing tool that provides more options for configuring test runs.

First, install ``pytest``:

.. code::

   pip install pytest
   
Then run the full test suite:

.. code::

   pytest

You can also run a specific test module or class, or you can configure ``pytest`` to start the 
Python debugger (PDB) prompt on an error, e.g., 

.. code::

   pytest tests/unit/test_container.py                                   # run all tests in the module
   pytest tests/unit/test_container.py::TestContainer                    # run all tests in this class
   pytest tests/unit/test_container.py::TestContainer::test_constructor  # run this test method
   pytest --pdb tests/unit/test_container.py                             # start pdb on error


Finally, you can run tests across multiple Python versions using the tox_ automated testing tool. Running ``tox`` will
create a virtual environment, install dependencies, and run the test suite for Python 3.6, 3.7, 3.8, and 3.9.
This can take some time to run.

.. _pytest: https://docs.pytest.org/
.. _tox: https://tox.readthedocs.io/en/latest/

.. code::

   tox

Install latest pre-release
--------------------------

To try out the latest features or set up continuous integration of your own project against the
latest version of HDMF, install the latest release from GitHub.

.. code::

  pip install -U hdmf --find-links https://github.com/hdmf-dev/hdmf/releases/tag/latest --no-index
