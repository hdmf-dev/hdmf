.. _citing:

Citing HDMF
============

BibTeX entry
------------

If you use HDMF in your research, please use the following citation:

.. code-block:: bibtex

  @INPROCEEDINGS{9005648,
    author={A. J. {Tritt} and O. {Rübel} and B. {Dichter} and R. {Ly} and D. {Kang} and E. F. {Chang} and L. M. {Frank} and K. {Bouchard}},
    booktitle={2019 IEEE International Conference on Big Data (Big Data)},
    title={HDMF: Hierarchical Data Modeling Framework for Modern Science Data Standards},
    year={2019},
    volume={},
    number={},
    pages={165-179},
    doi={10.1109/BigData47090.2019.9005648}}

Using RRID
----------

* **RRID:**  (Hierarchical Data Modeling Framework, RRID:SCR_021303)

Using duecredit
-----------------

Citations can be generated using duecredit_. To install duecredit, run ``pip install duecredit``.

You can obtain a list of citations for your Python script, e.g., ``yourscript.py``, using:

.. code-block:: bash

   cd /path/to/your/module
   python -m duecredit yourscript.py

Alternatively, you can set the environment variable ``DUECREDIT_ENABLE=yes``

.. code-block:: bash

   DUECREDIT-ENABLE=yes python yourscript.py

Citations will be saved in a hidden file (``.duecredit.p``) in the current directory. You can then use the duecredit_
command line tool to export the citations to different formats. For example, you can display your citations in
BibTeX format using:

.. code-block:: bash

   duecredit summary --format=bibtex

For more information on using duecredit, please consult its `homepage <https://github.com/duecredit/duecredit>`_.

.. _duecredit: https://github.com/duecredit/duecredit
