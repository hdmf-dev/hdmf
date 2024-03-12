
..  _spec_language_support:

===========================================
Support for the HDMF Specification Language
===========================================

The HDMF API provides nearly full support for all features of the `HDMF Specification Language`_
version 3.0.0, except for the following:

1. Attributes containing multiple references (see `#833`_)
2. Certain text and integer values for quantity (see `#423`_, `#531`_)
3. Datasets that do not have a data_type_inc/data_type_def and contain either a reference dtype or a compound dtype (see `#737`_)
4. Passing dataset dtype and shape from parent data type to child data type (see `#320`_)

.. _HDMF Specification Language: https://hdmf-schema-language.readthedocs.io
.. _#833: https://github.com/hdmf-dev/hdmf/issues/833
.. _#423: https://github.com/hdmf-dev/hdmf/issues/423
.. _#531: https://github.com/hdmf-dev/hdmf/issues/531
.. _#737: https://github.com/hdmf-dev/hdmf/issues/737
.. _#320: https://github.com/hdmf-dev/hdmf/issues/320
