.. _linkml_mapping:

=================================
HDMF ↔ LinkML mapping conventions
=================================

These conventions cover the constructs used by ``base.yaml`` (``Data``, ``Container``,
``SimpleMultiContainer``) and ``sparse.yaml`` (``CSRMatrix``), packaged as a minimal test
namespace. This is **not** a complete mapping of HDMFSL; constructs not yet covered are
listed at the end.

The LinkML files that apply these conventions to that test namespace live in
``tests/unit/linkml_tests/fixtures/``. They are the reference the reader and writer are
tested against, and ``tests/unit/linkml_tests/test_fixtures.py`` checks that they load
under ``linkml-runtime``'s ``SchemaView`` and cover every field of the ``Spec`` objects HDMF
loads natively from the HDMFSL sources.

Goal and guiding principles
---------------------------

Define how HDMF Schema Language (HDMFSL) constructs are represented in LinkML, such that
HDMF can read LinkML into its ``Spec`` objects (``GroupSpec``, ``DatasetSpec``,
``AttributeSpec``) and write those ``Spec`` objects back to LinkML.

1. **Spec round-trip is the contract.** The ``Spec`` reconstructed from the LinkML must
   equal the ``Spec`` HDMF loads natively from the HDMFSL. Every field the ``Spec``
   classes carry (``name``, ``doc``, ``dtype``, ``dims``, ``shape``, ``required``,
   ``quantity``, ``data_type_def`` / ``data_type_inc``, containment) must be recoverable
   from the LinkML.

   ``Spec`` subclasses ``dict`` and inherits ``dict`` equality, so the comparison is over
   the keys actually present. This puts two requirements on the reader:

   - **Defaults stay absent.** HDMF omits a field left at its default: an
     ``AttributeSpec`` with ``required`` at its default carries no ``required`` key, and a
     dataset or subgroup at ``quantity`` ``1`` carries no ``quantity`` key. A reader that
     writes the default value out explicitly produces a ``Spec`` that does not compare
     equal.
   - **Order within a construct list is preserved.** ``attributes``, ``datasets``, and
     ``groups`` are lists, so their order is part of the comparison. LinkML holds all three
     kinds in one ``attributes`` block on the class, so the reader partitions the slots by
     ``spec_type`` and keeps their relative order within each partition. Declaring the
     slots in HDMFSL order (attributes, then datasets, then groups) makes the LinkML read
     the same way as the HDMFSL source.
2. **Annotations carry HDMFSL provenance.** LinkML flattens groups, datasets, and
   attributes into classes and slots; HDMF needs to recover which was which. We record
   that with a small, explicit annotation vocabulary rather than inferring it.
3. **Prefer native LinkML; preserve, don't drop.** Use native LinkML constructs
   (``is_a``, the arrays metamodel, ``required`` / ``multivalued``) wherever they fit.
   Anything HDMFSL needs that has no native home is preserved in an annotation. Nothing
   the reader needs is silently dropped.
4. **No LinkML features that Spec cannot model.** Constructs with no HDMFSL equivalent are
   out of scope.

Annotation vocabulary
---------------------

All annotations are written in LinkML's compact form, e.g. ``spec_type: dataset``, which
LinkML expands to ``{tag: spec_type, value: dataset}``.

.. list-table::
   :header-rows: 1

   * - Annotation
     - Applies to
     - Values
     - Purpose
   * - ``spec_type``
     - class
     - ``group``, ``dataset``
     - Which ``Spec`` subclass the ``data_type_def`` builds (``GroupSpec`` vs
       ``DatasetSpec``).
   * - ``spec_type``
     - slot
     - ``attribute``, ``dataset``, ``group``
     - Which ``Spec`` construct the slot maps back to. Not used on the identifier slot,
       which LinkML already marks with ``identifier: true`` (see Naming and identity).

Namespace-level metadata annotations are described in the Namespace section.

File and schema structure
-------------------------

HDMFSL has two tiers (a ``namespace.yaml`` plus the schema files it lists). LinkML's unit
is a ``SchemaDefinition`` per file with ``imports``. We mirror the HDMFSL file layout:

- One LinkML schema **per HDMFSL schema file**: ``base.yaml`` → a ``base`` schema,
  ``sparse.yaml`` → a ``sparse`` schema. Each holds the classes for the ``data_type_def``\ s
  in that file and imports whatever it references.
- One **namespace-level** LinkML schema for the test namespace, which imports the per-file
  schemas. This is what HDMF loads through the namespace/catalog path.
- One **types** schema (``hdmf-linkml-types``) defining the HDMFSL dtypes as LinkML
  ``TypeDefinition``\ s (see dtypes), imported by any schema that uses them.

Type-level mappings
-------------------

data_type_def / data_type_inc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Each ``data_type_def`` → a LinkML ``class`` whose name is the type name.
- ``data_type_inc`` (when the type extends another) → ``is_a``.
- A class-level ``spec_type`` annotation records whether the type is a group or a dataset,
  because HDMF builds a ``GroupSpec`` for a ``data_type_def`` declared under ``groups:``
  and a ``DatasetSpec`` for one under ``datasets:``, and that cannot be inferred from the
  LinkML structure alone.

.. code-block:: yaml

   # Container (a group def) and Data (a dataset def), both abstract bases
   Container:
     description: An abstract data type for a group storing collections of data and metadata. Base type for all data and metadata containers.
     annotations:
       spec_type: group
   Data:
     description: An abstract data type for a dataset.
     annotations:
       spec_type: dataset

Attributes, datasets, and subgroups → slots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each ``attributes:``, ``datasets:``, or ``groups:`` entry within a type becomes a slot on
the class, tagged with ``spec_type`` so the reader rebuilds the right construct (the
identifier slot, marked ``identifier: true``, is the one exception and carries no
``spec_type``):

- ``spec_type: attribute`` → ``AttributeSpec``
- ``spec_type: dataset`` → ``DatasetSpec``
- ``spec_type: group`` → ``GroupSpec`` (subgroup)

How the reader tells a **named, typed** dataset/attribute from a **typed include**
(``data_type_inc``):

- If the slot ``range`` is a **dtype**, or the ``AnyType`` class → a named
  dataset/attribute; the slot name is the HDMFSL ``name``. ``AnyType`` is a class rather
  than a type (see dtypes), so it is called out here as the one class range that does not
  mean an include.
- If the slot ``range`` is any **other defined class** → an include of that type
  (``data_type_inc``). In scope, includes are unnamed, so the slot name is synthesized
  from the included type (snake_case) and is informational only (the writer re-emits the
  entry with ``data_type_inc`` and no ``name``). Named includes are deferred (see Out of
  scope).

The HDMFSL ``doc`` on the entry becomes the slot ``description``, for includes as well as
for named entries.

A multivalued class-valued slot (an include with ``quantity`` ``*`` or ``+``) uses
``inlined_as_list: true``, so its contents serialize as a list of the included objects.

Naming and identity (the name identifier slot)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LinkML requires an identifier for objects that are inlined as dictionaries, and HDMF
objects are keyed by name in the file hierarchy. HDMFSL leaves this implicit (objects are
named where they are used, not via a declared attribute); LinkML makes it explicit. A class
with no ``is_a`` declares a ``name`` slot (``identifier: true``, ``range: string``,
``required: true``); classes with an ``is_a`` inherit it.

.. code-block:: yaml

       name:
         identifier: true
         range: string
         required: true

It is declared once at the root of each hierarchy (``Data`` and ``Container`` in the test
namespace) because LinkML permits at most one identifier per class and inheritance already
supplies it to every subclass. Every HDMF object therefore has an identifier, and the
reader only ever sees the slot on the class that declares it.

The reader handles the ``identifier`` slot specially: it represents the object's hierarchy
name, not a declared attribute, so the reader never builds an ``AttributeSpec`` from it. No
``spec_type`` annotation is needed, because ``identifier: true`` already identifies this
slot. The slot is required for LinkML inlining of data instances and for
forward-compatibility with the Pydantic work.

Fixed names and default names on a type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``name`` identifier slot above is the name an *instance* is keyed by. HDMFSL also lets
a type constrain that name at the schema level, and those constraints map to the identifier
slot rather than to a separate construct:

- A ``data_type_def`` that also fixes a ``name`` (the name is not free) →
  ``equals_string: <name>`` on the identifier slot.
- ``default_name`` (a name used when the caller supplies none) →
  ``ifabsent: string(<default_name>)`` on the identifier slot.

Neither is used by the types in scope, so both are stated here as the convention and are
not exercised by the fixtures. A type with neither is unnamed: its instances are named
where they are used.

dtypes
~~~~~~

HDMFSL dtypes map to LinkML ranges via a companion ``hdmf-linkml-types`` schema. A slot's
``range`` is the HDMFSL dtype string verbatim (``range: uint``), and the reader recovers the
dtype from the range name.

**Every HDMFSL dtype string is its own named type, synonyms included.** HDMFSL treats
``uint`` and ``uint32``, or ``text`` / ``utf`` / ``utf8`` / ``utf-8``, as synonyms, and a
``Spec`` stores whichever spelling the schema used. Collapsing synonyms onto a single type
would rewrite ``dtype: uint`` as ``dtype: uint32`` on the way back and break the ``Spec``
comparison, so each spelling is a separate ``TypeDefinition``. A synonym's ``typeof`` points
at the primary dtype, which records the synonym relationship in LinkML itself; a primary
dtype's ``typeof`` points at a LinkML base type and carries the width and sign constraints.
Identity for the round trip is always the name, never ``typeof``.

**The dtypes LinkML already provides are reused rather than redefined.** HDMFSL's ``float``,
``double``, ``date``, and ``datetime`` collide by name with LinkML built-in types whose
semantics agree (``xsd:float`` is 32 bit, ``xsd:double`` is 64 bit), so ``hdmf-linkml-types``
imports ``linkml:types`` and leaves those four to it. Redefining them would shadow the
built-ins for every schema in the import closure.

.. code-block:: yaml

   # in hdmf-linkml-types
   imports:
     - linkml:types
   types:
     uint32:
       typeof: integer
       minimum_value: 0
       maximum_value: 4294967295
     uint:                      # synonym of uint32; kept distinct so the spelling survives
       typeof: uint32
     float32:
       typeof: float            # float comes from linkml:types
     text:
       typeof: string
     # ... one per HDMFSL dtype string

A dataset or attribute with **no dtype** (the ``CSRMatrix`` ``data`` dataset) →
``range: AnyType``, where ``AnyType`` is a class with ``class_uri: linkml:Any`` defined
alongside the types. The reader maps ``range: AnyType`` back to ``dtype = None``. It is a
class because LinkML expresses an unconstrained range that way, which is why the slot rules
above name it as the one class range that is not an include.

The reference dtype ``object`` is deferred along with reference and compound dtypes, so
``hdmf-linkml-types`` does not define it (see Out of scope).

Arrays (dims / shape)
~~~~~~~~~~~~~~~~~~~~~~~

A construct's ``dims`` / ``shape`` map to the LinkML arrays metamodel: an ``array`` with
one ``dimensions`` entry per axis. This applies uniformly to attributes and datasets.

- A ``null`` shape entry → a dimension with an ``alias`` and no cardinality.
- A fixed integer shape entry → ``exact_cardinality: N``.
- Multiple allowed shapes → ``any_of`` of ``array`` expressions. (Not exercised by the
  in-scope types; documented as the convention.)
- The dimension ``alias`` is the HDMFSL dimension label, used verbatim. LinkML's ``alias``
  is a free-form optional string (no pattern, identifier, or uniqueness constraint), so
  labels like ``number of rows, number of columns`` round-trip exactly. The reader
  reconstructs ``dims`` from the dimensions' aliases in order, and ``shape`` from
  ``exact_cardinality`` (or ``None`` when absent).

.. code-block:: yaml

         shape:                      # the CSRMatrix shape attribute: dims=["number of rows, number of columns"], shape=[2]
           range: uint
           required: true
           annotations:
             spec_type: attribute
           array:
             dimensions:
             - alias: number of rows, number of columns
               exact_cardinality: 2

quantity and required
~~~~~~~~~~~~~~~~~~~~~~~

- Attribute ``required`` (a boolean in HDMFSL) → slot ``required``, always written out
  explicitly. HDMFSL defaults an attribute to required while LinkML defaults a slot to
  optional, so an omitted ``required`` on an attribute slot would flip the meaning.
- Dataset/subgroup ``quantity`` → slot ``required`` + ``multivalued``:

.. list-table::
   :header-rows: 1

   * - ``quantity``
     - ``required``
     - ``multivalued``
   * - ``1`` (default)
     - true
     - —
   * - ``?``
     - —
     - —
   * - ``*``
     - —
     - true
   * - ``+``
     - true
     - true

A dash means the key is omitted. The four combinations are distinct, so a dataset or
subgroup slot recovers its ``quantity`` unambiguously.

``quantity`` (how many of the object) is independent of the array shape (the object's
dimensions): a single required array dataset is ``required: true`` with no ``multivalued``,
plus an ``array`` expression.

Namespace-level mapping
-----------------------

An HDMFSL ``namespace.yaml`` declares a namespace (name, version, metadata) and lists its
schema files. It maps to the namespace-level LinkML schema:

- ``name`` → schema ``name`` and ``id`` (a URI; a placeholder base URI is used for now, to
  be finalized with the LinkML team).
- ``version`` → schema ``version``.
- ``doc`` → schema ``description``.
- ``full_name`` → schema ``title``.
- The ``schema:`` list (the ``source:`` files) → ``imports`` of the corresponding
  per-file LinkML schemas.
- ``author`` and ``contact`` (positionally one-to-one in HDMFSL) have no native LinkML
  schema field. LinkML annotation values accept structured data, not just scalars, so the
  two lists are merged and preserved in a single ``authors`` annotation whose value is a
  list of ``{name, email}`` objects; this keeps each name bound to its email. The
  per-entry ``title`` / ``doc`` on each schema source are carried on the imported per-file
  schema (its ``title`` / ``description``).

Cross-namespace imports (an HDMFSL namespace importing another, e.g. ``hdmf-experimental``
importing ``hdmf-common``) are out of scope; the test namespace is self-contained.

LinkML schemas require an ``id`` URI and use ``prefixes`` / ``default_prefix``. A
placeholder base URI (e.g. ``https://w3id.org/hdmf/...``) is used for now; the final base
URI convention for HDMF/NWB LinkML schemas will be settled with the LinkML team. A prefix
is a CURIE prefix, so a namespace name containing a hyphen is spelled with underscores
there (``hdmf-common-test`` → ``hdmf_common_test``); the schema ``name`` keeps the HDMFSL
spelling, and that is what the reader reads the namespace name from.

Worked example
--------------

These are the LinkML files for the test namespace, as committed under
``tests/unit/linkml_tests/fixtures/``. They are shown here in full so the conventions above
can be read against a complete example, and they are the same files the tests load, so the
example cannot drift from what is validated.

HDMFSL dtypes → hdmf-linkml-types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../tests/unit/linkml_tests/fixtures/hdmf-linkml-types.yaml
   :language: yaml

base.yaml → base LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../tests/unit/linkml_tests/fixtures/base.yaml
   :language: yaml

sparse.yaml → sparse LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../tests/unit/linkml_tests/fixtures/sparse.yaml
   :language: yaml

Test namespace.yaml → namespace LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../tests/unit/linkml_tests/fixtures/namespace.yaml
   :language: yaml

Out of scope
------------

- Dataset special cases: scalar-with-attributes, list-like datasets, class-range
  references.
- Compound dtypes; reference dtypes, including the ``object`` dtype; links.
- The ``DynamicTable`` family (``VectorData``, ``VectorIndex``, ``DynamicTableRegion``,
  ragged arrays, inter-table references).
- Named includes (a ``data_type_inc`` entry that also fixes a ``name``).
- Inheritance roll-down (HDMFSL's recursive merging of parent fields into children).
- Cross-namespace imports.
- All LinkML features with no HDMFSL equivalent (enums, ontology URIs, rules,
  conditional/cross-field validation, mixins, abstract classes).
