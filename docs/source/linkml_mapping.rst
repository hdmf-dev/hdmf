.. _linkml_mapping:

=================================
HDMF ↔ LinkML mapping conventions
=================================

These conventions cover the constructs used by ``base.yaml`` (``Data``, ``Container``,
``SimpleMultiContainer``) and ``sparse.yaml`` (``CSRMatrix``), packaged as a minimal test
namespace. This is **not** a complete mapping of HDMFSL; constructs not yet covered are
listed at the end.

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

- If the slot ``range`` is a **dtype** (or ``AnyType``) → a named dataset/attribute; the
  slot name is the HDMFSL ``name``.
- If the slot ``range`` is a **defined class** → an include of that type
  (``data_type_inc``). In scope, includes are unnamed, so the slot name is synthesized
  from the included type (snake_case) and is informational only (the writer re-emits the
  entry with ``data_type_inc`` and no ``name``). Named includes are deferred (see Out of
  scope).

A multivalued class-valued slot (an include with ``quantity`` ``*`` or ``+``) uses
``inlined_as_list: true``, so its contents serialize as a list of the included objects.

Naming and identity (the name identifier slot)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LinkML requires an identifier for objects that are inlined as dictionaries, and HDMF
objects are keyed by name in the file hierarchy. HDMFSL leaves this implicit (objects are
named where they are used, not via a declared attribute); LinkML makes it explicit. Each
class gets a ``name`` slot (``identifier: true``, ``range: string``, ``required: true``).

.. code-block:: yaml

       name:
         identifier: true
         range: string
         required: true

The reader handles the ``identifier`` slot specially: it represents the object's hierarchy
name, not a declared attribute, so the reader never builds an ``AttributeSpec`` from it. No
``spec_type`` annotation is needed, because ``identifier: true`` already identifies this
slot (LinkML allows at most one identifier per class, and it is always this slot).

The ``name`` identifier slot is kept on every class. It is required for LinkML inlining of
data instances and for forward-compatibility with the Pydantic work; the reader does not
turn it into an ``AttributeSpec``.

dtypes
~~~~~~

HDMFSL dtypes map to LinkML ranges via a companion ``hdmf-linkml-types`` schema that
defines each dtype as its own ``TypeDefinition``, rather than collapsing to LinkML base
types. Each HDMFSL dtype is a distinct named type, so dtypes are differentiated by
**name**, not by their underlying ``typeof``: ``float32`` and ``float64`` are separate
named types even though both build on a LinkML base type, and a slot distinguishes them
with ``range: float32`` vs ``range: float64``. The reader recovers the exact HDMFSL dtype
from the range name. This is what keeps precision through the round-trip (``uint`` stays
``uint``, not a lossy ``integer``); ``typeof`` tracks the closest LinkML base type for
native LinkML consumers but does not carry the round-trip identity.

.. code-block:: yaml

   # in hdmf-linkml-types
   types:
     uint:
       typeof: integer
       minimum_value: 0
     int32:
       typeof: integer
     float32:
       typeof: float
     float64:
       typeof: double
     text:
       typeof: string
     # ... one per HDMFSL dtype

- A slot's ``range`` is the HDMFSL dtype name (e.g. ``range: uint``).
- A dataset or attribute with **no dtype** (the ``CSRMatrix`` ``data`` dataset) →
  ``range: AnyType``, where ``AnyType`` is a class with ``class_uri: linkml:Any``. The
  reader maps ``range: AnyType`` back to ``dtype = None``.

Compound and reference dtypes are deferred (see Out of scope).

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

- Attribute ``required`` (a boolean in HDMFSL) → slot ``required``.
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

``quantity`` (how many of the object) is independent of the array shape (the object's
dimensions): a single required array dataset is ``required: true``, ``multivalued: false``,
with an ``array`` expression.

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
URI convention for HDMF/NWB LinkML schemas will be settled with the LinkML team.

Worked example
--------------

base.yaml → base LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: base
   id: https://w3id.org/hdmf/test/base          # placeholder base URI (to be finalized)
   imports:
     - hdmf-linkml-types
   default_prefix: base
   classes:
     Data:
       description: An abstract data type for a dataset.
       annotations:
         spec_type: dataset
       attributes:
         name:
           identifier: true
           range: string
           required: true
     Container:
       description: An abstract data type for a group storing collections of data and metadata. Base type for all data and metadata containers.
       annotations:
         spec_type: group
       attributes:
         name:
           identifier: true
           range: string
           required: true
     SimpleMultiContainer:
       description: A simple Container for holding onto multiple containers.
       is_a: Container
       annotations:
         spec_type: group
       attributes:
         name:
           identifier: true
           range: string
           required: true
         data:                       # datasets: - data_type_inc: Data, quantity: '*'  (unnamed include)
           range: Data
           multivalued: true
           inlined_as_list: true
           annotations:
             spec_type: dataset
         container:                  # groups: - data_type_inc: Container, quantity: '*'  (unnamed include)
           range: Container
           multivalued: true
           inlined_as_list: true
           annotations:
             spec_type: group

sparse.yaml → sparse LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TBD

Test namespace.yaml → namespace LinkML schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   name: hdmf-common-test
   id: https://w3id.org/hdmf/test                # placeholder base URI (to be finalized)
   description: Minimal test namespace for the HDMF ↔ LinkML round-trip (base + sparse only).
   version: 0.1.0
   imports:
     - base
     - sparse
     - hdmf-linkml-types
   default_prefix: hdmf-common-test
   annotations:
     authors:                        # merged from namespace.yaml author + contact (one-to-one)
       value:
       - name: Andrew Tritt
         email: ajtritt@lbl.gov
       - name: Oliver Ruebel
         email: oruebel@lbl.gov
       - name: Ryan Ly
         email: rly@lbl.gov
       - name: Ben Dichter
         email: bdichter@lbl.gov

Out of scope
------------

- Dataset special cases: scalar-with-attributes, list-like datasets, class-range
  references.
- Compound dtypes; reference dtypes; links.
- The ``DynamicTable`` family (``VectorData``, ``VectorIndex``, ``DynamicTableRegion``,
  ragged arrays, inter-table references).
- Named includes (a ``data_type_inc`` entry that also fixes a ``name``).
- Inheritance roll-down (HDMFSL's recursive merging of parent fields into children).
- Cross-namespace imports.
- All LinkML features with no HDMFSL equivalent (enums, ontology URIs, rules,
  conditional/cross-field validation, mixins, abstract classes).
