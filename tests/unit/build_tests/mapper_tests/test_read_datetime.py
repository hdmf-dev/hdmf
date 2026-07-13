"""Tests for read-path datetime parsing.

When a builder's spec (or its matched_spec, which captures position-specific
overrides like inc-site dtype) declares dtype='isodatetime' or dtype='datetime',
the construct path parses stored ISO 8601 str/bytes back into Python
datetime/date objects.
"""

from datetime import date, datetime, timedelta, timezone

import numpy as np

from hdmf import Container, Data
from hdmf.build import BuildManager, DatasetBuilder, GroupBuilder, ObjectMapper, TypeMap
from hdmf.build.objectmapper import _parse_isoformat
from hdmf.spec import (
    AttributeSpec,
    DatasetSpec,
    GroupSpec,
    NamespaceCatalog,
    SpecCatalog,
    SpecNamespace,
)
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.helpers.utils import CORE_NAMESPACE


class StampedThing(Data):

    @property
    def data_type(self):
        return "StampedThing"


class TimestampedRecord(Container):

    @docval(
        {"name": "name", "type": str, "doc": "name"},
        {
            "name": "created_at",
            "type": (datetime, date, str),
            "doc": "when",
            "default": None,
        },
        {"name": "tag", "type": str, "doc": "a tag", "default": "t"},
        {
            "name": "stamped",
            "type": StampedThing,
            "doc": "a stamped child",
            "default": None,
        },
    )
    def __init__(self, **kwargs):
        name, created_at, tag, stamped = getargs("name", "created_at", "tag", "stamped", kwargs)
        super().__init__(name=name)
        self.__created_at = created_at
        self.__tag = tag
        self.__stamped = stamped
        if stamped is not None and stamped.parent is None:
            stamped.parent = self

    @property
    def data_type(self):
        return "TimestampedRecord"

    @property
    def created_at(self):
        return self.__created_at

    @property
    def tag(self):
        return self.__tag

    @property
    def stamped(self):
        return self.__stamped


_TYPE_TO_CLASS = {"TimestampedRecord": TimestampedRecord, "StampedThing": StampedThing}


def _build_type_map(parent_spec, child_specs=()):
    catalog = SpecCatalog()
    catalog.register_spec(parent_spec, "test.yaml")
    for s in child_specs:
        catalog.register_spec(s, "test.yaml")
    namespace = SpecNamespace(
        "a test namespace",
        CORE_NAMESPACE,
        [{"source": "test.yaml"}],
        version="0.1.0",
        catalog=catalog,
    )
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    namespace_catalog.resolve_all_specs()
    type_map = TypeMap(namespace_catalog)
    for s in [parent_spec] + list(child_specs):
        dt = getattr(s, "data_type_def", None)
        if dt is not None and dt in _TYPE_TO_CLASS:
            type_map.register_container_type(CORE_NAMESPACE, dt, _TYPE_TO_CLASS[dt])
    return type_map


def _construct(parent_spec, child_specs, parent_builder):
    type_map = _build_type_map(parent_spec, child_specs)
    manager = BuildManager(type_map)
    mapper = ObjectMapper(parent_spec)
    return mapper.construct(parent_builder, manager)


class TestReadAttributeDatetime(TestCase):

    def test_attribute_isodatetime_with_time(self):
        spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[
                AttributeSpec("created_at", "when", "isodatetime"),
                AttributeSpec("tag", "a tag", "text"),
            ],
        )
        gb = GroupBuilder(
            "r",
            attributes={
                "created_at": b"2024-03-15T12:30:00+00:00",
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(spec, [], gb)
        self.assertEqual(record.created_at, datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc))

    def test_attribute_isodatetime_z_designator(self):
        """A stored 'Z' (UTC) timestamp reads back equal to the '+00:00' form.

        Regression test for issue #1524: datetime.fromisoformat did not accept the
        'Z' designator until Python 3.11, so on the supported 3.10 floor a 'Z'
        timestamp (as written by many peer tools) raised ValueError on read, even
        though hdmf's own writer emits the equivalent '+00:00' offset.
        """
        spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[
                AttributeSpec("created_at", "when", "isodatetime"),
                AttributeSpec("tag", "a tag", "text"),
            ],
        )
        gb = GroupBuilder(
            "r",
            attributes={
                "created_at": b"2024-03-15T12:30:00Z",
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(spec, [], gb)
        self.assertEqual(record.created_at, datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc))

    def test_attribute_isodatetime_date_only(self):
        spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[
                AttributeSpec("created_at", "when", "isodatetime"),
                AttributeSpec("tag", "a tag", "text"),
            ],
        )
        gb = GroupBuilder(
            "r",
            attributes={
                "created_at": "2024-03-15",
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(spec, [], gb)
        self.assertEqual(record.created_at, date(2024, 3, 15))

    def test_attribute_non_datetime_dtype_unchanged(self):
        spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[AttributeSpec("tag", "a tag", "text")],
        )
        gb = GroupBuilder(
            "r",
            attributes={
                "tag": "2024-03-15",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(spec, [], gb)
        self.assertEqual(record.tag, "2024-03-15")

    def test_attribute_already_parsed_passes_through(self):
        """A value that has already been parsed (a datetime object) must not raise."""
        spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[
                AttributeSpec("created_at", "when", "isodatetime"),
                AttributeSpec("tag", "a tag", "text"),
            ],
        )
        already = datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc)
        gb = GroupBuilder(
            "r",
            attributes={
                "created_at": already,
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(spec, [], gb)
        self.assertEqual(record.created_at, already)


class TestReadDatasetDatetime(TestCase):

    def test_def_site_isodatetime_array(self):
        """A typed Data subclass whose def-site declares dtype='isodatetime' parses on construct."""
        child_def = DatasetSpec(
            doc="stamped",
            data_type_def="StampedThing",
            dtype="isodatetime",
            shape=(None,),
        )
        named_subspec = DatasetSpec(doc="stamped child", data_type_inc="StampedThing", name="stamped")
        parent_spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[AttributeSpec("tag", "a tag", "text")],
            datasets=[named_subspec],
        )

        child_db = DatasetBuilder(
            "stamped",
            np.array([b"2024-03-15T00:00:00+00:00", b"2024-04-01T12:00:00+00:00"]),
            attributes={
                "data_type": "StampedThing",
                "namespace": CORE_NAMESPACE,
                "object_id": "sid",
            },
        )
        gb = GroupBuilder(
            "r",
            datasets={"stamped": child_db},
            attributes={
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(parent_spec, [child_def], gb)
        self.assertEqual(
            record.stamped.data,
            [
                datetime(2024, 3, 15, tzinfo=timezone.utc),
                datetime(2024, 4, 1, 12, 0, tzinfo=timezone.utc),
            ],
        )

    def test_inc_site_dtype_override(self):
        """Child def has no dtype; parent's inc-site declares isodatetime.

        The parent's matcher records the inc-site subspec on the child builder's
        matched_spec, and the construct path honors that subspec's dtype on read.
        """
        child_def = DatasetSpec(doc="generic", data_type_def="StampedThing")
        col_subspec = DatasetSpec(
            doc="date column",
            data_type_inc="StampedThing",
            name="stamped",
            dtype="isodatetime",
        )
        parent_spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[AttributeSpec("tag", "a tag", "text")],
            datasets=[col_subspec],
        )

        child_db = DatasetBuilder(
            "stamped",
            ["2020-01-01", "2021-06-15"],
            attributes={
                "data_type": "StampedThing",
                "namespace": CORE_NAMESPACE,
                "object_id": "sid",
            },
        )
        gb = GroupBuilder(
            "r",
            datasets={"stamped": child_db},
            attributes={
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(parent_spec, [child_def], gb)
        self.assertEqual(record.stamped.data, [date(2020, 1, 1), date(2021, 6, 15)])

    def test_non_datetime_dtype_unchanged(self):
        """A typed dataset with an unrelated dtype is not mutated by the parser."""
        child_def = DatasetSpec(doc="stamped", data_type_def="StampedThing", dtype="int", shape=(None,))
        named_subspec = DatasetSpec(doc="child", data_type_inc="StampedThing", name="stamped")
        parent_spec = GroupSpec(
            doc="record",
            data_type_def="TimestampedRecord",
            attributes=[AttributeSpec("tag", "a tag", "text")],
            datasets=[named_subspec],
        )

        ints = [1, 2, 3]
        child_db = DatasetBuilder(
            "stamped",
            ints,
            attributes={
                "data_type": "StampedThing",
                "namespace": CORE_NAMESPACE,
                "object_id": "sid",
            },
        )
        gb = GroupBuilder(
            "r",
            datasets={"stamped": child_db},
            attributes={
                "tag": "t",
                "data_type": "TimestampedRecord",
                "namespace": CORE_NAMESPACE,
                "object_id": "rid",
            },
        )
        record = _construct(parent_spec, [child_def], gb)
        self.assertEqual(list(record.stamped.data), ints)


class TestParseIsoformatZDesignator(TestCase):
    """Directly exercise ``_parse_isoformat`` normalization of the 'Z' (UTC) designator.

    ``datetime.fromisoformat`` did not accept a trailing 'Z' until Python 3.11
    (bpo-35829), yet hdmf's writer emits the equivalent '+00:00' offset, so a
    'Z'-terminated timestamp from a peer tool was unreadable on the supported 3.10
    floor. These assertions hold on every supported Python version and guard the
    fix for issue #1524. Crucially, asserting that the 'Z' and '+00:00' forms parse
    to *equal* tz-aware UTC datetimes is a cross-version check: it fails on
    Python < 3.11 without the fix and passes on all versions with it.
    """

    def test_z_equals_offset_and_is_utc(self):
        z = _parse_isoformat("2026-07-02T08:27:27.216166Z")
        offset = _parse_isoformat("2026-07-02T08:27:27.216166+00:00")
        self.assertEqual(z, offset)
        self.assertEqual(z.tzinfo, timezone.utc)
        self.assertEqual(z.utcoffset(), timedelta(0))

    def test_z_without_microseconds(self):
        self.assertEqual(
            _parse_isoformat("2024-03-15T12:30:00Z"),
            datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc),
        )

    def test_lowercase_z(self):
        # RFC 3339 permits a lowercase 'z'; stdlib fromisoformat accepts neither the
        # lowercase 'z' (any version) nor an uppercase 'Z' before 3.11, so normalize both.
        self.assertEqual(
            _parse_isoformat("2024-03-15T12:30:00z"),
            datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc),
        )

    def test_bytes_z(self):
        self.assertEqual(
            _parse_isoformat(b"2024-03-15T12:30:00Z"),
            datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc),
        )

    def test_explicit_offset_unchanged(self):
        # A real UTC offset (what hdmf writes) and a non-UTC offset must be untouched.
        self.assertEqual(
            _parse_isoformat("2024-03-15T12:30:00+00:00"),
            datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            _parse_isoformat("2024-03-15T12:30:00-05:00"),
            datetime(2024, 3, 15, 12, 30, tzinfo=timezone(timedelta(hours=-5))),
        )

    def test_naive_string_unchanged(self):
        # No trailing 'Z' => still naive; normalization must not attach a tzinfo.
        result = _parse_isoformat("2024-03-15T12:30:00")
        self.assertEqual(result, datetime(2024, 3, 15, 12, 30))
        self.assertIsNone(result.tzinfo)
