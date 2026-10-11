"""Tests for casting scalar numeric attributes to their spec dtype on read and on write.

h5py returns numeric attributes as numpy scalars, but backends that store attributes as JSON, such as Zarr,
return Python scalars. On read, a Python int, float, or bool is cast to the numpy type of the spec dtype.
"""

import math

import numpy as np

from hdmf.build import BuildManager, GroupBuilder
from hdmf.spec import AttributeSpec, GroupSpec
from hdmf.testing import TestCase

from tests.unit.helpers.utils import CORE_NAMESPACE, create_test_type_map


class TestScalarAttributeDtype(TestCase):

    def setUp(self):
        spec = GroupSpec(
            doc="a group with scalar attributes",
            data_type_def="Params",
            attributes=[
                AttributeSpec(name="a_int64", doc="an int64 attribute", dtype="int64", required=False),
                AttributeSpec(name="a_long", doc="a long attribute", dtype="long", required=False),
                AttributeSpec(name="a_int32", doc="an int32 attribute", dtype="int32", required=False),
                AttributeSpec(name="a_uint8", doc="a uint8 attribute", dtype="uint8", required=False),
                AttributeSpec(name="a_float32", doc="a float32 attribute", dtype="float32", required=False),
                AttributeSpec(name="a_float64", doc="a float64 attribute", dtype="float64", required=False),
                AttributeSpec(name="a_bool", doc="a bool attribute", dtype="bool", required=False),
                AttributeSpec(name="a_text", doc="a text attribute", dtype="text", required=False),
                AttributeSpec(name="a_array", doc="an array attribute", dtype="int64", shape=(None,),
                              required=False),
            ],
        )
        self.type_map = create_test_type_map([spec], {})
        self.cls = self.type_map.get_dt_container_cls("Params", CORE_NAMESPACE)

    def _read(self, **attributes):
        attributes.update(namespace=CORE_NAMESPACE, data_type="Params")
        builder = GroupBuilder("params", attributes=attributes)
        return BuildManager(self.type_map).construct(builder)

    def assertTypedEqual(self, value, expected):
        self.assertIs(type(value), type(expected))
        self.assertEqual(value, expected)

    def test_read_python_int_as_int64(self):
        container = self._read(a_int64=3, a_long=4)
        self.assertTypedEqual(container.a_int64, np.int64(3))
        self.assertTypedEqual(container.a_long, np.int64(4))

    def test_read_python_int_as_spec_dtype(self):
        container = self._read(a_int32=3, a_uint8=255)
        self.assertTypedEqual(container.a_int32, np.int32(3))
        self.assertTypedEqual(container.a_uint8, np.uint8(255))

    def test_read_python_float_as_spec_dtype(self):
        container = self._read(a_float32=0.5, a_float64=0.1)
        self.assertTypedEqual(container.a_float32, np.float32(0.5))
        self.assertTypedEqual(container.a_float64, np.float64(0.1))

    def test_read_python_bool(self):
        container = self._read(a_bool=True)
        self.assertTypedEqual(container.a_bool, np.bool_(True))

    def test_read_numpy_scalar_unchanged(self):
        """A numpy scalar, as h5py returns, is passed through as stored."""
        container = self._read(a_int64=np.int64(3), a_int32=np.int64(5))
        self.assertTypedEqual(container.a_int64, np.int64(3))
        self.assertTypedEqual(container.a_int32, np.int64(5))

    def test_read_value_that_does_not_fit_unchanged(self):
        """A value that the spec dtype cannot hold exactly is returned unchanged."""
        container = self._read(a_int32=2**40, a_float32=0.1)
        self.assertTypedEqual(container.a_int32, 2**40)
        self.assertTypedEqual(container.a_float32, 0.1)

    def test_read_nan_unchanged(self):
        container = self._read(a_float64=float("nan"))
        self.assertIs(type(container.a_float64), float)
        self.assertTrue(math.isnan(container.a_float64))

    def test_read_text_and_array_unchanged(self):
        container = self._read(a_text="3", a_array=[1, 2])
        self.assertTypedEqual(container.a_text, "3")
        self.assertEqual(container.a_array, [1, 2])

    def test_construct_with_python_int_for_int64(self):
        container = self.cls(name="params", a_int64=3, a_long=4)
        self.assertEqual(container.a_int64, 3)
        self.assertEqual(container.a_long, 4)

    def test_write_python_int_as_int64(self):
        container = self.cls(name="params", a_int64=3, a_long=4)
        builder = BuildManager(self.type_map).build(container)
        self.assertTypedEqual(builder.attributes["a_int64"], np.int64(3))
        self.assertTypedEqual(builder.attributes["a_long"], np.int64(4))
