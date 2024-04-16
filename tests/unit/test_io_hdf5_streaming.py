from copy import copy, deepcopy
import os
import urllib.request
import h5py

from hdmf.build import TypeMap, BuildManager
from hdmf.common import get_hdf5io, get_type_map
from hdmf.spec import GroupSpec, DatasetSpec, SpecNamespace, NamespaceBuilder, NamespaceCatalog
from hdmf.testing import TestCase
from hdmf.utils import docval, get_docval


class TestRos3(TestCase):
    """Test reading an HDMF file using HDF5 ROS3 streaming.

    TODO: test streaming via fsspec/h5py
    """

    def setUp(self):
        # Skip ROS3 tests if internet is not available or the ROS3 driver is not installed
        try:
            # this is a 174 KB file
            urllib.request.urlopen("https://dandiarchive.s3.amazonaws.com/ros3test.nwb", timeout=1)
        except urllib.request.URLError:
            self.skipTest("Internet access to DANDI failed. Skipping all Ros3 streaming tests.")
        if "ros3" not in h5py.registered_drivers():
            self.skipTest("ROS3 driver not installed. Skipping all Ros3 streaming tests.")

        # set up build manager with a simplified version of the NWB schema so that we can test
        # ROS3 streaming from S3
        namespace_name = "core"
        self.ns_filename = namespace_name + ".namespace.yaml"
        self.ext_filename = namespace_name + ".extensions.yaml"
        self.output_dir = "."
        nwb_container_spec = NWBGroupSpec(
            neurodata_type_def="NWBContainer",
            neurodata_type_inc="Container",
            doc=("An abstract data type for a generic container storing collections of data and metadata. "
                 "Base type for all data and metadata containers."),
        )
        subject_spec = NWBGroupSpec(
            neurodata_type_def="Subject",
            neurodata_type_inc="NWBContainer",
            doc="Information about the animal or person from which the data was measured.",
        )
        nwbfile_spec = NWBGroupSpec(
            neurodata_type_def="NWBFile",
            neurodata_type_inc="NWBContainer",
            doc="An NWB file storing cellular-based neurophysiology data from a single experimental session.",
            groups=[
                NWBGroupSpec(
                    name="subject",
                    neurodata_type_inc="Subject",
                    doc="Information about the animal or person from which the data was measured.",
                    quantity="?",
                ),
            ],
        )

        ns_builder = NamespaceBuilder(
            name=namespace_name,
            doc="a test namespace",
            version="0.1.0",
        )
        ns_builder.include_namespace("hdmf-common")
        ns_builder.add_spec(self.ext_filename, nwb_container_spec)
        ns_builder.add_spec(self.ext_filename, subject_spec)
        ns_builder.add_spec(self.ext_filename, nwbfile_spec)

        ns_builder.export(self.ns_filename, outdir=self.output_dir)
        ns_path = os.path.join(self.output_dir, self.ns_filename)

        ns_catalog = NamespaceCatalog(NWBGroupSpec, NWBDatasetSpec, NWBNamespace)
        type_map = TypeMap(ns_catalog)
        type_map.merge(get_type_map(), ns_catalog=True)
        type_map.load_namespaces(ns_path)

        self.manager = BuildManager(type_map)

    def tearDown(self):
        if os.path.exists(self.ns_filename):
            os.remove(self.ns_filename)
        if os.path.exists(self.ext_filename):
            os.remove(self.ext_filename)

    def test_basic_read(self):
        s3_path = "https://dandiarchive.s3.amazonaws.com/blobs/11e/c89/11ec8933-1456-4942-922b-94e5878bb991"

        with get_hdf5io(s3_path, "r", manager=self.manager, driver="ros3") as io:
            io.read()

# Util functions and classes to enable loading of the NWB namespace -- see pynwb/src/pynwb/spec.py


def __swap_inc_def(cls):
    args = get_docval(cls.__init__)
    clsname = "NWB%s" % cls.__name__
    ret = list()
    # do not set default neurodata_type_inc for base hdmf-common types that should not have data_type_inc
    for arg in args:
        if arg["name"] == "data_type_def":
            ret.append({"name": "neurodata_type_def", "type": str,
                        "doc": "the NWB data type this spec defines", "default": None})
        elif arg["name"] == "data_type_inc":
            ret.append({"name": "neurodata_type_inc", "type": (clsname, str),
                        "doc": "the NWB data type this spec includes", "default": None})
        else:
            ret.append(copy(arg))
    return ret


class BaseStorageOverride:
    """ This class is used for the purpose of overriding
        BaseStorageSpec classmethods, without creating diamond
        inheritance hierarchies.
    """

    __type_key = "neurodata_type"
    __inc_key = "neurodata_type_inc"
    __def_key = "neurodata_type_def"

    @classmethod
    def type_key(cls):
        """ Get the key used to store data type on an instance"""
        return cls.__type_key

    @classmethod
    def inc_key(cls):
        """ Get the key used to define a data_type include."""
        return cls.__inc_key

    @classmethod
    def def_key(cls):
        """ Get the key used to define a data_type definition."""
        return cls.__def_key

    @property
    def neurodata_type_inc(self):
        return self.data_type_inc

    @property
    def neurodata_type_def(self):
        return self.data_type_def

    @classmethod
    def build_const_args(cls, spec_dict):
        """Extend base functionality to remap data_type_def and data_type_inc keys"""
        spec_dict = copy(spec_dict)
        proxy = super()
        if proxy.inc_key() in spec_dict:
            spec_dict[cls.inc_key()] = spec_dict.pop(proxy.inc_key())
        if proxy.def_key() in spec_dict:
            spec_dict[cls.def_key()] = spec_dict.pop(proxy.def_key())
        ret = proxy.build_const_args(spec_dict)
        return ret

    @classmethod
    def _translate_kwargs(cls, kwargs):
        """Swap neurodata_type_def and neurodata_type_inc for data_type_def and data_type_inc, respectively"""
        proxy = super()
        kwargs[proxy.def_key()] = kwargs.pop(cls.def_key())
        kwargs[proxy.inc_key()] = kwargs.pop(cls.inc_key())
        return kwargs


_dataset_docval = __swap_inc_def(DatasetSpec)


class NWBDatasetSpec(BaseStorageOverride, DatasetSpec):
    """ The Spec class to use for NWB dataset specifications.

    Classes will automatically include NWBData if None is specified.
    """

    @docval(*deepcopy(_dataset_docval))
    def __init__(self, **kwargs):
        kwargs = self._translate_kwargs(kwargs)
        # set data_type_inc to NWBData only if it is not specified and the type is not an HDMF base type
        if kwargs["data_type_inc"] is None and kwargs["data_type_def"] not in (None, "Data"):
            kwargs["data_type_inc"] = "NWBData"
        super().__init__(**kwargs)


_group_docval = __swap_inc_def(GroupSpec)


class NWBGroupSpec(BaseStorageOverride, GroupSpec):
    """ The Spec class to use for NWB group specifications.

    Classes will automatically include NWBContainer if None is specified.
    """

    @docval(*deepcopy(_group_docval))
    def __init__(self, **kwargs):
        kwargs = self._translate_kwargs(kwargs)
        # set data_type_inc to NWBData only if it is not specified and the type is not an HDMF base type
        # NOTE: CSRMatrix in hdmf-common-schema does not have a data_type_inc but should not inherit from
        # NWBContainer. This will be fixed in hdmf-common-schema 1.2.1.
        if kwargs["data_type_inc"] is None and kwargs["data_type_def"] not in (None, "Container", "CSRMatrix"):
            kwargs["data_type_inc"] = "NWBContainer"
        super().__init__(**kwargs)

    @classmethod
    def dataset_spec_cls(cls):
        return NWBDatasetSpec

    @docval({"name": "neurodata_type", "type": str, "doc": "the neurodata_type to retrieve"})
    def get_neurodata_type(self, **kwargs):
        """ Get a specification by "neurodata_type" """
        return super().get_data_type(kwargs["neurodata_type"])


class NWBNamespace(SpecNamespace):
    """
    A Namespace class for NWB
    """

    __types_key = "neurodata_types"

    @classmethod
    def types_key(cls):
        return cls.__types_key
