from . import query
from .backends.hdf5.h5_utils import H5Dataset, H5RegionSlicer
from .container import Container, Data, DataRegion, ExternalResourcesManager
from .region import ListSlicer
from .utils import docval, getargs


@docval(
    {"name": "dataset", "type": None, "doc": "the HDF5 dataset to slice"},
    {"name": "region", "type": None, "doc": "the region reference to use to slice"},
    is_method=False,
)
def get_region_slicer(**kwargs):
    import warnings  # noqa: E402

    warnings.warn(
        "get_region_slicer is deprecated and will be removed in HDMF 3.0.",
        DeprecationWarning,
    )

    dataset, region = getargs("dataset", "region", kwargs)
    if isinstance(dataset, (list, tuple, Data)):
        return ListSlicer(dataset, region)
    elif isinstance(dataset, H5Dataset):
        return H5RegionSlicer(dataset, region)
    return None


try:
    from importlib.metadata import version  # noqa: E402
except ImportError:
    # TODO: Remove when python 3.8 becomes the new minimum
    from importlib_metadata import version  # noqa: E402

__version__ = version(__package__)
del version


from ._due import BibTeX, due  # noqa: E402

due.cite(
    BibTeX("""
@INPROCEEDINGS{9005648,
  author={A. J. {Tritt} and O. {Rübel} and B. {Dichter} and R. {Ly} and D. {Kang} and E. F. {Chang} and L. M. {Frank} and K. {Bouchard}},
  booktitle={2019 IEEE International Conference on Big Data (Big Data)},
  title={HDMF: Hierarchical Data Modeling Framework for Modern Science Data Standards},
  year={2019},
  volume={},
  number={},
  pages={165-179},
  doi={10.1109/BigData47090.2019.9005648}}
"""),  # noqa: E501
    description="HDMF: Hierarchical Data Modeling Framework for Modern Science Data Standards",
    path="hdmf/",
    version=__version__,
    cite_module=True,
)
del due, BibTeX
