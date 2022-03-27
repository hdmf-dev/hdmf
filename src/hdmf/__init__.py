from . import query  # noqa: F401
from .container import Container, Data, DataRegion
from .utils import docval, getargs
from .region import ListSlicer
from .backends.hdf5.h5_utils import H5RegionSlicer, H5Dataset

from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions


from ._due import due, BibTeX  # noqa: E402
due.cite(BibTeX("""
@INPROCEEDINGS{9005648,
  author={A. J. {Tritt} and O. {Rübel} and B. {Dichter} and R. {Ly} and D. {Kang} and E. F. {Chang} and L. M. {Frank} and K. {Bouchard}},
  booktitle={2019 IEEE International Conference on Big Data (Big Data)},
  title={HDMF: Hierarchical Data Modeling Framework for Modern Science Data Standards},
  year={2019},
  volume={},
  number={},
  pages={165-179},
  doi={10.1109/BigData47090.2019.9005648}}
"""), description="HDMF: Hierarchical Data Modeling Framework for Modern Science Data Standards",  # noqa: E501
         path="hdmf/", version=__version__, cite_module=True)
del due, BibTeX
