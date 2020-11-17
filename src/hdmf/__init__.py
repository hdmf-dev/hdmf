from . import query  # noqa: F401
from ._version import get_versions  # noqa: E402
from .container import Container, Data, DataRegion
from .utils import docval, getargs

__version__ = get_versions()['version']
del get_versions
