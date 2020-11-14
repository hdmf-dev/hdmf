from . import query  # noqa: F401
from .container import Container, Data, DataRegion
from .utils import docval, getargs


from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions
