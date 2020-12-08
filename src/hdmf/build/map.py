# this prevents breaking of code that imports these classes directly from map.py
from .manager import Proxy, BuildManager, TypeSource, TypeMap  # noqa: F401
from .objectmapper import ObjectMapper  # noqa: F401

import warnings
warnings.warn('Classes in map.py should be imported from hdmf.build. Importing from hdmf.build.map will be removed '
              'in HDMF 3.0.', DeprecationWarning)
