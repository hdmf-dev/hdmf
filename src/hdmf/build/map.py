# this prevents breaking of code that imports these classes directly from map.py
from .manager import Proxy, BuildManager, TypeSource, TypeMap  # noqa: F401
from .objectmapper import ObjectMapper  # noqa: F401

import warnings
warnings.warn(PendingDeprecationWarning('Classes in map.py should be imported from hdmf.build. Importing from '
                                        'hdmf.build.map is subject to change in HDMF 2.0.'))
