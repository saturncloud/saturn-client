"""
imports added so users do not have to think about submodules
"""

from .core import SaturnConnection  # noqa: F401
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from . import _version
__version__ = _version.get_versions()['version']
