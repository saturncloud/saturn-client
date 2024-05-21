"""
Settings used for interacting with Saturn
"""

import os

from typing import Optional
from urllib.parse import urlparse

from ._version import get_versions

__version__ = get_versions()["version"]


class Settings:
    """Global settings"""

    BASE_URL: str
    SATURN_TOKEN: str
    REFRESH_TOKEN: Optional[str] = None
    WORKING_DIRECTORY: str = "/home/jovyan/workspace"

    def __init__(
        self,
        base_url: Optional[str] = None,
        saturn_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ):
        if base_url:
            self.BASE_URL = base_url
        else:
            try:
                self.BASE_URL = os.environ["SATURN_BASE_URL"]
            except KeyError as err:
                err_msg = "Missing required value Saturn url."
                raise RuntimeError(err_msg) from err

        parsed = urlparse(self.BASE_URL)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f'"{self.BASE_URL}" is not a valid URL')

        if saturn_token:
            self.SATURN_TOKEN = saturn_token
        else:
            try:
                self.SATURN_TOKEN = os.environ["SATURN_TOKEN"]
            except KeyError as err:
                err_msg = "Missing required value Saturn api token."
                raise RuntimeError(err_msg) from err

        if refresh_token:
            self.REFRESH_TOKEN = refresh_token
        else:
            self.REFRESH_TOKEN = os.getenv("SATURN_REFRESH_TOKEN")

    def update_tokens(
        self, access_token: Optional[str] = None, refresh_token: Optional[str] = None
    ):
        if access_token:
            self.SATURN_TOKEN = access_token
            if "SATURN_TOKEN" in os.environ:
                os.environ["SATURN_TOKEN"] = access_token
        if refresh_token:
            self.REFRESH_TOKEN = refresh_token
            if "SATURN_REFRESH_TOKEN" in os.environ:
                os.environ["SATURN_REFRESH_TOKEN"] = refresh_token

    @property
    def url(self):
        """Saturn url"""
        return self.BASE_URL

    @property
    def headers(self):
        """Saturn auth headers including saturn-client version"""
        return {
            "Authorization": f"token {self.SATURN_TOKEN}",
            "X-Saturn-Client-Version": __version__,
        }
