import logging

from owslib.feature import WebFeatureService_
from owslib.feature.common import WFSCapabilitiesReader
from owslib.feature.schema import get_schema
from owslib.util import Authentication


from .custom_wfs200 import WebFeatureService_2_0_0
from .oidc import OIDC


LOGGER = logging.getLogger(__name__)


class WfsOauth(WebFeatureService_2_0_0):

    def __init__(
        self,
        url,
        version,
        xml=None,
        parse_remote_metadata=False,
        timeout=30,
        headers=None,
        oauth_info=None,
    ):
        """Initialize."""
        super(WebFeatureService_, self).__init__()
        self.auth = Authentication()
        LOGGER.debug("building WFS %s" % url)
        self.url = url
        self.version = version
        self.timeout = timeout
        self.headers = headers
        if self.headers is None:
            self.headers = {}
        self._capabilities = None

        self.oidc = OIDC(oauth2_client_info=oauth_info)

        self.inject_access_token()
        reader = WFSCapabilitiesReader(self.version, headers=self.headers, auth=self.auth)
        if xml:
            self._capabilities = reader.readString(xml)
        else:
            self._capabilities = reader.read(self.url, self.timeout)
        self._buildMetadata(parse_remote_metadata)


    def inject_access_token(self):
        if self.oidc.expires_soon():
            LOGGER.debug("Access token is expiring soon, refreshing...")
            self.oidc.request_access_token()
        access_token = self.oidc.get_access_token()
        if access_token:
            self.headers["Authorization"] = f"Bearer {access_token}"

    def get_schema(self, typename):
        """
        Get layer schema compatible with :class:`fiona` schema object
        """
        self.inject_access_token()
        return get_schema(self.url, typename, self.version, headers=self.headers, auth=self.auth)

    def getfeature(self, *args, **kwargs):
        self.inject_access_token()
        return super().getfeature(*args, **kwargs)
