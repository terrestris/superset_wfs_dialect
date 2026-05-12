from unittest.mock import MagicMock, patch

import pytest

from superset_wfs_dialect.wfs_oauth import WfsOauth


class TestWfsOauth:
    """Tests for the WfsOauth class."""

    @pytest.fixture
    def oauth_info(self):
        """Fixture providing sample OAuth information."""
        return {
            "id": "test-client-id",
            "secret": "test-client-secret",
            "scope": "test-scope",
            "token_request_uri": "https://example.com/oauth/token"
        }

    @pytest.fixture
    def sample_capabilities_xml(self):
        """Fixture providing sample WFS capabilities XML."""
        return """<?xml version="1.0" encoding="UTF-8"?>
        <WFS_Capabilities version="2.0.0">
            <ServiceIdentification>
                <Title>Test WFS</Title>
            </ServiceIdentification>
        </WFS_Capabilities>"""

    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_init_with_oauth_info(self, mock_oidc_class, mock_reader_class, oauth_info):
        """Test WfsOauth initialization with OAuth info."""
        # Setup mocks
        mock_oidc_instance = MagicMock()
        mock_oidc_instance.expires_soon.return_value = False
        mock_oidc_instance.get_access_token.return_value = "test-token"
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        # Initialize WfsOauth
        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0",
            oauth_info=oauth_info
        )

        # Verify OIDC was initialized with oauth_info
        mock_oidc_class.assert_called_once_with(oauth2_client_info=oauth_info)

        # Verify attributes
        assert wfs.url == "https://example.com/wfs"
        assert wfs.version == "2.0.0"
        assert wfs.oidc == mock_oidc_instance
        assert "Authorization" in wfs.headers
        assert wfs.headers["Authorization"] == "Bearer test-token"

    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_init_with_custom_headers(self, mock_oidc_class, mock_reader_class):
        """Test WfsOauth initialization with custom headers."""
        mock_oidc_instance = MagicMock()
        mock_oidc_instance.expires_soon.return_value = False
        mock_oidc_instance.get_access_token.return_value = None
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        custom_headers = {"User-Agent": "TestAgent"}
        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0",
            headers=custom_headers
        )

        assert "User-Agent" in wfs.headers
        assert wfs.headers["User-Agent"] == "TestAgent"

    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_inject_access_token_when_expires_soon(self, mock_oidc_class, mock_reader_class):
        """Test inject_access_token when token expires soon."""
        mock_oidc_instance = MagicMock()
        # First call returns True (expires soon), second call returns False
        mock_oidc_instance.expires_soon.side_effect = [True, True]
        mock_oidc_instance.get_access_token.side_effect = [None, "new-token"]
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0"
        )

        # Call inject_access_token again
        wfs.inject_access_token()

        # Verify request_access_token was called
        assert mock_oidc_instance.request_access_token.call_count == 2
        assert wfs.headers["Authorization"] == "Bearer new-token"

    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_inject_access_token_when_not_expires_soon(self, mock_oidc_class, mock_reader_class):
        """Test inject_access_token when token doesn't expire soon."""
        mock_oidc_instance = MagicMock()
        mock_oidc_instance.expires_soon.side_effect = [False, False]
        mock_oidc_instance.get_access_token.return_value = "existing-token"
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0"
        )

        # Reset call count from init
        mock_oidc_instance.request_access_token.reset_mock()

        # Call inject_access_token again
        wfs.inject_access_token()

        # Verify request_access_token was NOT called
        mock_oidc_instance.request_access_token.assert_not_called()
        assert wfs.headers["Authorization"] == "Bearer existing-token"

    @patch('superset_wfs_dialect.wfs_oauth.get_schema')
    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_get_schema(self, mock_oidc_class, mock_reader_class, mock_get_schema):
        """Test get_schema method."""
        mock_oidc_instance = MagicMock()
        mock_oidc_instance.expires_soon.side_effect = [False, False]
        mock_oidc_instance.get_access_token.return_value = "test-token"
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        mock_schema = {"properties": {"name": "string"}}
        mock_get_schema.return_value = mock_schema

        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0"
        )

        # Call get_schema
        result = wfs.get_schema("test:layer")

        # Verify inject_access_token was called (expires_soon called twice: init + get_schema)
        assert mock_oidc_instance.expires_soon.call_count == 2

        # Verify get_schema was called with correct parameters
        mock_get_schema.assert_called_once_with(
            "https://example.com/wfs",
            "test:layer",
            "2.0.0",
            headers=wfs.headers,
            auth=wfs.auth
        )
        assert result == mock_schema

    @patch('superset_wfs_dialect.wfs_oauth.WFSCapabilitiesReader')
    @patch('superset_wfs_dialect.wfs_oauth.OIDC')
    def test_getfeature(self, mock_oidc_class, mock_reader_class):
        """Test getfeature method."""
        mock_oidc_instance = MagicMock()
        mock_oidc_instance.expires_soon.side_effect = [False, False]
        mock_oidc_instance.get_access_token.return_value = "test-token"
        mock_oidc_class.return_value = mock_oidc_instance

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = MagicMock()
        mock_reader_class.return_value = mock_reader_instance

        wfs = WfsOauth(
            url="https://example.com/wfs",
            version="2.0.0"
        )

        # Mock the parent class getfeature method
        with patch.object(wfs.__class__.__bases__[0], 'getfeature', return_value="feature_response") as mock_parent_getfeature:
            result = wfs.getfeature(typename="test:layer", maxfeatures=10)

            # Verify inject_access_token was called
            assert mock_oidc_instance.expires_soon.call_count == 2

            # Verify parent getfeature was called with correct parameters
            mock_parent_getfeature.assert_called_once_with(typename="test:layer", maxfeatures=10)
            assert result == "feature_response"
