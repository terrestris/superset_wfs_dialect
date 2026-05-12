import json
import time
from base64 import urlsafe_b64encode
from unittest.mock import MagicMock, patch

import pytest

from superset_wfs_dialect.oidc import OIDC, EXPIRATION_THRESHOLD_SECONDS


class TestOIDC:
    """Tests for the OIDC class."""

    @pytest.fixture
    def oauth2_client_info(self):
        """Fixture providing sample OAuth2 client information."""
        return {
            "id": "test-client-id",
            "secret": "test-client-secret",
            "scope": "test-scope",
            "token_request_uri": "https://example.com/oauth/token"
        }

    @pytest.fixture
    def sample_token_payload(self):
        """Fixture providing a sample JWT token payload."""
        current_time = int(time.time())
        return {
            "exp": current_time + 3600,  # Expires in 1 hour
        }

    @pytest.fixture
    def encoded_token(self, sample_token_payload):
        """Fixture providing an encoded JWT token (without signature verification)."""
        # Create a simple JWT-like structure: header.payload.signature
        header = {"alg": "RS256", "typ": "JWT"}
        header_encoded = urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip('=')
        payload_encoded = urlsafe_b64encode(json.dumps(sample_token_payload).encode()).decode().rstrip('=')
        signature = "fake-signature"
        return f"{header_encoded}.{payload_encoded}.{signature}"

    @patch('superset_wfs_dialect.oidc.OAuth2Session')
    def test_init_with_oauth2_client_info(self, mock_oauth_session, oauth2_client_info):
        """Test OIDC initialization with OAuth2 client info."""
        mock_client = MagicMock()
        mock_oauth_session.return_value = mock_client

        oidc = OIDC(oauth2_client_info=oauth2_client_info)

        assert oidc._oauth2_client_info == oauth2_client_info
        assert oidc._client == mock_client
        mock_oauth_session.assert_called_once_with(
            client_id="test-client-id",
            client_secret="test-client-secret",
            scope="test-scope"
        )

    @patch('superset_wfs_dialect.oidc.OAuth2Session')
    def test_create_oauth_client(self, mock_oauth_session, oauth2_client_info):
        """Test OAuth client creation."""
        mock_client = MagicMock()
        mock_oauth_session.return_value = mock_client

        oidc = OIDC()
        client = oidc._create_oauth_client(oauth2_client_info)

        assert client == mock_client
        mock_oauth_session.assert_called_once_with(
            client_id="test-client-id",
            client_secret="test-client-secret",
            scope="test-scope"
        )

    @patch('superset_wfs_dialect.oidc.OAuth2Session')
    def test_request_access_token(self, mock_oauth_session, oauth2_client_info, encoded_token, sample_token_payload):
        """Test requesting an access token."""
        mock_client = MagicMock()
        mock_client.fetch_token.return_value = {"access_token": encoded_token}
        mock_oauth_session.return_value = mock_client

        oidc = OIDC(oauth2_client_info=oauth2_client_info)
        oidc.request_access_token()

        mock_client.fetch_token.assert_called_once_with(
            "https://example.com/oauth/token",
            grant_type="client_credentials"
        )
        assert oidc._access_token == encoded_token
        assert oidc._decoded_access_token is not None
        assert oidc._decoded_access_token["exp"] == sample_token_payload["exp"]

    def test_naive_decode_token(self, encoded_token, sample_token_payload):
        """Test naive JWT token decoding."""
        oidc = OIDC()
        decoded = oidc.naive_decode_token(encoded_token)

        assert decoded["exp"] == sample_token_payload["exp"]

    def test_naive_decode_token_with_padding(self):
        """Test naive JWT token decoding with different padding scenarios."""
        oidc = OIDC()

        # Create a payload that requires padding
        payload = {"test": "data"}
        payload_encoded = urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
        token = f"header.{payload_encoded}.signature"

        decoded = oidc.naive_decode_token(token)
        assert decoded["test"] == "data"

    def test_expires_soon_no_token(self):
        """Test expires_soon when no token is set."""
        oidc = OIDC()

        assert oidc.expires_soon() is True

    def test_expires_soon_expired_token(self):
        """Test expires_soon with an expired token."""
        current_time = int(time.time())
        expired_payload = {
            "exp": current_time - 10,  # Expired 10 seconds ago
        }
        expired_token_encoded = urlsafe_b64encode(json.dumps(expired_payload).encode()).decode().rstrip('=')
        token = f"header.{expired_token_encoded}.signature"

        oidc = OIDC()
        oidc._decoded_access_token = oidc.naive_decode_token(token)

        assert oidc.expires_soon() is True

    def test_expires_soon_expiring_within_threshold(self):
        """Test expires_soon with a token expiring within the threshold."""
        current_time = int(time.time())
        expiring_payload = {
            "exp": current_time + EXPIRATION_THRESHOLD_SECONDS - 1,  # Expires within threshold
            "sub": "test"
        }
        expiring_token_encoded = urlsafe_b64encode(json.dumps(expiring_payload).encode()).decode().rstrip('=')
        token = f"header.{expiring_token_encoded}.signature"

        oidc = OIDC()
        oidc._decoded_access_token = oidc.naive_decode_token(token)

        assert oidc.expires_soon() is True

    def test_expires_soon_valid_token(self):
        """Test expires_soon with a valid token that won't expire soon."""
        current_time = int(time.time())
        valid_payload = {
            "exp": current_time + 3600,  # Expires in 1 hour
            "sub": "test"
        }
        valid_token_encoded = urlsafe_b64encode(json.dumps(valid_payload).encode()).decode().rstrip('=')
        token = f"header.{valid_token_encoded}.signature"

        oidc = OIDC()
        oidc._decoded_access_token = oidc.naive_decode_token(token)

        assert oidc.expires_soon() is False
