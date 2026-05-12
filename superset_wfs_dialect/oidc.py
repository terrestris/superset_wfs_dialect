from base64 import urlsafe_b64decode
import time
import json

from authlib.integrations.requests_client import OAuth2Session

EXPIRATION_THRESHOLD_SECONDS = 5

class OIDC():

    def __init__(self, oauth2_client_info=None):
        self._oauth2_client_info = oauth2_client_info
        self._client = None
        self._access_token = None
        self._decoded_access_token = None

        if oauth2_client_info is not None:
            self._client = self._create_oauth_client(oauth2_client_info)

    def _create_oauth_client(self, oauth2_client_info):
        client = OAuth2Session(
            client_id=oauth2_client_info.get("id"),
            client_secret=oauth2_client_info.get("secret"),
            scope=oauth2_client_info.get("scope"),
        )
        return client

    def request_access_token(self):
        token_url = self._oauth2_client_info.get("token_request_uri")
        token = self._client.fetch_token(token_url, grant_type="client_credentials")
        self._access_token = token.get("access_token")
        self._decoded_access_token = self.naive_decode_token(self._access_token)

    def get_access_token(self):
        return self._access_token

    def get_decoded_access_token(self):
        return self._decoded_access_token

    def get_client(self):
        return self._client

    def naive_decode_token(self, token):
        """Naively decode the JWT access token to extract the payload without verifying the signature.
           This should be ok, since we fetch the token directly from the provider and only
           read the expiration time to manage token refreshing.
        """
        payload = token.split(".")[1]
        padding = '=' * (-len(payload) % 4)
        decoded_token = urlsafe_b64decode(payload + padding)
        return json.loads(decoded_token)

    def expires_soon(self):
        if self._decoded_access_token is None:
            return True
        expires_at = self._decoded_access_token.get("exp")
        current_time = int(time.time())
        return expires_at - current_time < EXPIRATION_THRESHOLD_SECONDS
