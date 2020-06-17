import os
import requests
import base64


class CredentialError(Exception):
    def __init__(self):
        self.message = "Authorization Failed: Verify CLIENT_ID and CLIENT_SECRET are set in the environment"
        super().__init__(self.message)


class Whetstone:
    def __init__(self, qa=False):
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        if qa:
            subdomain = "api-qa"
        else:
            subdomain = "api"
        self.url = f"https://{subdomain}.whetstoneeducation.com"
        self.token = self._authorize()

    def _authorize(self):
        auth_url = f"{self.url}/auth/client/token"
        headers = {"Authorization": self._encode_credentials()}
        response = requests.post(auth_url, headers=headers)

        if response.status_code == 200:
            response_json = response.json()
            return response_json["access_token"]
        else:
            raise CredentialError

    def _encode_credentials(self):
        if self.client_id is None or self.client_secret is None:
            raise CredentialError
        client_credential_string = self.client_id + ":" + self.client_secret
        encoded_credentials = base64.b64encode(client_credential_string.encode("utf-8"))
        encoded_credentials_string = str(encoded_credentials, "utf-8")
        return "Basic " + encoded_credentials_string

    def get_all(self, endpoint):
        endpoint_url = f"{self.url}/external/{endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(endpoint_url, headers=headers)
        if response.status_code == 200:
            response_json = response.json()
            return response_json["data"]
        else:
            raise Exception(f"Failed to list {endpoint}")
