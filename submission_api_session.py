"""
Creates a service to enable automated refreshing of the SubmissionAPI token. 

To fetch the token we need to execute this curl comamnd. 

curl -X POST "http://localhost:8085/realms/Dare-Control/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=Dare-Control-API" \
  -d "client_secret=2e60b956-16bc-4dea-8b49-118a8baac5e5" \
  -d "username=globaladminuser" \
  -d "password=password123" \
  -d "grant_type=password"



Design Outline 
---------------

1. Handle the login and auth process as a context manager which clearly defines a scope for the session. 
2. Rather than use a background process routed via multi-threading we instead have the session object check if the token 
   has expired before making a request, is so, then refresh the token. 
3. Or an even simpler logic: 
    * Do your network request
    * Was there an error with the access token...? (Or keeping time to expiry variable cached)
    * If so get a refresh access token.
    * Retry the request with an access token.

Changes 
- MinioClient needs to be modfied to accept a token session rather than a static token string. 
- AnalysisRunner constructor needs to be modified to except a token session rather than a static string. 
- Main code block on run_analysis needs to use the token session context manger functionality.
"""
import time 
import threading 

import requests 


CLIENT_ID = "Dare-Control-API"
CLIENT_SECRET = "2e60b956-16bc-4dea-8b49-118a8baac5e5"
USERNAME = "globaladminuser"
PASSWORD = "password123"
TOKEN_URL = "http://localhost:8085/realms/Dare-Control/protocol/openid-connect/token"
LOGOUT_URL = "http://localhost:8085/realms/Dare-Control/protocol/openid-connect/logout"


class SubmissionAPISession(): 
    def __init__(
        self, 
        client_id: str,
        client_secret: str, 
        username: str, 
        password: str, 
        token_url: str, 
        logout_url: str 
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.token_url = token_url
        self.logout_url = logout_url

        self._access_token = None
        self._refresh_token = None

    def __enter__(self):
        self._login()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._logout()

    @property
    def access_token(self): 
        return self._access_token
    
    @property
    def refresh_token(self): 
        return self._refresh_token 

    def _login(self):
        payload = {
            "client_id": self.client_id, 
            "client_secret": self.client_secret, 
            "username": self.username, 
            "password": self.password, 
            "grant_type": "password"
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        response = requests.post(
            self.token_url,
            data=payload,  
            headers=headers
        )
        response.raise_for_status()
        response_json = response.json()
        self._access_token = response_json["access_token"]
        self._refresh_token = response_json["refresh_token"]

    def _refresh(self):
        """
        Function which refreshes the current session token. 
        In order to do this we fetch the 'refresh_token' string from the payload and make a 
        POST request to the submission API. 
        """
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
        )
        response.raise_for_status()
        response_json = response.json()
        self._access_token = response_json["access_token"]
        self._refresh_token = response_json["refresh_token"]

    def _logout(self):
        requests.post(
            self.logout_url,
            data={
                "client_id": self.client_id,
                "refresh_token": self.refresh_token,
            }
        )
        self._access_token = None
        self._refresh_token = None

    def request(self, method, url, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        kwargs["headers"] = headers

        r = requests.request(method, url, **kwargs)
        if r.status_code == 401:
            self._refresh()
            headers["Authorization"] = f"Bearer {self.access_token}"
            kwargs["headers"] = headers
            r = requests.request(method, url, **kwargs)
        return r
