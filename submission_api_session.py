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
import os 

from dotenv import load_dotenv
import requests 


load_dotenv()


class SubmissionAPISession(): 
    def __init__(
        self, 
        client_id: str = None,
        client_secret: str = None, 
        username: str = None, 
        password: str = None, 
        token_url: str = None, 
        logout_url: str = None  
    ):
        self.client_id = client_id or os.getenv("SubmissionAPIKeyCloakClientId")
        self.client_secret = client_secret or os.getenv("SubmissionAPIKeyCloakSecret")
        self.username = username or os.getenv("SubmissionAPIKeyCloakUsername")
        self.password = password or os.getenv("SubmissionAPIKeyCloakPassword")
        self.token_url = token_url or os.getenv("SubmissionAPIKeyCloakTokenUrl")
        self.logout_url = logout_url or os.getenv("SubmissionAPIKeyCloakLogoutUrl")

        self._access_token = None
        self._refresh_token = None

        self._validate_config()

    def _validate_config(self): 
        required = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
            "token_url": self.token_url,
            "logout_url": self.logout_url,
        }

        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required Submission API configuration: {', '.join(missing)}"
                "Please make sure these are present in the .env file!"
            )

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

    def request(self, method, url, token_in="header", token_field="Authorization", **kwargs):
        kwargs = kwargs.copy()
        headers = dict(kwargs.pop("headers", {}))
        data = dict(kwargs.pop("data", {}))

        if token_in == "header":
            headers[token_field] = f"Bearer {self.access_token}"
        elif token_in == "body":
            data[token_field] = self.access_token
        else:
            raise ValueError(f"Unknown token_in value: {token_in}")

        kwargs["headers"] = headers
        kwargs["data"] = data

        response = requests.request(method, url, **kwargs)

        if response.status_code == 401:
            self._refresh()
            if token_in == "header":
                headers[token_field] = f"Bearer {self.access_token}"
            else:
                data[token_field] = self.access_token
            kwargs["headers"] = headers
            kwargs["data"] = data
            response = requests.request(method, url, **kwargs)

        return response
        