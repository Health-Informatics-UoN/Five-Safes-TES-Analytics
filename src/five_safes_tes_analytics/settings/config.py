from urllib.parse import urlparse
from pathlib import Path
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict

class TesClientConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    TES_base_url: Url
    TES_project: str
    TES_tres: list[str]
    tes_docker_image: str
    postgresPort: int = 5432
    postgresServer: str
    postgresUsername: str
    postgresPassword: str
    postgresDatabase: str
    postgresSchema: str = "public"

    @property
    def TES_url(self) -> str:
        parsed = urlparse(str(self.TES_base_url))
        path = Path(parsed.path) / "v1"
        path_str = str(path) if str(path).startswith('/') else '/' + str(path)
        return f"{parsed.scheme}://{parsed.netloc}{path_str}"


    @property
    def submission_url(self) -> str:
        parsed = urlparse(str(self.TES_base_url))
        path = Path(parsed.path) / "api" / "Submission"
        path_str = str(path) if str(path).startswith('/') else '/' + str(path)
        return f"{parsed.scheme}://{parsed.netloc}{path_str}"

    @property
    def default_image(self) -> str:
        return self.tes_docker_image

    @property
    def default_db_port(self) -> int:
        return self.postgresPort

    @property
    def db_url(self) -> str:
        return f"postgres://{self.postgresUsername}:{self.postgresPassword}@{self.postgresServer}:{self.postgresPort}/{self.postgresDatabase}"

    @property
    def default_db_config(self) -> dict[str, str]:
        #only here for compatibility
        return {
                "host": self.postgresServer,
                "username": self.postgresUsername,
                "password": self.postgresPassword,
                "name": self.postgresDatabase,
                "port": str(self.postgresPort),
                "schema": self.postgresSchema,
                }


class BunnyConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    collection_id: str
    # leaving in case people set their own levels, but could easily be Literal['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    bunny_logger_level: str
    task_api_base_url: Url
    task_api_username: str
    task_api_password: str
    

class SubmissionConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    SubmissionAPIKeyCloakClientId: str
    SubmissionAPIKeyCloakSecret:str
    SubmissionAPIKeyCloakUsername: str
    SubmissionAPIKeyCloakPassword: str
    SubmissionAPIBaseKeyCloakUrl: Url

    @property
    def client_id(self) -> str:
        return self.SubmissionAPIKeyCloakClientId

    @property
    def client_secret(self) -> str:
        return self.SubmissionAPIKeyCloakSecret

    @property
    def username(self) -> str:
        return self.SubmissionAPIKeyCloakUsername

    @property
    def password(self) -> str:
        return self.SubmissionAPIKeyCloakPassword

    @property
    def base_keycloak_url(self) -> str:
        return str(self.SubmissionAPIBaseKeyCloakUrl)


class MinioConfig(BaseSettings):
    minio_sts_endpoint: Url
    minio_endpoint: Url
    minio_output_bucket: str


