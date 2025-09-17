from pydantic import AnyHttpUrl, AnyUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    tre_fx_token: str
    tre_fx_project: str

    # TES (task execution service) configuration
    tes_base_url: AnyHttpUrl
    tes_docker_image: AnyUrl

    # database configuration
    db_host: AnyUrl
    db_port: int
    db_username: str
    db_password: str
    db_name: str

    # minio configuration
    minio_sts_endpoint: str
    minio_endpoint: str
    minio_output_bucket: str

    # Optional configuration
    tre_fx_tres: list[str] | None = None


class DbConfig(BaseModel):
    db_host: AnyUrl
    db_port: int
    db_username: str
    db_password: str
    db_name: str
