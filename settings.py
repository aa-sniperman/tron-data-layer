from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Get the project root dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[0]
dotenv_path = PROJECT_ROOT / ".env"

class ClickhouseConfig(BaseSettings):
    host: str = Field(default="localhost", validation_alias="CLICKHOUSE_HOST")
    username: str = Field(..., validation_alias="CLICKHOUSE_USERNAME")
    password: str = Field(..., validation_alias="CLICKHOUSE_PASSWORD")
    database: str = Field(..., validation_alias="CLICKHOUSE_DB")

    model_config = SettingsConfigDict(env_file=dotenv_path, extra="allow")

class RedisConfig(BaseSettings):
    host: str = Field(default="localhost", validation_alias="REDIS_HOST")
    password: str = Field(..., validation_alias="REDIS_PASSWORD")
    @property
    def url(self):
        return f"redis://:{self.password}@{self.host}:6379/0"
    
    model_config = SettingsConfigDict(env_file=dotenv_path, extra="allow")

class KeysConfig(BaseSettings):
    trongrid_api_key: str = Field(..., validation_alias="TRONGRID_API_KEY")

    model_config = SettingsConfigDict(env_file=dotenv_path, extra="allow")

class Settings(BaseSettings):
    clickhouse: ClickhouseConfig = ClickhouseConfig()
    redis: RedisConfig = RedisConfig()
    keys: KeysConfig = KeysConfig()


# Instantiate settings
settings = Settings()