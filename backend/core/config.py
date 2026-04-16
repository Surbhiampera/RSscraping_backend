import os
from urllib.parse import quote
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "insurance_v2"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_SSL: bool = False
    DB_SSL_MODE: str = "prefer"

    # JWT
    JWT_SECRET_KEY: str = "super-secret"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440

    # -------------------------
    # CORS
    # -------------------------
    CORS_ORIGINS: str = "http://localhost:5173,http://localhost:3000,http://localhost:8080"

    # -------------------------
    # Redis
    # -------------------------
    REDIS_HOST: str = "localhost"
    REDIS_KEY: str = "defaultkey"

    @property
    def REDIS_URL(self) -> str:
        return f"rediss://:{quote(self.REDIS_KEY, safe='')}@{self.REDIS_HOST}:6380"

    # -------------------------
    # Proxy (Scraper)
    # -------------------------
    PB_PROXY_SERVER: str = ""
    PB_PROXY_USERNAME: str = ""
    PB_PROXY_PASSWORD: str = ""

    # -------------------------
    # App
    # -------------------------
    APP_ENV: str = "development"
    APP_DEBUG: bool = True

    # -------------------------
    # Build DATABASE_URL
    # -------------------------
    @property
    def DATABASE_URL(self) -> str:
        base_url = (
            f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

        if self.DB_SSL:
            base_url += f"?sslmode={self.DB_SSL_MODE}"

        return base_url

    # -------------------------
    # Convert CORS → list
    # -------------------------
    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    # -------------------------
    # DB_CONFIG dict for psycopg2
    # -------------------------
    @property
    def DB_CONFIG(self) -> dict:
        params = {
            "host": self.DB_HOST,
            "port": self.DB_PORT,
            "database": self.DB_NAME,
            "user": self.DB_USER,
            "password": self.DB_PASSWORD,
        }
        if self.DB_SSL:
            params["sslmode"] = self.DB_SSL_MODE
        return params

    # -------------------------
    # Pydantic v2 Settings
    # -------------------------
    model_config = SettingsConfigDict(
        env_file=os.path.join(
            os.path.dirname(os.path.dirname(__file__)), ".env"
        ),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
