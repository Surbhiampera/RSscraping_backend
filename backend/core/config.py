from email.quoprimime import quote
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "insurance_v2"
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_ssl: bool = False
    db_ssl_mode: str = "prefer"

    # JWT
    JWT_SECRET_KEY: str = "super-secret"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440

    # -------------------------
    # CORS
    # -------------------------
    cors_origins: str = "http://localhost:5173,http://localhost:3000,http://localhost:8080"

    # -------------------------
    # Redis
    # -------------------------
    REDIS_HOST: str = "localhost"
    REDIS_KEY: str = "defaultkey"
    
    @property
    def REDIS_URL(self) -> str:
        return f"rediss://:{self.REDIS_KEY}@{self.REDIS_HOST}:6380"

    # -------------------------
    # App
    # -------------------------
    app_env: str = "development"
    app_debug: bool = True

    # -------------------------
    # Build DATABASE_URL
    # -------------------------
    @property
    def DATABASE_URL(self) -> str:
        base_url = (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        if self.db_ssl:
            base_url += f"?sslmode={self.db_ssl_mode}"

        return base_url

    # -------------------------
    # Convert CORS → list
    # -------------------------
    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

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