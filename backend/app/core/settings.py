from functools import lru_cache
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    APP_NAME: str = 'BTT Fusion Platform'
    APP_ENV: str = 'development'
    SECRET_KEY: str = 'change-me'
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 10080
    DATABASE_URL: str = 'sqlite:///./storage/app.db'
    FRONTEND_URL: str = 'http://localhost:3000'
    ADMIN_EMAIL: str = 'owner@example.com'
    ADMIN_PASSWORD: str = 'change-me-now'
    TRIAL_HOURS: int = 24
    BTT_MAX_RUNS_PER_USER_PER_DAY: int = 2
    MICROCAP_AUTO_START: bool = True
    MICROCAP_PUBLIC_MODE: str = 'paper'
    MICROCAP_LIVE_ENABLED: bool = False
    STORAGE_ROOT: str = './storage'
    CORS_ALLOW_ORIGINS: str = 'http://localhost:3000'
    STRIPE_SECRET_KEY: str = ''
    STRIPE_WEBHOOK_SECRET: str = ''
    STRIPE_PRICE_MONTHLY: str = ''
    STRIPE_PRICE_YEARLY: str = ''
    STRIPE_SUCCESS_URL: str = 'http://localhost:3000/dashboard?checkout=success'
    STRIPE_CANCEL_URL: str = 'http://localhost:3000/pricing?checkout=cancel'

    @field_validator('MICROCAP_PUBLIC_MODE')
    @classmethod
    def validate_mode(cls, value: str) -> str:
        v = (value or 'paper').strip().lower()
        return v if v in {'paper', 'live'} else 'paper'

    @property
    def cors_origins(self) -> List[str]:
        return [x.strip() for x in self.CORS_ALLOW_ORIGINS.split(',') if x.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
