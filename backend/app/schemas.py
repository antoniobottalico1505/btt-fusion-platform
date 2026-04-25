from pydantic import BaseModel, EmailStr, Field


class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    full_name: str = ''


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class StripeCheckoutIn(BaseModel):
    plan: str


class AdminTextUpdate(BaseModel):
    value: str


class AdminJsonUpdate(BaseModel):
    value: dict


class MicrocapControlIn(BaseModel):
    mode: str | None = None


class ExternalMicrocapHeartbeatIn(BaseModel):
    api_key: str
    process: dict = Field(default_factory=dict)
    dashboard: dict = Field(default_factory=dict)
