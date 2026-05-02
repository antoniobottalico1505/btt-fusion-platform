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

class AcceptTermsIn(BaseModel):
    accepted: bool

class ForgotPasswordIn(BaseModel):
    email: EmailStr

class ResetPasswordIn(BaseModel):
    token: str
    password: str = Field(min_length=6)

class WalletConnectIn(BaseModel):
    address: str
    chain_id: int = 8453
    message: str
    signature: str


class ZeroExQuoteIn(BaseModel):
    chain_id: int = 8453
    sell_token: str
    buy_token: str
    sell_amount: str
