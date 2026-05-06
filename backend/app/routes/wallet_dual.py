from datetime import datetime, timezone
import os
import re
from typing import Any

import base58
import httpx
from fastapi import APIRouter, Depends, HTTPException
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.db import get_db
from app.dependencies import get_current_user
from app.models import User
from app.services.noncustodial import build_zeroex_quote, normalize_wallet_address, verify_wallet_signature

settings = get_settings()
router = APIRouter()

SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
SOLANA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOLANA_SOL_MINT = "So11111111111111111111111111111111111111112"

class EvmWalletConnectIn(BaseModel):
    address: str
    chain_id: int = 8453
    message: str
    signature: str

class SolanaWalletConnectIn(BaseModel):
    address: str
    message: str
    signature: str

class EvmQuoteIn(BaseModel):
    chain_id: int = 8453
    sell_token: str
    buy_token: str
    sell_amount: str

class SolanaSwapIn(BaseModel):
    sell_mint: str = SOLANA_USDC_MINT
    buy_mint: str = SOLANA_SOL_MINT
    amount: str
    slippage_bps: int = 600

def _terms_ok(user: User) -> bool:
    return (getattr(user, "accepted_terms_version", "") or "") == settings.TERMS_VERSION

def _require_paid_ready(user: User) -> None:
    if not getattr(user, "email_verified", False):
        raise HTTPException(status_code=403, detail="Completa prima la verifica email.")
    if not _terms_ok(user):
        raise HTTPException(status_code=403, detail="Accetta prima termini e policy.")
    if getattr(user, "subscription_status", "") != "active":
        raise HTTPException(status_code=402, detail="Serve un abbonamento attivo.")

def _valid_solana_address(address: str) -> str:
    value = str(address or "").strip()
    if not SOLANA_ADDRESS_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail="Wallet Solana non valido")
    try:
        raw = base58.b58decode(value)
    except Exception:
        raise HTTPException(status_code=400, detail="Wallet Solana non valido")
    if len(raw) != 32:
        raise HTTPException(status_code=400, detail="Wallet Solana non valido")
    return value

def _verify_solana_signature(address: str, message: str, signature: str) -> bool:
    addr = _valid_solana_address(address)
    msg = str(message or "")
    sig = str(signature or "").strip()
    if not msg or not sig:
        return False
    if addr not in msg:
        raise HTTPException(status_code=400, detail="Il messaggio firmato non contiene il wallet Solana dichiarato")
    try:
        VerifyKey(base58.b58decode(addr)).verify(msg.encode("utf-8"), base58.b58decode(sig))
        return True
    except BadSignatureError:
        return False
    except Exception:
        return False

def _evm_address(user: User) -> str:
    return str(getattr(user, "evm_wallet_address", "") or getattr(user, "wallet_address", "") or "").strip()

def _sol_address(user: User) -> str:
    return str(getattr(user, "solana_wallet_address", "") or "").strip()

@router.get("/api/wallet/dual/me")
def wallet_dual_me(user: User = Depends(get_current_user)):
    evm = _evm_address(user)
    sol = _sol_address(user)
    paid_ready = bool(getattr(user, "email_verified", False) and _terms_ok(user) and getattr(user, "subscription_status", "") == "active")
    return {
        "email_verified": bool(getattr(user, "email_verified", False)),
        "terms_ok": _terms_ok(user),
        "subscription_status": getattr(user, "subscription_status", "") or "inactive",
        "paid_ready": paid_ready,
        "evm": {"connected": bool(evm), "address": evm, "chain_id": int(getattr(user, "evm_wallet_chain_id", None) or getattr(user, "wallet_chain_id", None) or 8453), "ready": bool(paid_ready and evm)},
        "solana": {"connected": bool(sol), "address": sol, "ready": bool(paid_ready and sol)},
    }

@router.post("/api/wallet/evm/connect")
def wallet_evm_connect(payload: EvmWalletConnectIn, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    address = normalize_wallet_address(payload.address)
    if not verify_wallet_signature(address=address, message=payload.message, signature=payload.signature):
        raise HTTPException(status_code=400, detail="Firma wallet EVM non valida")
    user.evm_wallet_address = address
    user.evm_wallet_chain_id = int(payload.chain_id or 8453)
    user.evm_wallet_connected_at = datetime.now(timezone.utc)
    user.evm_wallet_link_message = payload.message
    user.evm_wallet_link_signature = payload.signature
    user.wallet_address = address
    user.wallet_chain_id = int(payload.chain_id or 8453)
    user.wallet_connected_at = user.evm_wallet_connected_at
    user.wallet_link_message = payload.message
    user.wallet_link_signature = payload.signature
    db.commit()
    return {"ok": True, "connected": True, "address": user.evm_wallet_address, "chain_id": user.evm_wallet_chain_id}

@router.delete("/api/wallet/evm/disconnect")
def wallet_evm_disconnect(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user.evm_wallet_address = ""
    user.evm_wallet_chain_id = 8453
    user.evm_wallet_connected_at = None
    user.evm_wallet_link_message = ""
    user.evm_wallet_link_signature = ""
    user.wallet_address = ""
    user.wallet_chain_id = 8453
    user.wallet_connected_at = None
    user.wallet_link_message = ""
    user.wallet_link_signature = ""
    db.commit()
    return {"ok": True, "connected": False}

@router.post("/api/wallet/evm/quote")
def wallet_evm_quote(payload: EvmQuoteIn, user: User = Depends(get_current_user)):
    _require_paid_ready(user)
    evm = _evm_address(user)
    if not evm:
        raise HTTPException(status_code=400, detail="Collega prima il wallet EVM")
    user.wallet_address = evm
    user.wallet_chain_id = int(getattr(user, "evm_wallet_chain_id", None) or payload.chain_id or 8453)
    return build_zeroex_quote(user=user, chain_id=payload.chain_id, sell_token=payload.sell_token, buy_token=payload.buy_token, sell_amount=payload.sell_amount)

@router.post("/api/wallet/solana/connect")
def wallet_solana_connect(payload: SolanaWalletConnectIn, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    address = _valid_solana_address(payload.address)
    if not _verify_solana_signature(address=address, message=payload.message, signature=payload.signature):
        raise HTTPException(status_code=400, detail="Firma wallet Solana non valida")
    user.solana_wallet_address = address
    user.solana_wallet_connected_at = datetime.now(timezone.utc)
    user.solana_wallet_link_message = payload.message
    user.solana_wallet_link_signature = payload.signature
    db.commit()
    return {"ok": True, "connected": True, "address": user.solana_wallet_address}

@router.delete("/api/wallet/solana/disconnect")
def wallet_solana_disconnect(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user.solana_wallet_address = ""
    user.solana_wallet_connected_at = None
    user.solana_wallet_link_message = ""
    user.solana_wallet_link_signature = ""
    db.commit()
    return {"ok": True, "connected": False}

@router.post("/api/wallet/solana/swap")
def wallet_solana_swap(payload: SolanaSwapIn, user: User = Depends(get_current_user)):
    _require_paid_ready(user)
    sol = _sol_address(user)
    if not sol:
        raise HTTPException(status_code=400, detail="Collega prima il wallet Solana")
    amount = str(payload.amount or "").strip()
    if not amount.isdigit() or int(amount) <= 0:
        raise HTTPException(status_code=400, detail="Importo Solana non valido")
    base = os.getenv("SOLANA_QUOTE_BASE_URL", "https://lite-api.jup.ag").rstrip("/")
    try:
        with httpx.Client(timeout=25.0) as client:
            quote_resp = client.get(f"{base}/swap/v1/quote", params={"inputMint": payload.sell_mint, "outputMint": payload.buy_mint, "amount": amount, "slippageBps": int(payload.slippage_bps or 600)})
            quote_data: Any = quote_resp.json()
            if quote_resp.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"Jupiter quote error {quote_resp.status_code}: {str(quote_data)[:800]}")
            swap_resp = client.post(f"{base}/swap/v1/swap", json={"quoteResponse": quote_data, "userPublicKey": sol, "wrapAndUnwrapSol": True, "dynamicComputeUnitLimit": True, "prioritizationFeeLamports": "auto"})
            swap_data: Any = swap_resp.json()
            if swap_resp.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"Jupiter swap error {swap_resp.status_code}: {str(swap_data)[:800]}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Connessione Jupiter fallita: {type(exc).__name__}: {exc}")
    tx = swap_data.get("swapTransaction")
    if not tx:
        raise HTTPException(status_code=502, detail="Jupiter non ha restituito una transazione firmabile")
    return {"ok": True, "execution_model": "solana_non_custodial_wallet_signature", "wallet": sol, "quote": quote_data, "swap_transaction": tx}
