import os
import re
from typing import Any
from urllib.parse import urlencode

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct
from fastapi import HTTPException

from app.models import User
from app.services.admin_config import get_microcap_env


EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def normalize_wallet_address(address: str) -> str:
    value = str(address or "").strip()

    if not EVM_ADDRESS_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail="Wallet EVM non valido")

    return Account.from_key(
        "0x" + "1".zfill(64)
    ).address[:0] + value.lower()


def verify_wallet_signature(address: str, message: str, signature: str) -> bool:
    normalized = normalize_wallet_address(address)
    msg = str(message or "")
    sig = str(signature or "").strip()

    if not msg or not sig:
        return False

    if normalized.lower() not in msg.lower():
        raise HTTPException(
            status_code=400,
            detail="Il messaggio firmato non contiene il wallet dichiarato",
        )

    try:
        recovered = Account.recover_message(
            encode_defunct(text=msg),
            signature=sig,
        )
    except Exception:
        return False

    return recovered.lower() == normalized.lower()


def get_zeroex_api_key() -> str:
    env = get_microcap_env(masked=False)
    key = str(env.get("ZEROEX_API_KEY") or os.getenv("ZEROEX_API_KEY") or "").strip()

    if not key:
        raise HTTPException(
            status_code=500,
            detail="ZEROEX_API_KEY mancante nel backend/runtime env Microcap",
        )

    return key


def build_zeroex_quote(
    *,
    user: User,
    chain_id: int,
    sell_token: str,
    buy_token: str,
    sell_amount: str,
) -> dict[str, Any]:
    taker = str(getattr(user, "wallet_address", "") or "").strip()

    if not taker:
        raise HTTPException(status_code=400, detail="Collega prima il wallet")

    if not EVM_ADDRESS_RE.fullmatch(taker):
        raise HTTPException(status_code=400, detail="Wallet utente non valido")

    params = {
        "chainId": str(int(chain_id)),
        "sellToken": str(sell_token).strip(),
        "buyToken": str(buy_token).strip(),
        "sellAmount": str(sell_amount).strip(),
        "taker": taker,
    }

    if not params["sellToken"] or not params["buyToken"] or not params["sellAmount"]:
        raise HTTPException(status_code=400, detail="sellToken, buyToken e sellAmount sono obbligatori")

    headers = {
        "0x-api-key": get_zeroex_api_key(),
        "0x-version": "v2",
        "Content-Type": "application/json",
    }

    url = "https://api.0x.org/swap/allowance-holder/quote?" + urlencode(params)

    try:
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url, headers=headers)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Connessione 0x fallita: {type(exc).__name__}: {exc}",
        )

    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"0x quote error {resp.status_code}: {str(data)[:1200]}",
        )

    tx = data.get("transaction") or {}

    if not tx.get("to") or not tx.get("data"):
        raise HTTPException(
            status_code=502,
            detail="0x non ha restituito una transazione eseguibile",
        )

    return {
        "ok": True,
        "execution_model": "non_custodial_wallet_signature",
        "taker": taker,
        "chain_id": chain_id,
        "quote": data,
        "allowance_target": data.get("allowanceTarget")
            or (((data.get("issues") or {}).get("allowance") or {}).get("spender")),
        "transaction": {
            "from": taker,
            "to": tx.get("to"),
            "data": tx.get("data"),
            "value": hex(int(tx.get("value") or "0")),
            "gas": hex(int(tx["gas"])) if tx.get("gas") else None,
            "gasPrice": hex(int(tx["gasPrice"])) if tx.get("gasPrice") else None,
            "maxFeePerGas": hex(int(tx["maxFeePerGas"])) if tx.get("maxFeePerGas") else None,
            "maxPriorityFeePerGas": hex(int(tx["maxPriorityFeePerGas"])) if tx.get("maxPriorityFeePerGas") else None,
        },
    }