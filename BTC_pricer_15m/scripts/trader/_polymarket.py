"""Shared Polymarket client construction helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True)
class ClobClientContext:
    client: object
    funder: Optional[str]


def _load_clob_client_class(*, allow_legacy: bool):
    try:
        from py_clob_client_v2 import ClobClient

        return ClobClient
    except ImportError:
        if not allow_legacy:
            raise ImportError(
                "py-clob-client-v2 is required for live trading. "
                "Install with: pip install py-clob-client-v2==1.0.0"
            )
    try:
        from py_clob_client.client import ClobClient

        return ClobClient
    except ImportError as exc:
        raise ImportError(
            "py-clob-client-v2 or py-clob-client is required for Polymarket access."
        ) from exc


def create_clob_client(
    *,
    private_key: Optional[str] = None,
    funder: Optional[str] = None,
    signature_type: Optional[int] = None,
    host: str = "https://clob.polymarket.com",
    chain_id: int = 137,
    allow_legacy: bool = False,
) -> ClobClientContext:
    """Create an authenticated Polymarket CLOB client from explicit args/env."""
    priv = private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
    funder_addr = funder or os.getenv("POLYMARKET_FUNDER") or os.getenv("ADDRESS")
    sig_type = (
        int(signature_type)
        if signature_type is not None
        else int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1"))
    )
    if not priv:
        raise ValueError("POLYMARKET_PRIVATE_KEY environment variable is required")
    if sig_type != 0 and not funder_addr:
        raise ValueError(
            "POLYMARKET_FUNDER or ADDRESS environment variable is required "
            f"for signature type {sig_type}"
        )

    ClobClient = _load_clob_client_class(allow_legacy=allow_legacy)
    if sig_type == 0:
        client = ClobClient(host, key=priv, chain_id=chain_id, signature_type=0)
        funder_addr = None
    else:
        client = ClobClient(
            host,
            key=priv,
            chain_id=chain_id,
            signature_type=sig_type,
            funder=funder_addr,
        )

    derive = getattr(client, "create_or_derive_api_key", None)
    if derive is None:
        derive = getattr(client, "create_or_derive_api_creds", None)
    if derive is None:
        raise AttributeError("CLOB client has no API credential derivation method")
    try:
        client.set_api_creds(derive())
    except Exception as exc:
        log.warning("API key derivation failed (%s), falling back to .env credentials", exc)
        api_key = os.getenv("API_KEY")
        secret = os.getenv("SECRET_KEY")
        passphrase = os.getenv("PASSPHRASE")
        if api_key and secret and passphrase:
            try:
                from py_clob_client_v2 import ApiCreds
            except ImportError:
                from py_clob_client.clob_types import ApiCreds
            client.set_api_creds(ApiCreds(
                api_key=api_key, api_secret=secret, api_passphrase=passphrase,
            ))
        else:
            raise
    return ClobClientContext(client=client, funder=funder_addr)
