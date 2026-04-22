"""
OIDC JWT verifier for self-host auth.

The mobile app signs in via the local OIDC provider (default:
https://auth.shifusenproductions.com), receives an RS256 id_token, and
sends it as `Authorization: Bearer <token>` on every API call. This
module verifies the token — signature, issuer, audience, expiry — and
returns the `sub` claim for use as the Omi user id.

Activation: `AUTH_PROVIDER=oidc` in env. When unset, Firebase auth is
used (legacy path, handled in utils/other/endpoints.py).

JWKS are fetched lazily on first use and cached with a TTL; a miss on
the `kid` header triggers a forced refresh (rotation-safe).
"""
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

import httpx
import jwt
from jwt import PyJWKClient

logger = logging.getLogger(__name__)


class OIDCConfigError(RuntimeError):
    """Raised when AUTH_PROVIDER=oidc but required env vars are missing or unreachable."""


_OIDC_ISSUER = (os.environ.get('OIDC_ISSUER') or '').rstrip('/')
_OIDC_JWKS_URL = os.environ.get('OIDC_JWKS_URL') or (f'{_OIDC_ISSUER}/oauth/jwks.json' if _OIDC_ISSUER else '')
_OIDC_AUDIENCE = os.environ.get('OIDC_AUDIENCE')
_OIDC_DISCOVERY_URL = os.environ.get('OIDC_DISCOVERY_URL') or (
    f'{_OIDC_ISSUER}/.well-known/openid-configuration' if _OIDC_ISSUER else ''
)
_JWKS_CACHE_TTL_SEC = int(os.environ.get('OIDC_JWKS_TTL_SEC', '3600'))

_discovery_cache: Optional[Dict[str, Any]] = None
_discovery_expires_at: float = 0.0
_lock = threading.Lock()

# PyJWKClient handles JWKS fetch + caching + kid rotation.
_jwks_client: Optional[PyJWKClient] = None


def _fetch_discovery() -> Dict[str, Any]:
    """Pull /.well-known/openid-configuration and cache it."""
    global _discovery_cache, _discovery_expires_at
    now = time.monotonic()
    with _lock:
        cached = _discovery_cache
        if cached is not None and now < _discovery_expires_at:
            return cached
        if not _OIDC_DISCOVERY_URL:
            raise OIDCConfigError('OIDC_DISCOVERY_URL or OIDC_ISSUER must be set when AUTH_PROVIDER=oidc')
        try:
            resp = httpx.get(_OIDC_DISCOVERY_URL, timeout=10.0)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise OIDCConfigError(f'Unable to fetch OIDC discovery from {_OIDC_DISCOVERY_URL}: {e}') from e
        fresh = resp.json()
        _discovery_cache = fresh
        _discovery_expires_at = now + _JWKS_CACHE_TTL_SEC
        return fresh


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is not None:
        return _jwks_client
    jwks_url = _OIDC_JWKS_URL
    if not jwks_url:
        disc = _fetch_discovery()
        jwks_url = disc.get('jwks_uri') or ''
    if not jwks_url:
        raise OIDCConfigError('OIDC_JWKS_URL missing and discovery did not return jwks_uri')
    _jwks_client = PyJWKClient(jwks_url, cache_keys=True, lifespan=_JWKS_CACHE_TTL_SEC)
    return _jwks_client


def _expected_issuer() -> str:
    if _OIDC_ISSUER:
        return _OIDC_ISSUER
    disc = _fetch_discovery()
    issuer = disc.get('issuer') or ''
    if not issuer:
        raise OIDCConfigError('OIDC issuer could not be determined from env or discovery')
    return issuer.rstrip('/')


def is_enabled() -> bool:
    return os.environ.get('AUTH_PROVIDER', '').lower() == 'oidc'


def verify_token(token: str) -> str:
    """
    Verify an OIDC ID token and return the `sub` claim (the stable user id).

    Raises jwt.PyJWTError (or subclasses) on invalid / expired / wrong-audience tokens.
    The caller should translate those into the framework-appropriate 401.
    """
    jwks_client = _get_jwks_client()
    signing_key = jwks_client.get_signing_key_from_jwt(token).key

    decode_kwargs: Dict[str, Any] = {
        'algorithms': ['RS256'],
        'issuer': _expected_issuer(),
        'options': {'require': ['exp', 'iat', 'sub', 'iss']},
    }
    if _OIDC_AUDIENCE:
        decode_kwargs['audience'] = _OIDC_AUDIENCE
    else:
        # Some providers omit audience for first-party tokens. Skip audience
        # verification when no expected audience is configured.
        decode_kwargs['options']['verify_aud'] = False

    payload: Dict[str, Any] = jwt.decode(token, signing_key, **decode_kwargs)
    uid = payload.get('sub')
    if not uid:
        raise jwt.InvalidTokenError('OIDC token missing "sub" claim')
    return str(uid)
