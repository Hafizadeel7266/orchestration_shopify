"""PII hashing with HMAC-SHA256."""
import hmac
import hashlib
import os
from typing import Any, Optional

PII_HASH_SECRET = os.environ.get("PII_HASH_SECRET", "")


def hash_pii(value: Any) -> Optional[str]:
    """Hash a PII value using HMAC-SHA256 with secret, or SHA-256 if no secret."""
    if value is None:
        return None
    try:
        val_str = str(value).encode('utf-8')
        if PII_HASH_SECRET:
            return hmac.new(PII_HASH_SECRET.encode('utf-8'), val_str, hashlib.sha256).hexdigest()
        return hashlib.sha256(val_str).hexdigest()
    except Exception:
        return None
