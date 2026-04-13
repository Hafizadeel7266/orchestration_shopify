"""Transforms applied to raw GraphQL responses before yielding to DLT."""
import logging
from typing import Dict, List

from .parsers import parse_shopify_id
from .hashing import hash_pii

logger = logging.getLogger("shopify_pipeline.transforms")

# --- PII Path Configs ---
ORDER_PII_PATHS = [
    'email', 'phone', 'note',
    'customer.email', 'customer.firstName', 'customer.lastName', 'customer.phone',
    'billingAddress.firstName', 'billingAddress.lastName', 'billingAddress.name',
    'billingAddress.address1', 'billingAddress.address2', 'billingAddress.phone',
    'shippingAddress.firstName', 'shippingAddress.lastName', 'shippingAddress.name',
    'shippingAddress.address1', 'shippingAddress.address2', 'shippingAddress.phone',
]

LINE_ITEM_PII_PATHS = []  # No PII in line items currently

REFUND_PII_PATHS = ['note']

JOURNEY_PII_PATHS = []

CUSTOMER_PII_PATHS = [
    'firstName', 'lastName', 'note',
    'email', 'phone',  # Flattened from defaultEmailAddress/defaultPhoneNumber
]


def parse_gids(obj):
    """Recursively find 'id' fields containing 'gid://' and convert to int."""
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key == 'id' and isinstance(val, str) and 'gid://' in val:
                obj[key] = parse_shopify_id(val)
            else:
                parse_gids(val)
    elif isinstance(obj, list):
        for item in obj:
            parse_gids(item)
    return obj


def hash_pii_fields(obj: Dict, paths: List[str]) -> Dict:
    """Hash PII fields at specified dot-notation paths in a dict.

    Supports:
    - Simple paths: 'email', 'phone'
    - Nested paths: 'customer.email', 'billingAddress.firstName'
    - Array wildcards: 'addresses[*].firstName'
    """
    for path in paths:
        _hash_at_path(obj, path.split('.'))
    return obj


def _hash_at_path(obj, parts):
    """Recursively traverse and hash the value at the end of the path."""
    if not parts or not isinstance(obj, dict):
        return

    key = parts[0]

    if '[*]' in key:
        # Array wildcard: 'addresses[*]' means iterate the array
        array_key = key.replace('[*]', '')
        arr = obj.get(array_key)
        if isinstance(arr, list):
            for item in arr:
                if len(parts) == 1:
                    pass  # Can't hash an array element itself
                else:
                    _hash_at_path(item, parts[1:])
    elif len(parts) == 1:
        # Leaf: hash the value
        if key in obj and obj[key] is not None:
            obj[key] = hash_pii(obj[key])
    else:
        # Intermediate: recurse into nested dict
        nested = obj.get(key)
        if isinstance(nested, dict):
            _hash_at_path(nested, parts[1:])
