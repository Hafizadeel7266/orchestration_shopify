"""Parse Shopify IDs from GID format."""

from typing import Any, Optional

def parse_shopify_id(id_str: Any) -> Optional[int]:
    """Convert 'gid://shopify/Order/123456' to 123456."""
    if not id_str:
        return None
    id_str = str(id_str)
    if 'gid://' in id_str:
        id_str = id_str.split('/')[-1]
    if '?' in id_str:
        id_str = id_str.split('?')[0]
    try:
        return int(id_str)
    except (ValueError, TypeError):
        return None