"""GraphQL queries for Shopify API."""

from .orders import ORDERS_QUERY
from .customers import CUSTOMERS_QUERY
from .products import PRODUCTS_QUERY
from .collections import COLLECTIONS_QUERY, COLLECTION_PRODUCTS_QUERY
from .shop import SHOP_QUERY

__all__ = [
    "ORDERS_QUERY",
    "CUSTOMERS_QUERY",
    "PRODUCTS_QUERY",
    "COLLECTIONS_QUERY",
    "COLLECTION_PRODUCTS_QUERY",
    "SHOP_QUERY",
]
