"""Table definitions for Shopify DLT pipeline."""

from .orders import orders_table
from .customers import customers_table
from .products import products_table
from .collections import collections_table, collection_products_table
from .shop import shop_table

__all__ = [
    "orders_table",
    "customers_table",
    "products_table",
    "collections_table",
    "collection_products_table",
    "shop_table",
]
