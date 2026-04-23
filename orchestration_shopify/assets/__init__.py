"""Shopify Dagster assets — all tables as individual assets."""

from .shopify_assets import (
shop_asset,
order_asset, order_line_items_asset, order_li_tax_asset,
order_li_discounts_asset, order_li_attrs_asset,
order_refunds_asset, order_refund_li_asset, order_refund_adj_asset,
order_discounts_asset, order_tax_asset,
order_shipping_asset, order_shipping_tax_asset,
customer_asset, customer_tags_asset,
product_asset, product_variants_asset, product_var_options_asset,
product_images_asset, product_options_asset, product_opt_values_asset,
product_tags_asset,
collection_asset, collection_rules_asset,
collection_product_asset,
)

all_assets = [
shop_asset,
order_asset, order_line_items_asset, order_li_tax_asset,
order_li_discounts_asset, order_li_attrs_asset,
order_refunds_asset, order_refund_li_asset, order_refund_adj_asset,
order_discounts_asset, order_tax_asset,
order_shipping_asset, order_shipping_tax_asset,
customer_asset, customer_tags_asset,
product_asset, product_variants_asset, product_var_options_asset,
product_images_asset, product_options_asset, product_opt_values_asset,
product_tags_asset,
collection_asset, collection_rules_asset,
collection_product_asset,
]

__all__ = ["all_assets"]