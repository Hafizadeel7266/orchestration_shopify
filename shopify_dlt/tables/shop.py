"""Shop table definition - single table."""

import dlt
import json
from typing import Iterator, Dict, Optional
from datetime import datetime

import logging

from ..helpers.parsers import parse_shopify_id
from ..schemas.schemas import SHOP_COLUMNS
from ..queries.shop import SHOP_QUERY

logger = logging.getLogger("shopify_pipeline.shop")



def transform_shop(shop: Dict) -> Dict:
    """Transform shop data to match the desired schema."""
    
    result = {}
    
    # Parse ID
    if shop.get('id'):
        result['id'] = parse_shopify_id(shop['id'])
    
    # Core field mappings
    field_mappings = {
        'name': 'name',
        'email': 'email',
        'myshopifyDomain': 'myshopify_domain',
        'currencyCode': 'currency',
        'ianaTimezone': 'iana_timezone',
        'timezoneAbbreviation': 'timezone_abbreviation',
        'checkoutApiSupported': 'checkout_api_supported',
        'taxesIncluded': 'taxes_included',
        'taxShipping': 'tax_shipping',
        'createdAt': 'created_at',
        'updatedAt': 'updated_at',
        'contactEmail': 'contact_email',
        'description': 'description',
        'customerAccounts': 'customer_accounts',
    }

    for gql_field, table_field in field_mappings.items():
        if gql_field in shop and shop[gql_field] is not None:
            result[table_field] = shop[gql_field]
    
    # Domain information
    if shop.get('primaryDomain'):
        primary_domain = shop['primaryDomain']
        result['domain'] = primary_domain.get('host')
    
    # Enabled currencies as JSON
    if shop.get('enabledPresentmentCurrencies'):
        result['enabled_presentment_currencies'] = json.dumps(shop['enabledPresentmentCurrencies'])
    
    # Plan information
    if shop.get('plan'):
        plan = shop['plan']
        result['plan_partner_development'] = plan.get('partnerDevelopment')
        result['plan_shopify_plus'] = plan.get('shopifyPlus')
        if plan.get('shopifyPlus'):
            result['plan_name'] = 'shopify_plus'
        elif plan.get('partnerDevelopment'):
            result['plan_name'] = 'partner_development'
        else:
            result['plan_name'] = 'unknown'
    
    # Currency formats
    if shop.get('currencyFormats'):
        formats = shop['currencyFormats']
        result['money_format'] = formats.get('moneyFormat')
        result['money_in_emails_format'] = formats.get('moneyInEmailsFormat')
        result['money_with_currency_format'] = formats.get('moneyWithCurrencyFormat')
        result['money_with_currency_in_emails_format'] = formats.get('moneyWithCurrencyInEmailsFormat')
    
    # Features
    if shop.get('features'):
        features = shop['features']
        result['features_json'] = json.dumps(features)
        
        # Top-level features
        result['has_avalara_avatax'] = features.get('avalaraAvatax')
        result['branding'] = features.get('branding')
        result['has_captcha'] = features.get('captcha')
        result['has_dynamic_remarketing'] = features.get('dynamicRemarketing')
        result['eligible_for_subscription_migration'] = features.get('eligibleForSubscriptionMigration')
        result['eligible_for_subscriptions'] = features.get('eligibleForSubscriptions')
        result['has_gift_cards_feature'] = features.get('giftCards')
        result['has_harmonized_system_code'] = features.get('harmonizedSystemCode')
        result['has_legacy_subscription_gateway'] = features.get('legacySubscriptionGatewayEnabled')
        result['has_live_view'] = features.get('liveView')
        result['paypal_express_subscription_status'] = features.get('paypalExpressSubscriptionGatewayStatus')
        result['has_reports'] = features.get('reports')
        result['sells_subscriptions'] = features.get('sellsSubscriptions')
        result['show_metrics'] = features.get('showMetrics')
        result['has_storefront_feature'] = features.get('storefront')
        result['has_unified_markets'] = features.get('unifiedMarkets')
        
        # Bundles
        if features.get('bundles'):
            bundles = features['bundles']
            result['bundles_eligible'] = bundles.get('eligibleForBundles')
            result['bundles_ineligibility_reason'] = bundles.get('ineligibilityReason')
            result['bundles_sells'] = bundles.get('sellsBundles')
        
        # Cart Transform
        if features.get('cartTransform') and features['cartTransform'].get('eligibleOperations'):
            ops = features['cartTransform']['eligibleOperations']
            result['cart_transform_expand'] = ops.get('expandOperation')
            result['cart_transform_merge'] = ops.get('mergeOperation')
            result['cart_transform_update'] = ops.get('updateOperation')

    # Currency settings
    if shop.get('currencySettings') and shop['currencySettings'].get('edges'):
        currencies = []
        for edge in shop['currencySettings']['edges']:
            node = edge.get('node', {})
            currencies.append({
                'currency_code': node.get('currencyCode'),
                'currency_name': node.get('currencyName'),
                'enabled': node.get('enabled'),
                'rate_updated_at': node.get('rateUpdatedAt')
            })
        if currencies:
            result['currency_settings'] = json.dumps(currencies)

    result['source'] = 'shopify_graphql'
    result['_loaded_at'] = datetime.now().isoformat()
    
    return result


def fetch_shop_graphql(graphql_client) -> Dict:
    """Fetch shop information using GraphQL client."""
    
    logger.info("   Fetching shop data...")
    
    try:
        result = graphql_client(SHOP_QUERY)
    except Exception as e:
        logger.error(f"Failed to fetch shop data: {e}")
        raise
    
    if 'data' not in result or 'shop' not in result['data']:
        logger.warning("   No shop data found")
        return {}
    
    shop = result['data']['shop']
    transformed = transform_shop(shop)
    
    logger.info("   Shop data fetched successfully")
    return transformed


@dlt.resource(
    name="shop",
    primary_key='id',
    write_disposition="replace",
    table_name="shop",
    columns=SHOP_COLUMNS
)
def shop_table(graphql_client) -> Iterator[Dict]:
    """Shop table resource."""
    
    logger.info(f" Fetching shop data")
    
    shop = fetch_shop_graphql(graphql_client)
    
    if shop:
        logger.info(f" Shop data loaded: ID {shop.get('id')}, Name: {shop.get('name')}")
        yield shop
    else:
        logger.warning(" No shop data to yield")