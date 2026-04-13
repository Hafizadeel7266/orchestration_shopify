"""Schema hints — column types and nested table configuration for all tables.

Column names use camelCase matching GraphQL field names (yield-raw approach).
DLT auto-flattens nested dicts with __ separator into the parent table.
Lists of dicts become child tables.
"""

import dlt


# =============================================================================
# HELPERS — reduce repetition for Shopify money types
# =============================================================================


def _money_v2(prefix):
    """MoneyV2: {amount, currencyCode} → 2 columns on parent."""
    return {
        f"{prefix}__amount": {"data_type": "double", "nullable": True},
        f"{prefix}__currencyCode": {"data_type": "text", "nullable": True},
    }


def _money_bag(prefix):
    """MoneyBag: {shopMoney: MoneyV2, presentmentMoney: MoneyV2} → 4 columns."""
    return {
        **_money_v2(f"{prefix}__shopMoney"),
        **_money_v2(f"{prefix}__presentmentMoney"),
    }


def _money_v2_list(prefix):
    """MoneyV2 columns for child tables (list format)."""
    return [
        {"name": f"{prefix}__amount", "data_type": "double"},
        {"name": f"{prefix}__currencyCode", "data_type": "text"},
    ]


def _money_bag_list(prefix):
    """MoneyBag columns for child tables (list format)."""
    return [
        *_money_v2_list(f"{prefix}__shopMoney"),
        *_money_v2_list(f"{prefix}__presentmentMoney"),
    ]


# =============================================================================
# ORDER TABLE — parent columns
# =============================================================================

ORDER_COLUMNS = {
    # --- PK ---
    "id": {"data_type": "bigint", "nullable": False},
    "legacyResourceId": {"data_type": "text", "nullable": True},
    "name": {"data_type": "text", "nullable": True},
    # --- Shop identity (injected by source for multi-shop clustering) ---
    "shopId": {"data_type": "bigint", "nullable": False},
    # --- Timestamps ---
    "createdAt": {"data_type": "timestamp", "nullable": True},
    "processedAt": {"data_type": "timestamp", "nullable": True},
    "updatedAt": {"data_type": "timestamp", "nullable": True},
    "cancelledAt": {"data_type": "timestamp", "nullable": True},
    "closedAt": {"data_type": "timestamp", "nullable": True},
    # --- Booleans ---
    "confirmed": {"data_type": "bool", "nullable": True},
    "test": {"data_type": "bool", "nullable": True},
    "taxesIncluded": {"data_type": "bool", "nullable": True},
    "canMarkAsPaid": {"data_type": "bool", "nullable": True},
    "canNotifyCustomer": {"data_type": "bool", "nullable": True},
    "capturable": {"data_type": "bool", "nullable": True},
    # --- Status ---
    "displayFinancialStatus": {"data_type": "text", "nullable": True},
    "displayFulfillmentStatus": {"data_type": "text", "nullable": True},
    "cancelReason": {"data_type": "text", "nullable": True},
    # --- Currency ---
    "currencyCode": {"data_type": "text", "nullable": True},
    "presentmentCurrencyCode": {"data_type": "text", "nullable": True},
    # --- PII (hashed to 64-char hex) ---
    "email": {"data_type": "text", "nullable": True},
    "phone": {"data_type": "text", "nullable": True},
    "note": {"data_type": "text", "nullable": True},
    # --- Metadata ---
    "tags": {"data_type": "text", "nullable": True},
    "customerLocale": {"data_type": "text", "nullable": True},
    "sourceName": {"data_type": "text", "nullable": True},
    "sourceIdentifier": {"data_type": "text", "nullable": True},
    "discountCode": {"data_type": "text", "nullable": True},
    "statusPageUrl": {"data_type": "text", "nullable": True},
    "registeredSourceUrl": {"data_type": "text", "nullable": True},
    "clientIp": {"data_type": "text", "nullable": True},
    # --- Top-level money (GraphQL returns as strings) ---
    "subtotalPrice": {"data_type": "double", "nullable": True},
    "totalDiscounts": {"data_type": "double", "nullable": True},
    "totalPrice": {"data_type": "double", "nullable": True},
    "totalShippingPrice": {"data_type": "double", "nullable": True},
    "totalTax": {"data_type": "double", "nullable": True},
    "totalWeight": {"data_type": "double", "nullable": True},
    # --- Customer (flattened from nested dict) ---
    "customer__id": {"data_type": "bigint", "nullable": True},
    "customer__email": {"data_type": "text", "nullable": True},  # hashed PII
    # "customer__firstName": {"data_type": "text", "nullable": True},    # hashed PII
    # "customer__lastName": {"data_type": "text", "nullable": True},     # hashed PII
    # "customer__phone": {"data_type": "text", "nullable": True},        # hashed PII
    # "customer__numberOfOrders": {"data_type": "text", "nullable": True},
    # "customer__totalSpent": {"data_type": "double", "nullable": True},
    # "customer__totalSpentCurrency": {"data_type": "text", "nullable": True},
    # --- App (flattened) ---
    "app__id": {"data_type": "bigint", "nullable": True},
    "app__name": {"data_type": "text", "nullable": True},
    # --- Billing Address (flattened, PII fields hashed) ---
    "billingAddress__phone": {"data_type": "text", "nullable": True},  # hashed PII
    "billingAddress__company": {"data_type": "text", "nullable": True},
    "billingAddress__city": {"data_type": "text", "nullable": True},
    "billingAddress__province": {"data_type": "text", "nullable": True},
    "billingAddress__provinceCode": {"data_type": "text", "nullable": True},
    "billingAddress__country": {"data_type": "text", "nullable": True},
    "billingAddress__countryCodeV2": {"data_type": "text", "nullable": True},
    "billingAddress__zip": {"data_type": "text", "nullable": True},
    # --- Shipping Address (flattened, PII fields hashed) ---
    "shippingAddress__phone": {"data_type": "text", "nullable": True},  # hashed PII
    "shippingAddress__company": {"data_type": "text", "nullable": True},
    "shippingAddress__city": {"data_type": "text", "nullable": True},
    "shippingAddress__province": {"data_type": "text", "nullable": True},
    "shippingAddress__provinceCode": {"data_type": "text", "nullable": True},
    "shippingAddress__country": {"data_type": "text", "nullable": True},
    "shippingAddress__countryCodeV2": {"data_type": "text", "nullable": True},
    "shippingAddress__zip": {"data_type": "text", "nullable": True},
    # --- Price Sets (MoneyBag → 4 cols each) ---
    **_money_bag("subtotalPriceSet"),
    **_money_bag("totalDiscountsSet"),
    **_money_bag("totalPriceSet"),
    **_money_bag("totalShippingPriceSet"),
    **_money_bag("totalTaxSet"),
    **_money_bag("totalReceivedSet"),
    **_money_bag("totalRefundedSet"),
    **_money_bag("originalTotalDutiesSet"),
    **_money_bag("currentTotalPriceSet"),
    **_money_bag("currentTotalDutiesSet"),
    **_money_bag("currentShippingPriceSet"),
    **_money_bag("cartDiscountAmountSet"),
    **_money_bag("currentCartDiscountAmountSet"),
    # --- Total Tip (MoneyV2 → 2 cols) ---
    **_money_v2("totalTipReceived"),
    # --- Customer Journey Summary (flattened from nested dict) ---
    "customerJourneySummary__customerOrderIndex": {
        "data_type": "bigint",
        "nullable": True,
    },
    "customerJourneySummary__daysToConversion": {
        "data_type": "bigint",
        "nullable": True,
    },
    "customerJourneySummary__ready": {"data_type": "bool", "nullable": True},
    "customerJourneySummary__momentsCount__count": {
        "data_type": "bigint",
        "nullable": True,
    },
    "customerJourneySummary__momentsCount__precision": {
        "data_type": "text",
        "nullable": True,
    },
    # First Visit
    "customerJourneySummary__firstVisit__id": {"data_type": "bigint", "nullable": True},
    "customerJourneySummary__firstVisit__landingPage": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__occurredAt": {
        "data_type": "timestamp",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__referralCode": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__referralInfoHtml": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__referrerUrl": {
        "data_type": "text",
        "nullable": True,
    },  # hashed PII
    "customerJourneySummary__firstVisit__sourceType": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__source": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__sourceDescription": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__utmParameters__campaign": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__utmParameters__content": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__utmParameters__medium": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__utmParameters__source": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__firstVisit__utmParameters__term": {
        "data_type": "text",
        "nullable": True,
    },
    # Last Visit
    "customerJourneySummary__lastVisit__id": {"data_type": "bigint", "nullable": True},
    "customerJourneySummary__lastVisit__landingPage": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__occurredAt": {
        "data_type": "timestamp",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__referralCode": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__referralInfoHtml": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__referrerUrl": {
        "data_type": "text",
        "nullable": True,
    },  # hashed PII
    "customerJourneySummary__lastVisit__sourceType": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__source": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__sourceDescription": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__utmParameters__campaign": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__utmParameters__content": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__utmParameters__medium": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__utmParameters__source": {
        "data_type": "text",
        "nullable": True,
    },
    "customerJourneySummary__lastVisit__utmParameters__term": {
        "data_type": "text",
        "nullable": True,
    },
    # --- Audit ---
    "_loaded_at": {"data_type": "timestamp", "nullable": True},
}


# =============================================================================
# LINE ITEM COLUMNS — child of order (lineItems)
# =============================================================================

LINE_ITEM_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "orderId", "data_type": "bigint", "nullable": False},
    # Product (flattened from nested dict)
    {"name": "product__id", "data_type": "bigint"},
    {"name": "product__title", "data_type": "text"},
    {"name": "product__productType", "data_type": "text"},
    {"name": "product__vendor", "data_type": "text"},
    # Variant (flattened from nested dict)
    {"name": "variant__id", "data_type": "bigint"},
    {"name": "variant__title", "data_type": "text"},
    {"name": "variant__sku", "data_type": "text"},
    {"name": "variant__price", "data_type": "double"},
    {"name": "variant__compareAtPrice", "data_type": "double"},
    # Core
    {"name": "title", "data_type": "text"},
    {"name": "name", "data_type": "text"},
    {"name": "sku", "data_type": "text"},
    {"name": "vendor", "data_type": "text"},
    # Quantities
    {"name": "quantity", "data_type": "bigint"},
    {"name": "fulfillableQuantity", "data_type": "bigint"},
    {"name": "refundableQuantity", "data_type": "bigint"},
    {"name": "unfulfilledQuantity", "data_type": "bigint"},
    {"name": "currentQuantity", "data_type": "bigint"},
    {"name": "nonFulfillableQuantity", "data_type": "bigint"},
    # Booleans
    {"name": "taxable", "data_type": "bool"},
    {"name": "isGiftCard", "data_type": "bool"},
    {"name": "requiresShipping", "data_type": "bool"},
    {"name": "merchantEditable", "data_type": "bool"},
    {"name": "restockable", "data_type": "bool"},
    # Status
    {"name": "fulfillmentStatus", "data_type": "text"},
    # Price sets (MoneyBag → 4 cols each)
    *_money_bag_list("originalTotalSet"),
    *_money_bag_list("discountedTotalSet"),
    *_money_bag_list("totalDiscountSet"),
]


# =============================================================================
# REFUND COLUMNS — child of order (refunds)
# =============================================================================

REFUND_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "orderId", "data_type": "bigint", "nullable": False},
    {"name": "createdAt", "data_type": "timestamp"},
    {"name": "note", "data_type": "text"},  # hashed PII
    # Return info (flattened from nested dict)
    {"name": "return__id", "data_type": "bigint"},
    {"name": "return__status", "data_type": "text"},
    # Total refunded (MoneyBag)
    *_money_bag_list("totalRefundedSet"),
]


# =============================================================================
# REFUND LINE ITEM COLUMNS — child of refund (refundLineItems)
# =============================================================================

REFUND_LINE_ITEM_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "refundId", "data_type": "bigint", "nullable": False},
    {"name": "quantity", "data_type": "bigint"},
    {"name": "subtotal", "data_type": "double"},
    {"name": "totalTax", "data_type": "double"},
    {"name": "restockType", "data_type": "text"},
    # Line item ref (flattened)
    {"name": "lineItem__id", "data_type": "bigint"},
    # Price sets
    *_money_bag_list("subtotalSet"),
    *_money_bag_list("totalTaxSet"),
]


# =============================================================================
# ORDER ADJUSTMENT COLUMNS — child of refund (orderAdjustments)
# =============================================================================

ORDER_ADJUSTMENT_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "refundId", "data_type": "bigint", "nullable": False},
    {"name": "reason", "data_type": "text"},
    # Price sets
    *_money_bag_list("amountSet"),
    *_money_bag_list("taxAmountSet"),
]


# =============================================================================
# CUSTOMER TABLE — parent columns
# =============================================================================

CUSTOMER_COLUMNS = {
    # PK
    "id": {"data_type": "bigint", "nullable": False},
    # --- Shop identity (injected by source for multi-shop clustering) ---
    "shopId": {"data_type": "bigint", "nullable": False},
    # PII (hashed)
    "firstName": {"data_type": "text", "nullable": True},
    "lastName": {"data_type": "text", "nullable": True},
    "email": {"data_type": "text", "nullable": True},
    "phone": {"data_type": "text", "nullable": True},
    "note": {"data_type": "text", "nullable": True},
    # Email marketing (flattened from defaultEmailAddress by transform)
    "emailMarketingConsentState": {"data_type": "text", "nullable": True},
    "emailMarketingOptInLevel": {"data_type": "text", "nullable": True},
    "emailMarketingConsentUpdatedAt": {"data_type": "timestamp", "nullable": True},
    # Status
    "state": {"data_type": "text", "nullable": True},
    "verifiedEmail": {"data_type": "bool", "nullable": True},
    "taxExempt": {"data_type": "bool", "nullable": True},
    "canDelete": {"data_type": "bool", "nullable": True},
    # Timestamps
    "createdAt": {"data_type": "timestamp", "nullable": True},
    "updatedAt": {"data_type": "timestamp", "nullable": True},
    # Stats (flattened from amountSpent by transform)
    "numberOfOrders": {"data_type": "text", "nullable": True},
    "lifetimeDuration": {"data_type": "text", "nullable": True},
    "totalSpent": {"data_type": "double", "nullable": True},
    "totalSpentCurrency": {"data_type": "text", "nullable": True},
    # Audit
    "_loaded_at": {"data_type": "timestamp", "nullable": True},
}


# =============================================================================
# PRODUCT TABLE — parent columns
# =============================================================================

PRODUCT_COLUMNS = {
    "id": {"data_type": "bigint", "nullable": False},
    "shopId": {"data_type": "bigint", "nullable": False},
    "legacyResourceId": {"data_type": "text", "nullable": True},
    "title": {"data_type": "text", "nullable": True},
    "handle": {"data_type": "text", "nullable": True},
    "description": {"data_type": "text", "nullable": True},
    "descriptionHtml": {"data_type": "text", "nullable": True},
    "productType": {"data_type": "text", "nullable": True},
    "status": {"data_type": "text", "nullable": True},
    "vendor": {"data_type": "text", "nullable": True},
    "templateSuffix": {"data_type": "text", "nullable": True},
    "totalInventory": {"data_type": "bigint", "nullable": True},
    "createdAt": {"data_type": "timestamp", "nullable": True},
    "updatedAt": {"data_type": "timestamp", "nullable": True},
    "publishedAt": {"data_type": "timestamp", "nullable": True},
    "featuredImage__id": {"data_type": "bigint", "nullable": True},
    "featuredImage__altText": {"data_type": "text", "nullable": True},
    "featuredImage__url": {"data_type": "text", "nullable": True},
    "featuredImage__width": {"data_type": "bigint", "nullable": True},
    "featuredImage__height": {"data_type": "bigint", "nullable": True},
    "_loaded_at": {"data_type": "timestamp", "nullable": True},
}

PRODUCT_VARIANT_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "productId", "data_type": "bigint", "nullable": False},
    {"name": "title", "data_type": "text"},
    {"name": "sku", "data_type": "text"},
    {"name": "barcode", "data_type": "text"},
    {"name": "position", "data_type": "bigint"},
    {"name": "price", "data_type": "double"},
    {"name": "compareAtPrice", "data_type": "double"},
    {"name": "taxable", "data_type": "bool"},
    {"name": "inventoryPolicy", "data_type": "text"},
    {"name": "inventoryQuantity", "data_type": "bigint"},
    {"name": "createdAt", "data_type": "timestamp"},
    {"name": "updatedAt", "data_type": "timestamp"},
    {"name": "image__id", "data_type": "bigint"},
    {"name": "image__altText", "data_type": "text"},
    {"name": "image__url", "data_type": "text"},
    {"name": "image__width", "data_type": "bigint"},
    {"name": "image__height", "data_type": "bigint"},
]

PRODUCT_VARIANT_SELECTED_OPTION_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "variantId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "name", "data_type": "text"},
    {"name": "value", "data_type": "text"},
]

PRODUCT_IMAGE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "productId", "data_type": "bigint", "nullable": False},
    {"name": "altText", "data_type": "text"},
    {"name": "url", "data_type": "text"},
    {"name": "width", "data_type": "bigint"},
    {"name": "height", "data_type": "bigint"},
]

PRODUCT_OPTION_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "productId", "data_type": "bigint", "nullable": False},
    {"name": "name", "data_type": "text"},
    {"name": "position", "data_type": "bigint"},
]

PRODUCT_OPTION_VALUE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "id", "data_type": "bigint"},
    {"name": "productOptionId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "name", "data_type": "text"},
    {"name": "hasVariants", "data_type": "bool"},
]

PRODUCT_TAG_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "productId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "value", "data_type": "text"},
]


# =============================================================================
# COLLECTION TABLE — parent columns
# =============================================================================

COLLECTION_COLUMNS = {
    "id": {"data_type": "bigint", "nullable": False},
    "shopId": {"data_type": "bigint", "nullable": False},
    "title": {"data_type": "text", "nullable": True},
    "handle": {"data_type": "text", "nullable": True},
    "description": {"data_type": "text", "nullable": True},
    "descriptionHtml": {"data_type": "text", "nullable": True},
    "sortOrder": {"data_type": "text", "nullable": True},
    "templateSuffix": {"data_type": "text", "nullable": True},
    "updatedAt": {"data_type": "timestamp", "nullable": True},
    "collectionType": {"data_type": "text", "nullable": True},
    "ruleSetAppliedDisjunctively": {"data_type": "bool", "nullable": True},
    "image__id": {"data_type": "bigint", "nullable": True},
    "image__url": {"data_type": "text", "nullable": True},
    "image__altText": {"data_type": "text", "nullable": True},
    "image__width": {"data_type": "bigint", "nullable": True},
    "image__height": {"data_type": "bigint", "nullable": True},
    "productsCount__count": {"data_type": "bigint", "nullable": True},
    "productsCount__precision": {"data_type": "text", "nullable": True},
    "seo__title": {"data_type": "text", "nullable": True},
    "seo__description": {"data_type": "text", "nullable": True},
    "_loaded_at": {"data_type": "timestamp", "nullable": True},
}

COLLECTION_RULE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "collectionId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "column", "data_type": "text"},
    {"name": "relation", "data_type": "text"},
    {"name": "condition", "data_type": "text"},
]

COLLECTION_PRODUCT_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "collectionId", "data_type": "bigint", "nullable": False},
    {"name": "productId", "data_type": "bigint", "nullable": False},
    {"name": "_loaded_at", "data_type": "timestamp", "nullable": True},
]


# =============================================================================
# DISCOUNT APPLICATION COLUMNS — child of order (discountApplications)
# =============================================================================

DISCOUNT_APPLICATION_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "orderId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "allocationMethod", "data_type": "text"},
    {"name": "targetSelection", "data_type": "text"},
    {"name": "targetType", "data_type": "text"},
    {"name": "code", "data_type": "text"},
    {"name": "title", "data_type": "text"},
    {"name": "description", "data_type": "text"},
    # Normalized from polymorphic value field by transform
    {"name": "valueType", "data_type": "text"},
    {"name": "valuePercentage", "data_type": "double"},
    {"name": "valueAmount", "data_type": "double"},
    {"name": "valueCurrencyCode", "data_type": "text"},
]


# =============================================================================
# TAX LINE COLUMNS — child of order (taxLines)
# =============================================================================

TAX_LINE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "orderId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "title", "data_type": "text"},
    {"name": "rate", "data_type": "double"},
    {"name": "ratePercentage", "data_type": "double"},
    {"name": "channelLiable", "data_type": "bool"},
    *_money_bag_list("priceSet"),
]


# =============================================================================
# SHIPPING LINE COLUMNS — child of order (shippingLines)
# =============================================================================

SHIPPING_LINE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
    {"name": "orderId", "data_type": "bigint", "nullable": False},
    {"name": "title", "data_type": "text"},
    {"name": "code", "data_type": "text"},
    {"name": "carrierIdentifier", "data_type": "text"},
    {"name": "deliveryCategory", "data_type": "text"},
    {"name": "source", "data_type": "text"},
    {"name": "phone", "data_type": "text"},
    *_money_bag_list("originalPriceSet"),
    *_money_bag_list("discountedPriceSet"),
    *_money_bag_list("currentDiscountedPriceSet"),
]


# =============================================================================
# SHIPPING LINE TAX LINE COLUMNS — child of shippingLine (taxLines)
# =============================================================================

SHIPPING_TAX_LINE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "shippingLineId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "title", "data_type": "text"},
    {"name": "rate", "data_type": "double"},
    {"name": "ratePercentage", "data_type": "double"},
    *_money_bag_list("priceSet"),
]


# =============================================================================
# DISCOUNT ALLOCATION COLUMNS — child of lineItem (discountAllocations)
# =============================================================================

DISCOUNT_ALLOCATION_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "lineItemId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    *_money_bag_list("allocatedAmountSet"),
    {"name": "discountApplication__allocationMethod", "data_type": "text"},
    {"name": "discountApplication__targetSelection", "data_type": "text"},
    {"name": "discountApplication__targetType", "data_type": "text"},
]


# =============================================================================
# LINE ITEM TAX LINE COLUMNS — child of lineItem (taxLines)
# =============================================================================

LINE_ITEM_TAX_LINE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "lineItemId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "title", "data_type": "text"},
    {"name": "rate", "data_type": "double"},
    *_money_bag_list("priceSet"),
]


# =============================================================================
# CUSTOM ATTRIBUTE COLUMNS — child of lineItem (customAttributes)
# =============================================================================

CUSTOM_ATTRIBUTE_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "lineItemId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "key", "data_type": "text"},
    {"name": "value", "data_type": "text"},
]


# =============================================================================
# ASSEMBLED HINTS — used by @dlt.resource decorators via **HINTS spread
# =============================================================================

ORDER_HINTS = {
    "columns": ORDER_COLUMNS,
    "nested_hints": {
        # --- 1st-level children with natural ID ---
        "lineItems": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=LINE_ITEM_COLUMNS,
        ),
        "refunds": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=REFUND_COLUMNS,
        ),
        "shippingLines": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=SHIPPING_LINE_COLUMNS,
        ),
        # --- 1st-level children without natural ID ---
        "discountApplications": dlt.mark.make_nested_hints(
            primary_key=["shopId", "orderId", "index"],
            write_disposition={"disposition": "merge"},
            columns=DISCOUNT_APPLICATION_COLUMNS,
        ),
        "taxLines": dlt.mark.make_nested_hints(
            primary_key=["shopId", "orderId", "index"],
            write_disposition={"disposition": "merge"},
            columns=TAX_LINE_COLUMNS,
        ),
        # --- 2nd-level grandchildren ---
        ("refunds", "refundLineItems"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=REFUND_LINE_ITEM_COLUMNS,
        ),
        ("refunds", "orderAdjustments"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=ORDER_ADJUSTMENT_COLUMNS,
        ),
        ("lineItems", "taxLines"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "lineItemId", "index"],
            write_disposition={"disposition": "merge"},
            columns=LINE_ITEM_TAX_LINE_COLUMNS,
        ),
        ("lineItems", "discountAllocations"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "lineItemId", "index"],
            write_disposition={"disposition": "merge"},
            columns=DISCOUNT_ALLOCATION_COLUMNS,
        ),
        ("lineItems", "customAttributes"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "lineItemId", "index"],
            write_disposition={"disposition": "merge"},
            columns=CUSTOM_ATTRIBUTE_COLUMNS,
        ),
        ("shippingLines", "taxLines"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "shippingLineId", "index"],
            write_disposition={"disposition": "merge"},
            columns=SHIPPING_TAX_LINE_COLUMNS,
        ),
    },
}

CUSTOMER_TAG_COLUMNS = [
    {"name": "shopId", "data_type": "bigint", "nullable": False},
    {"name": "customerId", "data_type": "bigint", "nullable": False},
    {"name": "index", "data_type": "bigint", "nullable": False},
    {"name": "value", "data_type": "text"},
]

CUSTOMER_HINTS = {
    "columns": CUSTOMER_COLUMNS,
    "nested_hints": {
        "tags": dlt.mark.make_nested_hints(
            primary_key=["shopId", "customerId", "index"],
            write_disposition={"disposition": "merge"},
            columns=CUSTOMER_TAG_COLUMNS,
        ),
    },
}

PRODUCT_HINTS = {
    "columns": PRODUCT_COLUMNS,
    "nested_hints": {
        "variants": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_VARIANT_COLUMNS,
        ),
        "images": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_IMAGE_COLUMNS,
        ),
        "options": dlt.mark.make_nested_hints(
            primary_key=["shopId", "id"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_OPTION_COLUMNS,
        ),
        "tags": dlt.mark.make_nested_hints(
            primary_key=["shopId", "productId", "index"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_TAG_COLUMNS,
        ),
        ("variants", "selectedOptions"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "variantId", "index"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_VARIANT_SELECTED_OPTION_COLUMNS,
        ),
        ("options", "optionValues"): dlt.mark.make_nested_hints(
            primary_key=["shopId", "productOptionId", "index"],
            write_disposition={"disposition": "merge"},
            columns=PRODUCT_OPTION_VALUE_COLUMNS,
        ),
    },
}

COLLECTION_HINTS = {
    "columns": COLLECTION_COLUMNS,
    "nested_hints": {
        "rules": dlt.mark.make_nested_hints(
            primary_key=["shopId", "collectionId", "index"],
            write_disposition={"disposition": "merge"},
            columns=COLLECTION_RULE_COLUMNS,
        ),
    },
}


# =============================================================================
# SHOP SCHEMA (1 TABLE) — kept as-is, shop.py does manual mapping
# =============================================================================

SHOP_COLUMNS = {
    # Core Identifiers
    "id": {"data_type": "bigint", "nullable": False},
    # Basic Info
    "name": {"data_type": "text", "nullable": True},
    "email": {"data_type": "text", "nullable": True},
    "domain": {"data_type": "text", "nullable": True},
    "myshopify_domain": {"data_type": "text", "nullable": True},
    # Currency
    "currency": {"data_type": "text", "nullable": True},
    "enabled_presentment_currencies": {"data_type": "text", "nullable": True},
    # Timezone
    "iana_timezone": {"data_type": "text", "nullable": True},
    "timezone_abbreviation": {"data_type": "text", "nullable": True},
    # Plan
    "plan_name": {"data_type": "text", "nullable": True},
    "plan_partner_development": {"data_type": "bool", "nullable": True},
    "plan_shopify_plus": {"data_type": "bool", "nullable": True},
    # Settings
    "checkout_api_supported": {"data_type": "bool", "nullable": True},
    "taxes_included": {"data_type": "bool", "nullable": True},
    "tax_shipping": {"data_type": "bool", "nullable": True},
    "customer_accounts": {"data_type": "text", "nullable": True},
    # Timestamps
    "created_at": {"data_type": "timestamp", "nullable": True},
    "updated_at": {"data_type": "timestamp", "nullable": True},
    # Contact
    "contact_email": {"data_type": "text", "nullable": True},
    "description": {"data_type": "text", "nullable": True},
    # Currency Formats
    "money_format": {"data_type": "text", "nullable": True},
    "money_in_emails_format": {"data_type": "text", "nullable": True},
    "money_with_currency_format": {"data_type": "text", "nullable": True},
    "money_with_currency_in_emails_format": {"data_type": "text", "nullable": True},
    # Features (JSON)
    "features_json": {"data_type": "text", "nullable": True},
    # Feature Flags
    "has_avalara_avatax": {"data_type": "bool", "nullable": True},
    "branding": {"data_type": "text", "nullable": True},
    "has_captcha": {"data_type": "bool", "nullable": True},
    "has_dynamic_remarketing": {"data_type": "bool", "nullable": True},
    "eligible_for_subscription_migration": {"data_type": "bool", "nullable": True},
    "eligible_for_subscriptions": {"data_type": "bool", "nullable": True},
    "has_gift_cards_feature": {"data_type": "bool", "nullable": True},
    "has_harmonized_system_code": {"data_type": "bool", "nullable": True},
    "has_legacy_subscription_gateway": {"data_type": "bool", "nullable": True},
    "has_live_view": {"data_type": "bool", "nullable": True},
    "paypal_express_subscription_status": {"data_type": "text", "nullable": True},
    "has_reports": {"data_type": "bool", "nullable": True},
    "sells_subscriptions": {"data_type": "bool", "nullable": True},
    "show_metrics": {"data_type": "bool", "nullable": True},
    "has_storefront_feature": {"data_type": "bool", "nullable": True},
    "has_unified_markets": {"data_type": "bool", "nullable": True},
    # Bundles
    "bundles_eligible": {"data_type": "bool", "nullable": True},
    "bundles_ineligibility_reason": {"data_type": "text", "nullable": True},
    "bundles_sells": {"data_type": "bool", "nullable": True},
    # Cart Transform
    "cart_transform_expand": {"data_type": "bool", "nullable": True},
    "cart_transform_merge": {"data_type": "bool", "nullable": True},
    "cart_transform_update": {"data_type": "bool", "nullable": True},
    # Currency Settings
    "currency_settings": {"data_type": "text", "nullable": True},
    # Audit
    "source": {"data_type": "text", "nullable": True},
    "_loaded_at": {"data_type": "timestamp", "nullable": True},
}
