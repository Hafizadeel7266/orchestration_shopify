"""GraphQL query for fetching shop information."""

SHOP_QUERY = """
query GetShop {
  shop {
    id
    name
    email
    myshopifyDomain
    currencyCode
    ianaTimezone
    timezoneAbbreviation
    primaryDomain {
      host
      id
    }
    
    plan {
      partnerDevelopment
      shopifyPlus
    }
    enabledPresentmentCurrencies
    checkoutApiSupported
    taxesIncluded
    taxShipping
    createdAt
    updatedAt
    contactEmail
    description
    currencyFormats {
      moneyFormat
      moneyInEmailsFormat
      moneyWithCurrencyFormat
      moneyWithCurrencyInEmailsFormat
    }
    features {
      avalaraAvatax
      branding
      bundles {
        eligibleForBundles
        ineligibilityReason
        sellsBundles
      }
      captcha
      cartTransform {
        eligibleOperations {
          expandOperation
          mergeOperation
          updateOperation
        }
      }
      dynamicRemarketing
      eligibleForSubscriptionMigration
      eligibleForSubscriptions
      giftCards
      harmonizedSystemCode
      legacySubscriptionGatewayEnabled
      liveView
      paypalExpressSubscriptionGatewayStatus
      reports
      sellsSubscriptions
      showMetrics
      storefront
      unifiedMarkets
    }
    currencySettings(first: 10) {
      edges {
        node {
          currencyCode
          currencyName
          enabled
          rateUpdatedAt
        }
      }
    }
    customerAccounts
  }
}
"""