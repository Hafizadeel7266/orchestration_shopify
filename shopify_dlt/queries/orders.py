"""GraphQL queries for fetching Shopify orders."""

ORDER_NODE_FIELDS = """
        # Core fields
        id
        legacyResourceId
        name
        confirmed
        createdAt
        processedAt
        updatedAt
        statusPageUrl
        currencyCode
        presentmentCurrencyCode
        email
        phone
        note
        tags
        taxesIncluded
        test
        totalWeight
        displayFinancialStatus
        displayFulfillmentStatus
        cancelReason
        cancelledAt
        closedAt
        customerLocale
        paymentGatewayNames
        sourceIdentifier
        sourceName
        discountCode
        discountCodes
        canMarkAsPaid
        canNotifyCustomer
        capturable
        registeredSourceUrl
        
        # Customer
        customer {
          id
          email
        }
        
        # App
        app {
          id
          name
        }
        
        # Addresses
        billingAddress {
          company
          city
          province
          provinceCode
          country
          countryCodeV2
          zip
          phone
        }
        shippingAddress {
          company
          city
          province
          provinceCode
          country
          countryCodeV2
          zip
          phone
        }
        
        # Price fields
        subtotalPrice
        totalDiscounts
        totalPrice
        totalShippingPrice
        totalTax
        
        # Price Sets
        subtotalPriceSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalDiscountsSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalPriceSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalShippingPriceSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalTaxSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalReceivedSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalRefundedSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        totalTipReceived {
          amount
          currencyCode
        }
        originalTotalDutiesSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        
        # Current price sets
        currentTotalPriceSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        currentTotalDutiesSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        currentShippingPriceSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        cartDiscountAmountSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }
        currentCartDiscountAmountSet {
          shopMoney { amount currencyCode }
          presentmentMoney { amount currencyCode }
        }

        # Discount Applications (order-level)
        discountApplications(first: 20) {
          edges {
            node {
              allocationMethod
              targetSelection
              targetType
              value {
                ... on MoneyV2 {
                  amount
                  currencyCode
                }
                ... on PricingPercentageValue {
                  percentage
                }
              }
              ... on DiscountCodeApplication {
                code
              }
              ... on AutomaticDiscountApplication {
                title
              }
              ... on ManualDiscountApplication {
                title
                description
              }
              ... on ScriptDiscountApplication {
                title
              }
            }
          }
        }

        # Tax Lines (order-level)
        taxLines {
          title
          rate
          ratePercentage
          priceSet {
            shopMoney { amount currencyCode }
            presentmentMoney { amount currencyCode }
          }
          channelLiable
        }

        # Shipping Lines
        shippingLines(first: 10) {
          edges {
            node {
              id
              title
              code
              carrierIdentifier
              deliveryCategory
              source
              phone
              requestedFulfillmentService {
                id
              }
              originalPriceSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              discountedPriceSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              currentDiscountedPriceSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              taxLines {
                title
                rate
                ratePercentage
                priceSet {
                  shopMoney { amount currencyCode }
                  presentmentMoney { amount currencyCode }
                }
              }
            }
          }
        }

        # Customer Journey
        customerJourneySummary {
          customerOrderIndex
          daysToConversion
          ready
          momentsCount {
            count
            precision
          }
          firstVisit {
            id
            landingPage
            occurredAt
            referralCode
            referralInfoHtml
            referrerUrl
            source
            sourceDescription
            sourceType
            utmParameters {
              campaign
              content
              medium
              source
              term
            }
          }
          lastVisit {
            id
            landingPage
            occurredAt
            referralCode
            referralInfoHtml
            referrerUrl
            source
            sourceDescription
            sourceType
            utmParameters {
              campaign
              content
              medium
              source
              term
            }
          }
        }
        
        # Line Items
        lineItems(first: 250) {
          edges {
            node {
              id
              product {
                id
                title
                productType
                vendor
              }
              variant {
                id
                title
                sku
                price
                compareAtPrice
              }
              title
              quantity
              sku
              vendor
              taxable
              isGiftCard
              requiresShipping
              name
              fulfillableQuantity
              refundableQuantity
              unfulfilledQuantity
              currentQuantity
              nonFulfillableQuantity
              originalTotalSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              discountedTotalSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              totalDiscountSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              taxLines {
                title
                rate
                priceSet {
                  shopMoney { amount currencyCode }
                  presentmentMoney { amount currencyCode }
                }
              }
              customAttributes {
                key
                value
              }
              discountAllocations {
                allocatedAmountSet {
                  shopMoney { amount currencyCode }
                  presentmentMoney { amount currencyCode }
                }
                discountApplication {
                  allocationMethod
                  targetSelection
                  targetType
                  value {
                    ... on MoneyV2 {
                      amount
                      currencyCode
                    }
                    ... on PricingPercentageValue {
                      percentage
                    }
                  }
                }
              }
              fulfillmentStatus
              merchantEditable
              restockable
            }
            cursor
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
        
        # Refunds
        refunds {
          id
          createdAt
          note
          totalRefundedSet {
            shopMoney { amount currencyCode }
            presentmentMoney { amount currencyCode }
          }
          return {
            id
            status
          }
          refundLineItems(first: 30) {
            edges {
              node {
                id
                lineItem {
                  id
                }
                quantity
                subtotal
                subtotalSet {
                  shopMoney { amount currencyCode }
                  presentmentMoney { amount currencyCode }
                }
                totalTax
                totalTaxSet {
                  shopMoney { amount currencyCode }
                  presentmentMoney { amount currencyCode }
                }
                restockType
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
          orderAdjustments(first: 20) {
            nodes {
              id
              amountSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              taxAmountSet {
                shopMoney { amount currencyCode }
                presentmentMoney { amount currencyCode }
              }
              reason
            }
          }
          
        }
"""


ORDERS_QUERY = f"""
query GetOrders($first: Int!, $after: String, $query: String) {{
  orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {{
    pageInfo {{
      hasNextPage
      endCursor
    }}
    edges {{
      cursor
      node {{
{ORDER_NODE_FIELDS}
      }}
    }}
  }}
}}
"""


ORDERS_BY_IDS_QUERY = f"""
query GetOrdersByIds($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Order {{
{ORDER_NODE_FIELDS}
    }}
  }}
}}
"""
