"""GraphQL query for fetching customers."""

CUSTOMERS_QUERY = """
query GetCustomers($first: Int!, $after: String, $query: String) {
  customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        firstName
        lastName
        note
        state
        verifiedEmail
        taxExempt
        createdAt
        updatedAt
        canDelete
        lifetimeDuration
        numberOfOrders
        tags
        defaultEmailAddress {
          emailAddress
          marketingState
          marketingOptInLevel
          marketingUpdatedAt
        }
        defaultPhoneNumber {
          phoneNumber
        }
        amountSpent {
          amount
          currencyCode
        }
      }
    }
  }
}
"""