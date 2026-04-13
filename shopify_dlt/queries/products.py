"""GraphQL query for fetching Shopify products."""

PRODUCTS_QUERY = """
query GetProducts($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        handle
        description
        descriptionHtml
        productType
        status
        tags
        vendor
        createdAt
        updatedAt
        publishedAt
        templateSuffix
        totalInventory
        featuredImage {
          id
          altText
          url
          width
          height
        }
        images(first: 250) {
          pageInfo {
            hasNextPage
          }
          nodes {
            id
            altText
            url
            width
            height
          }
        }
        options {
          id
          name
          position
          optionValues {
            id
            name
            hasVariants
          }
        }
        variants(first: 250) {
          pageInfo {
            hasNextPage
          }
          nodes {
            id
            title
            sku
            barcode
            position
            price
            compareAtPrice
            taxable
            inventoryPolicy
            inventoryQuantity
            createdAt
            updatedAt
            selectedOptions {
              name
              value
            }
            image {
              id
              altText
              url
              width
              height
            }
          }
        }
      }
    }
  }
}
"""
