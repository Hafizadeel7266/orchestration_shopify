"""GraphQL queries for fetching Shopify collections and membership."""

COLLECTIONS_QUERY = """
query GetCollections($first: Int!, $after: String, $query: String) {
  collections(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        title
        handle
        description
        descriptionHtml
        sortOrder
        templateSuffix
        updatedAt
        image {
          id
          url
          altText
          width
          height
        }
        productsCount {
          count
          precision
        }
        seo {
          title
          description
        }
        ruleSet {
          appliedDisjunctively
          rules {
            column
            relation
            condition
          }
        }
      }
    }
  }
}
"""

ALL_COLLECTION_IDS_QUERY = """
query GetAllCollectionIds($first: Int!, $after: String) {
  collections(first: $first, after: $after, sortKey: ID) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
      }
    }
  }
}
"""

COLLECTION_PRODUCTS_QUERY = """
query GetCollectionProducts($id: ID!, $first: Int!, $after: String) {
  collection(id: $id) {
    id
    products(first: $first, after: $after, sortKey: ID) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
      }
    }
  }
}
"""
