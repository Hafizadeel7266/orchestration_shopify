"""Custom exceptions for Shopify DLT pipeline."""

class ShopifyAPIError(Exception):
    """Base exception for Shopify API errors."""
    pass

class RateLimitError(ShopifyAPIError):
    """Raised when Shopify API rate limit is hit."""
    pass

class AuthenticationError(ShopifyAPIError):
    """Raised when authentication fails."""
    pass

class GraphQLError(ShopifyAPIError):
    """Raised when GraphQL query returns errors."""
    pass

class DataExtractionError(Exception):
    """Raised when data extraction fails."""
    pass