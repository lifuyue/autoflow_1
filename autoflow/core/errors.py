"""Custom exceptions used across AutoFlow."""


class AutoFlowError(Exception):
    """Base error for the application."""


class ConfigError(AutoFlowError):
    """Configuration related error."""


class CredentialsError(AutoFlowError):
    """Credentials acquisition or decryption failure."""


class DownloadError(AutoFlowError):
    """Raised when download fails."""


class TransformError(AutoFlowError):
    """Raised when data transform or templating fails."""


class UploadError(AutoFlowError):
    """Raised when upload fails."""


class BrowserError(AutoFlowError):
    """Raised when browser automation fails."""

