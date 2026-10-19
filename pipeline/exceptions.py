"""Custom exceptions so every failure says clearly what went wrong and where."""


class EtlError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(EtlError):
    """Invalid configuration, e.g. a badly formatted month or a missing setting."""


class SourceFileError(EtlError):
    """A source file is missing, empty or not a readable Parquet file."""


class SchemaError(EtlError):
    """A source file lacks a required column or has values that cannot be converted."""


class DataQualityError(EtlError):
    """A critical data quality check failed; the month is not published."""


class LoadError(EtlError):
    """Writing to the database failed."""


class DatabaseUnavailableError(LoadError):
    """The database cannot be reached. Temporary by nature, so Prefect retries it
    (data errors are never retried: running again would not fix bad data)."""
