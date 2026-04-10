"""Input validation utilities."""

import re

_SAFE_DB_NAME = re.compile(r'^[A-Za-z0-9_\-]+$')


def validate_database_name(name):
    """Reject database names that contain characters unsafe for XQuery interpolation."""
    if not _SAFE_DB_NAME.match(name):
        raise ValueError(
            f"Invalid database name '{name}': "
            "only alphanumeric characters, hyphens, and underscores are allowed"
        )
