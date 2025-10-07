"""Shared internal type aliases for backend components."""

from __future__ import annotations

from collections.abc import Iterable

# Lambda data structures
JsonObject = dict[str, object]
LambdaEvent = JsonObject
LambdaContext = object
LambdaResponse = JsonObject

# SQL helpers
SqlParameter = dict[str, object]
SqlParameters = Iterable[SqlParameter]
SqlResult = dict[str, object]
