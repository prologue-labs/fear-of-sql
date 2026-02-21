class ValidationError(Exception):
    """Base for query validation errors."""

    query_name: str | None = None
    sql: str | None = None


class ColumnCountMismatchError(ValidationError):
    def __init__(
        self,
        expected: int,
        actual: int,
    ) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(f"expected {expected} column(s), got {actual}")


class ColumnNotFoundError(ValidationError):
    def __init__(self, column: str) -> None:
        self.column = column
        super().__init__(f"column {column!r} not found in query results")


class TypeMismatchError(ValidationError):
    def __init__(
        self,
        column: str,
        expected: list[type],
        actual: type,
    ) -> None:
        self.column = column
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"column {column!r}: expected"
            f" {[t.__name__ for t in expected]},"
            f" got {actual.__name__}"
        )


class NullabilityError(ValidationError):
    def __init__(self, column: str) -> None:
        self.column = column
        super().__init__(
            f"column {column!r} is nullable"
            " but type does not allow None"
        )


class UnsupportedTypeError(Exception):
    def __init__(
        self,
        type_oid: int,
        column: str,
    ) -> None:
        self.type_oid = type_oid
        self.column = column
        super().__init__(
            f"unsupported PostgreSQL type OID"
            f" {type_oid} for column {column!r}"
        )
