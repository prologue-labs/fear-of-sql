# Changelog

## [0.3.1] - 2026-03-06

- Remove type override suffixes during execution

## [0.3.0] - 2026-03-06

- Fix validation for optional types in query args
- Fix validation for queries when types are stringified
- Add logging integration support
- Add exception type for `fetch_one()`
- `validate_all()` now returns the number of validated queries

## [0.2.1] - 2026-02-28

- Update type annotations for SQLAlchemy async support

## [0.2.0] - 2026-02-28

- SQLAlchemy async support — `Query.fetch_one()`, `fetch_all()`, `fetch_optional()`, and `Execute.execute()` now accept `AsyncSession` and `AsyncConnection` from `sqlalchemy.ext.asyncio`.

## [0.1.0] - 2026-02-21

Initial release.

[0.3.1]: https://github.com/prologue-labs/fear-of-sql/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/prologue-labs/fear-of-sql/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/prologue-labs/fear-of-sql/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/prologue-labs/fear-of-sql/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/prologue-labs/fear-of-sql/releases/tag/v0.1.0
