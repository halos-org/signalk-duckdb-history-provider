/**
 * Single-quoted SQL literal, escaped.
 *
 * Every path this package puts into a statement is composed from the
 * configured data directory rather than from a delta, and DuckDB has no
 * parameter binding for `COPY … TO`, `ATTACH`, `LOAD` or a `read_parquet` file
 * list. The data directory is still one string in them that a person types,
 * and this is the only thing between that string and the statement.
 *
 * Returns the inner text; the caller supplies the quotes.
 */
export function sqlLiteral(value: string): string {
  return value.replaceAll("'", "''");
}
