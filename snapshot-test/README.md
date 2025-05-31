# Snapshot test


> ![](screenshots/init.png) \
> Init

> ![](screenshots/compare-ok.png) \
> Comparison result: Same.

> ![](screenshots/compare-not-ok.png) \
> Comparison result: Different.


## Usage

```sh
PG_CONNECTION_URL="postgresql://hatter:mushrooms@127.0.0.1:5432/wonderworks" PG_TARGET_TABLE="computed_values" npm start
```

Snapshot files are written to `results/`.


## Technologies used

- Node 22

- DuckDB (`npm:@duckdb/node-api`)

- PostgreSQL 17

---

END.
