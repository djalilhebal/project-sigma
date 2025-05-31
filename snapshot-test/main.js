import { existsSync } from 'node:fs';
import { DuckDBInstance } from '@duckdb/node-api';
import { parse as parsePg } from 'pg-connection-string';

const PG_CONNECTION_URL = process.env.PG_CONNECTION_URL ?? 'postgresql://hatter:mushrooms@127.0.0.1:5432/wonderworks';
const PG_TARGET_TABLE = process.env.PG_TARGET_TABLE ?? 'computed_values';
const pgParams = parsePg(PG_CONNECTION_URL);

async function run() {
  try {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    await connection.run(/* sql */`
      ATTACH 'dbname=${pgParams.database} user=${pgParams.user} password=${pgParams.password} host=${pgParams.host}' AS db (TYPE postgres, SCHEMA 'public');
  `);

    const baselineExists = existsSync('results/baseline.parquet');
    const action = baselineExists ? 'COMPARE' : 'INIT';

    if (action === 'INIT') {
      console.log('[snapshot-test] Baseline does not exist. Creating one and exiting...');
      const resultReader = await connection.runAndReadAll(/* sql */`
        COPY
          (SELECT * FROM db.${PG_TARGET_TABLE})
          TO 'results/baseline.parquet'
          (FORMAT parquet)
    `);
      console.debug(resultReader.getRowObjects());
      process.exit(0);
    }

    if (action === 'COMPARE') {
      const updatedFilenameWithTime = `updated-${Date.now()}.parquet`;
      const query = /* sql */`
        COPY (
          SELECT * FROM db.${PG_TARGET_TABLE}
        ) TO 'results/${updatedFilenameWithTime}' (FORMAT parquet);

        WITH diff1 AS (
          SELECT * FROM read_parquet('results/baseline.parquet')
          EXCEPT
          SELECT * FROM read_parquet('results/${updatedFilenameWithTime}')
        ),
        diff2 AS (
          SELECT * FROM read_parquet('results/${updatedFilenameWithTime}')
          EXCEPT
          SELECT * FROM read_parquet('results/baseline.parquet')
        )
        SELECT
          (SELECT COUNT(*) FROM diff1) AS only_in_baseline,
          (SELECT COUNT(*) FROM diff2) AS only_in_updated,
          (SELECT COUNT(*) FROM diff1) + (SELECT COUNT(*) FROM diff2) = 0 AS same_content
        `;
      const resultReader = await connection.runAndReadAll(query);
      const row = resultReader.getRowObjects()[0];
      const sameContent = row.same_content;
      if (sameContent) {
        console.log('[snapshot-test] ✅ Output matches baseline.');
        process.exit(0);
      } else {
        console.error('[snapshot-test] ❌ Output does not match baseline.');
        process.exit(1);
      }
    }
  } catch (err) {
    console.error('[snapshot-test] 💥 Error during diff:', err);
    process.exit(1);
  }
}

run().catch(console.error);
