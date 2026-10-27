import type { Effect } from "effect";
import type { Db } from "pg-boss";

import type { PgBossError } from "./pgBossInternal";

export interface PgBossTransaction {
  readonly db: Db;
  readonly executeSql: (
    text: string,
    values?: ReadonlyArray<unknown>,
  ) => Effect.Effect<ReadonlyArray<unknown>, PgBossError>;
}
