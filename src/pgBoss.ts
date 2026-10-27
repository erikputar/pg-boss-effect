import { Context, Effect, Layer } from "effect";

import {
  PgBossInternal,
  type PgBossOptions,
  type PgBossService,
  makeRuntime,
} from "./pgBossInternal";

export {
  PgBossError,
  type PgBossEventPayload,
  type PgBossEvents,
  type PgBossOptions,
  type PgBossService,
  QueueNotFoundError,
} from "./pgBossInternal";

export class PgBoss extends Context.Service<PgBoss, PgBossService>()("PgBoss") {
  static layer(options: PgBossOptions) {
    return Layer.unwrap(
      Effect.map(makeRuntime(options), (runtime) =>
        Layer.mergeAll(
          Layer.succeed(PgBoss, runtime.public),
          Layer.succeed(PgBossInternal, runtime.internal),
        ),
      ),
    );
  }
}
