import { Data, Effect, Layer, Schema } from "effect";
import type {
  Job as PgBossJob,
  Queue as PgBossQueue,
  SendOptions,
} from "pg-boss";

import {
  type PgBossError,
  PgBossInternal,
  type QueueNotFoundError,
} from "./pgBossInternal";

export class QueuePayloadSchemaError extends Data.TaggedError(
  "QueuePayloadSchemaError",
)<{
  queue: string;
  cause: Schema.SchemaError;
}> {}

export type QueueError =
  | PgBossError
  | QueueNotFoundError
  | QueuePayloadSchemaError;

export type TypedJob<A> = Omit<PgBossJob, "data"> & { data: A };

export interface Queue<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
> {
  readonly name: Name;
  readonly payloadSchema: Payload;
  readonly queueOptions: Omit<PgBossQueue, "name"> | undefined;
  readonly toLayer: Layer.Layer<never, PgBossError, PgBossInternal>;
  readonly send: (
    data: Payload["Type"],
    options?: SendOptions,
  ) => Effect.Effect<string | null, QueueError, PgBossInternal>;
}

export const Queue = {
  make: <
    const Name extends string,
    Payload extends Schema.Codec<unknown, object | null>,
  >(
    name: Name,
    options: {
      readonly payload: Payload;
      readonly queueOptions?: Omit<PgBossQueue, "name">;
    },
  ): Queue<Name, Payload> => {
    const register = (): Effect.Effect<void, PgBossError, PgBossInternal> =>
      Effect.gen(function* () {
        const { createQueue } = yield* PgBossInternal;
        return yield* createQueue(name, options.queueOptions);
      });

    const toLayer = Layer.effectDiscard(register());

    return {
      name,
      payloadSchema: options.payload,
      queueOptions: options.queueOptions,
      toLayer,
      send: (data: Payload["Type"], sendOptions?: SendOptions) =>
        Effect.gen(function* () {
          const { send } = yield* PgBossInternal;
          const encoded = yield* Schema.encodeEffect(options.payload)(
            data,
          ).pipe(
            Effect.mapError(
              (cause) => new QueuePayloadSchemaError({ queue: name, cause }),
            ),
          );
          return yield* send(name, encoded, sendOptions);
        }),
    } as const;
  },
};
