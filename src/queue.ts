import { Data, Effect, Layer, Schema } from "effect";
import type {
  Job,
  JobWithMetadata,
  Queue as PgBossQueue,
  Schedule,
  ScheduleOptions,
  SendOptions,
} from "pg-boss";

import type {
  DeadLetterQueueNotFoundError,
  InvalidScheduleError,
} from "./errors";
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
  | QueuePayloadSchemaError
  | DeadLetterQueueNotFoundError;

export type QueueScheduleError = QueueError | InvalidScheduleError;

export type TypedJob<A> = Omit<Job, "data"> & { data: A };

export type TypedJobWithMetadata<A> = Omit<JobWithMetadata, "data"> & {
  data: A;
};

export interface QueueSchedule<Payload> {
  readonly cron: string;
  readonly data: Payload;
  readonly options?: Omit<ScheduleOptions, "key">;
}

export interface Queue<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
> {
  readonly name: Name;
  readonly payloadSchema: Payload;
  readonly queueOptions: Omit<PgBossQueue, "name"> | undefined;
  readonly schedules:
    | Readonly<Record<string, QueueSchedule<Payload["Type"]>>>
    | undefined;
  readonly toLayer: Layer.Layer<never, QueueScheduleError, PgBossInternal>;
  readonly send: (
    data: Payload["Type"],
    options?: SendOptions,
  ) => Effect.Effect<string | null, QueueError, PgBossInternal>;
  readonly getSchedules: () => Effect.Effect<
    ReadonlyArray<Schedule>,
    PgBossError,
    PgBossInternal
  >;
  readonly getSchedule: (
    key?: string,
  ) => Effect.Effect<Schedule | null, PgBossError, PgBossInternal>;
  readonly unschedule: (
    key?: string,
  ) => Effect.Effect<void, PgBossError, PgBossInternal>;
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
      readonly schedules?: Readonly<
        Record<string, QueueSchedule<Payload["Type"]>>
      >;
    },
  ): Queue<Name, Payload> => {
    const register = (): Effect.Effect<
      void,
      QueueScheduleError,
      PgBossInternal
    > =>
      Effect.gen(function* () {
        const { createQueue, schedule } = yield* PgBossInternal;
        yield* createQueue(name, options.queueOptions);

        yield* Effect.forEach(
          Object.entries(options.schedules ?? {}),
          ([key, definition]) =>
            Effect.gen(function* () {
              const encoded = yield* Schema.encodeEffect(options.payload)(
                definition.data,
              ).pipe(
                Effect.mapError(
                  (cause) =>
                    new QueuePayloadSchemaError({ queue: name, cause }),
                ),
              );
              yield* schedule(name, definition.cron, encoded, {
                ...definition.options,
                key,
              });
            }),
        ).pipe(Effect.asVoid);
      });

    const toLayer = Layer.effectDiscard(register());

    return {
      name,
      payloadSchema: options.payload,
      queueOptions: options.queueOptions,
      schedules: options.schedules,
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
      getSchedules: () =>
        Effect.gen(function* () {
          const { getSchedules } = yield* PgBossInternal;
          return yield* getSchedules(name);
        }),
      getSchedule: (key?: string) =>
        Effect.gen(function* () {
          const { getSchedule } = yield* PgBossInternal;
          return yield* getSchedule(name, key);
        }),
      unschedule: (key?: string) =>
        Effect.gen(function* () {
          const { unschedule } = yield* PgBossInternal;
          return yield* unschedule(name, key);
        }),
    } as const;
  },
};
