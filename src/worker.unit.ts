import { describe, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import type { Db, Job as PgBossJob, WorkOptions } from "pg-boss";
import { expect } from "vitest";

import { PgBossInternal } from "./pgBossInternal";
import { Queue, type TypedJob, type TypedJobWithMetadata } from "./queue";
import type { PgBossTransaction } from "./transaction";
import { Worker, WorkerJobPayloadSchemaError } from "./worker";

type WorkHandler = (
  jobs: PgBossJob[],
  transaction?: PgBossTransaction,
) => Promise<unknown>;

type PgBossFakeStateShape = {
  readonly createQueueCalls: Array<[string, unknown?]>;
  readonly workCalls: Array<[string, WorkOptions | undefined]>;
  readonly workHandlers: Map<string, WorkHandler>;
  readonly workErrors: Array<unknown>;
  readonly offWorkCalls: Array<[string, unknown?]>;
};

class PgBossFakeState extends Context.Service<
  PgBossFakeState,
  PgBossFakeStateShape
>()("PgBossFakeState") {}

const PgBossState = Layer.sync(PgBossFakeState, () => ({
  createQueueCalls: [],
  workCalls: [],
  workHandlers: new Map<string, WorkHandler>(),
  workErrors: [],
  offWorkCalls: [],
}));

const PgBossFake = Layer.effect(
  PgBossInternal,
  Effect.gen(function* () {
    const state = yield* PgBossFakeState;
    return PgBossInternal.of({
      createQueue: (name: string, options?: unknown) =>
        Effect.sync(() => {
          state.createQueueCalls.push([name, options]);
        }),
      send: () => Effect.succeed("job-id"),
      schedule: () => Effect.void,
      unschedule: () => Effect.void,
      getSchedules: () => Effect.succeed([]),
      getSchedule: () => Effect.succeed(null),
      offWork: (name: string, options?: unknown) =>
        Effect.sync(() => {
          state.offWorkCalls.push([name, options]);
        }),
      work: <A, E, R>(
        name: string,
        handler: (
          jobs: PgBossJob[],
          transaction: PgBossTransaction | undefined,
        ) => Effect.Effect<A, E, R>,
        options?: WorkOptions,
      ) =>
        Effect.gen(function* () {
          const context = yield* Effect.context<R>();
          state.workHandlers.set(name, (jobs, transaction) =>
            Effect.runPromiseWith(context)(
              handler(jobs, transaction).pipe(
                Effect.tapError((error) =>
                  Effect.sync(() => state.workErrors.push(error)),
                ),
              ),
            ),
          );
          state.workCalls.push([name, options]);
          return "worker-id";
        }),
    });
  }),
).pipe(Layer.provideMerge(PgBossState));

const TestPayload = Schema.Struct({
  hello: Schema.String,
});

const exampleQueue = Queue.make("example", {
  payload: TestPayload,
  queueOptions: { retryLimit: 0 },
});

const fakeJob: PgBossJob = {
  id: "job-1",
  name: "example",
  data: { hello: "world" },
  expireInSeconds: 900,
  heartbeatSeconds: null,
  retryCount: 0,
  signal: new AbortController().signal,
};

const fakeMetadataJob = {
  ...fakeJob,
  priority: 3,
  state: "created",
  retryLimit: 2,
} satisfies PgBossJob & {
  readonly priority: number;
  readonly state: "created";
  readonly retryLimit: number;
};

const fakeDb: Db = {
  executeSql: () => Promise.resolve({ rows: [] }),
};

const invokeWork = (
  queueName: string,
  jobs: PgBossJob[],
  transaction?: PgBossTransaction,
) =>
  Effect.gen(function* () {
    const state = yield* PgBossFakeState;
    const handler = state.workHandlers.get(queueName);
    if (!handler) {
      return yield* Effect.die(
        new Error(`no worker registered for queue ${queueName}`),
      );
    }
    return yield* Effect.tryPromise(() => handler(jobs, transaction));
  });

const provideFake = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(PgBossFake));

const isWorkerJobPayloadSchemaError = (
  error: unknown,
): error is WorkerJobPayloadSchemaError =>
  typeof error === "object" &&
  error !== null &&
  error instanceof WorkerJobPayloadSchemaError;

const provideWorker = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  R,
>(
  worker: Worker<Name, Payload, R>,
) => Effect.scoped(Effect.provide(Effect.void, worker.toLayer));

describe("Worker.make", () => {
  it.effect("decodes a single job into the handler", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const seen: Array<TypedJob<typeof TestPayload.Type>> = [];

        yield* provideWorker(
          Worker.make(exampleQueue, (job) => {
            seen.push(job);
            return Effect.void;
          }),
        );

        expect(state.workCalls).toEqual([["example", { batchSize: 1 }]]);

        yield* invokeWork("example", [fakeJob]);
        expect(seen).toHaveLength(1);
        expect(seen[0].data).toEqual({ hello: "world" });
        expect(seen[0].id).toBe("job-1");
      }),
    ),
  );

  it.effect("passes single-worker options through", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;

        yield* provideWorker(
          Worker.make(
            exampleQueue,
            { pollingIntervalSeconds: 2 },
            () => Effect.void,
          ),
        );

        expect(state.workCalls).toEqual([
          ["example", { pollingIntervalSeconds: 2, batchSize: 1 }],
        ]);
      }),
    ),
  );

  it.effect("passes all jobs to a batch handler", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const seen: Array<ReadonlyArray<TypedJob<typeof TestPayload.Type>>> =
          [];

        yield* provideWorker(
          Worker.make(
            exampleQueue,
            { batchSize: 2, pollingIntervalSeconds: 0.5 },
            (jobs) => {
              seen.push(jobs);
              return Effect.void;
            },
          ),
        );

        expect(state.workCalls).toEqual([
          ["example", { batchSize: 2, pollingIntervalSeconds: 0.5 }],
        ]);

        yield* invokeWork("example", [
          fakeJob,
          { ...fakeJob, id: "job-2", data: { hello: "again" } },
        ]);

        expect(seen).toHaveLength(1);
        expect(seen[0].map((job) => job.data)).toEqual([
          { hello: "world" },
          { hello: "again" },
        ]);
      }),
    ),
  );

  it.effect("passes includeMetadata and decodes metadata jobs", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const seen: Array<TypedJobWithMetadata<typeof TestPayload.Type>> = [];

        yield* provideWorker(
          Worker.make(exampleQueue, { includeMetadata: true }, (job) => {
            seen.push(job);
            return Effect.void;
          }),
        );

        expect(state.workCalls).toEqual([
          ["example", { includeMetadata: true, batchSize: 1 }],
        ]);

        yield* invokeWork("example", [fakeMetadataJob]);

        expect(seen).toHaveLength(1);
        expect(seen[0]?.state).toBe("created");
        expect(seen[0]?.priority).toBe(3);
        expect(seen[0]?.data).toEqual({ hello: "world" });
      }),
    ),
  );

  it.effect("provides PgBossTransaction to transactional handlers", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const transaction: PgBossTransaction = {
          db: fakeDb,
          executeSql: (text, values) => Effect.succeed([{ text, values }]),
        };

        yield* provideWorker(
          Worker.make(
            exampleQueue,
            { transactional: true, transactionTimeoutSeconds: 30 },
            (job, transaction) =>
              Effect.gen(function* () {
                return yield* transaction.executeSql("SELECT $1", [
                  job.data.hello,
                ]);
              }),
          ),
        );

        expect(state.workCalls).toEqual([
          [
            "example",
            {
              transactional: true,
              transactionTimeoutSeconds: 30,
              batchSize: 1,
            },
          ],
        ]);

        const rows = yield* invokeWork("example", [fakeJob], transaction);
        expect(rows).toEqual([{ text: "SELECT $1", values: ["world"] }]);
      }),
    ),
  );

  it.effect("provides PgBossTransaction to metadata handlers", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const seen: Array<TypedJobWithMetadata<typeof TestPayload.Type>> = [];
        const transaction: PgBossTransaction = {
          db: fakeDb,
          executeSql: (text, values) => Effect.succeed([{ text, values }]),
        };

        yield* provideWorker(
          Worker.make(
            exampleQueue,
            { transactional: true, includeMetadata: true },
            (job, transaction) =>
              Effect.gen(function* () {
                seen.push(job);
                return yield* transaction.executeSql("SELECT $1", [job.state]);
              }),
          ),
        );

        expect(state.workCalls).toEqual([
          [
            "example",
            { transactional: true, includeMetadata: true, batchSize: 1 },
          ],
        ]);

        const rows = yield* invokeWork(
          "example",
          [fakeMetadataJob],
          transaction,
        );
        expect(rows).toEqual([{ text: "SELECT $1", values: ["created"] }]);
        expect(seen[0]?.priority).toBe(3);
      }),
    ),
  );

  it.effect(
    "provides PgBossTransaction to transactional metadata batch handlers",
    () =>
      provideFake(
        Effect.gen(function* () {
          const state = yield* PgBossFakeState;
          const seen: Array<TypedJobWithMetadata<typeof TestPayload.Type>> = [];
          const transaction: PgBossTransaction = {
            db: fakeDb,
            executeSql: (text, values) => Effect.succeed([{ text, values }]),
          };

          yield* provideWorker(
            Worker.make(
              exampleQueue,
              {
                batchSize: 2,
                transactional: true,
                includeMetadata: true,
              },
              (jobs, transaction) =>
                Effect.gen(function* () {
                  seen.push(...jobs);
                  return yield* transaction.executeSql("SELECT $1", [
                    jobs.length,
                  ]);
                }),
            ),
          );

          expect(state.workCalls).toEqual([
            [
              "example",
              {
                batchSize: 2,
                transactional: true,
                includeMetadata: true,
              },
            ],
          ]);

          const rows = yield* invokeWork(
            "example",
            [
              fakeMetadataJob,
              {
                ...fakeMetadataJob,
                id: "job-2",
                data: { hello: "again" },
              },
            ],
            transaction,
          );
          expect(rows).toEqual([{ text: "SELECT $1", values: [2] }]);
          expect(seen).toHaveLength(2);
        }),
      ),
  );

  it.effect("stops the worker when its layer scope closes", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;

        yield* provideWorker(Worker.make(exampleQueue, () => Effect.void));

        expect(state.offWorkCalls).toEqual([
          ["example", { id: "worker-id", wait: true }],
        ]);
      }),
    ),
  );

  it.effect(
    "fails the job with WorkerJobPayloadSchemaError when the payload does not decode",
    () =>
      provideFake(
        Effect.gen(function* () {
          const state = yield* PgBossFakeState;
          const seen: Array<TypedJob<typeof TestPayload.Type>> = [];

          yield* provideWorker(
            Worker.make(exampleQueue, (job) => {
              seen.push(job);
              return Effect.void;
            }),
          );

          yield* invokeWork("example", [
            { ...fakeJob, data: { nope: 1 } },
          ]).pipe(Effect.ignore);

          const [error] = state.workErrors;
          expect(isWorkerJobPayloadSchemaError(error)).toBe(true);
          if (isWorkerJobPayloadSchemaError(error)) {
            expect(error.jobId).toBe("job-1");
          }
          expect(seen).toHaveLength(0);
        }),
      ),
  );
});
