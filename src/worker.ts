import { Data, Effect, Layer, Schema } from "effect";
import type { Job as PgBossJob, WorkOptions } from "pg-boss";

import { type PgBossError, PgBossInternal } from "./pgBossInternal";
import type { Queue, TypedJob } from "./queue";

export class WorkerJobPayloadSchemaError extends Data.TaggedError(
  "WorkerJobPayloadSchemaError",
)<{
  queue: string;
  jobId: string;
  cause: Schema.SchemaError;
}> {}

type BaseWorkerOptions = Omit<WorkOptions, "batchSize" | "perJobResults">;

export type SingleWorkerOptions = BaseWorkerOptions & {
  readonly batchSize?: undefined;
};

export type BatchWorkerOptions = BaseWorkerOptions & {
  readonly batchSize: number;
};

type SingleHandler<Payload, A, E, R> = (
  job: TypedJob<Payload>,
) => Effect.Effect<A, E, R>;

type BatchHandler<Payload, A, E, R> = (
  jobs: ReadonlyArray<TypedJob<Payload>>,
) => Effect.Effect<A, E, R>;

type WorkerConfig<Payload, A, E, R> =
  | {
      readonly kind: "single";
      readonly options: SingleWorkerOptions;
      readonly handler: SingleHandler<Payload, A, E, R>;
    }
  | {
      readonly kind: "batch";
      readonly options: BatchWorkerOptions;
      readonly handler: BatchHandler<Payload, A, E, R>;
    };

export interface Worker<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  R,
> {
  readonly queue: Queue<Name, Payload>;
  readonly toLayer: Layer.Layer<never, PgBossError, PgBossInternal | R>;
}

const makeWorker = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  config: WorkerConfig<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R> => {
  const decodeJob = (job: PgBossJob) =>
    Schema.decodeUnknownEffect(queue.payloadSchema)(job.data).pipe(
      Effect.mapError(
        (cause) =>
          new WorkerJobPayloadSchemaError({
            queue: queue.name,
            jobId: job.id,
            cause,
          }),
      ),
      Effect.map((data) => ({ ...job, data })),
    );

  const handleJobs = (jobs: PgBossJob[]) =>
    Effect.gen(function* () {
      const decoded = yield* Effect.forEach(jobs, decodeJob);

      if (config.kind === "batch") {
        return yield* config.handler(decoded);
      }

      const job = decoded[0];
      if (job === undefined) {
        return yield* Effect.die(
          new Error(`pg-boss returned no jobs for queue ${queue.name}`),
        );
      }

      return yield* config.handler(job);
    });

  const workOptions: WorkOptions =
    config.kind === "single"
      ? { ...config.options, batchSize: 1 }
      : config.options;

  const toLayer = Layer.effectDiscard(
    Effect.gen(function* () {
      const { work } = yield* PgBossInternal;
      yield* work(queue.name, handleJobs, workOptions);
    }),
  );

  return { queue, toLayer } as const;
};

type DefaultArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  handler: SingleHandler<Payload["Type"], A, E, R>,
];

type SingleArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: SingleWorkerOptions,
  handler: SingleHandler<Payload["Type"], A, E, R>,
];

type BatchArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: BatchWorkerOptions,
  handler: BatchHandler<Payload["Type"], A, E, R>,
];

type WorkerArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> =
  | DefaultArgs<Name, Payload, A, E, R>
  | SingleArgs<Name, Payload, A, E, R>
  | BatchArgs<Name, Payload, A, E, R>;

const isSingleArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is SingleArgs<Name, Payload, A, E, R> =>
  args.length === 3 && args[1].batchSize === undefined;

const isBatchArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is BatchArgs<Name, Payload, A, E, R> =>
  args.length === 3 && args[1].batchSize !== undefined;

function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  handler: SingleHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: SingleWorkerOptions,
  handler: SingleHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: BatchWorkerOptions,
  handler: BatchHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(...args: WorkerArgs<Name, Payload, A, E, R>): Worker<Name, Payload, R> {
  if (isBatchArgs(args)) {
    const [queue, options, handler] = args;
    return makeWorker(queue, { kind: "batch", options, handler });
  }
  if (isSingleArgs(args)) {
    const [queue, options, handler] = args;
    return makeWorker(queue, { kind: "single", options, handler });
  }
  const [queue, handler] = args;
  return makeWorker(queue, { kind: "single", options: {}, handler });
}

export const Worker = { make };
