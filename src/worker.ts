import { Data, Effect, Layer, Schema } from "effect";
import type {
  Job as PgBossJob,
  JobWithMetadata as PgBossJobWithMetadata,
  WorkOptions,
} from "pg-boss";

import { type PgBossError, PgBossInternal } from "./pgBossInternal";
import type { Queue, TypedJob, TypedJobWithMetadata } from "./queue";
import type { PgBossTransaction } from "./transaction";

export class WorkerJobPayloadSchemaError extends Data.TaggedError(
  "WorkerJobPayloadSchemaError",
)<{
  queue: string;
  jobId: string;
  cause: Schema.SchemaError;
}> {}

type BaseWorkerOptions = Omit<
  WorkOptions,
  | "batchSize"
  | "perJobResults"
  | "includeMetadata"
  | "transactional"
  | "transactionTimeoutSeconds"
>;

type NonTransactionalOptions = {
  readonly transactional?: false;
};

type TransactionalOptions = {
  readonly transactional: true;
  readonly transactionTimeoutSeconds?: number;
};

type NonMetadataOptions = {
  readonly includeMetadata?: false;
};

type MetadataOptions = {
  readonly includeMetadata: true;
};

export type SingleWorkerOptions = BaseWorkerOptions &
  NonTransactionalOptions &
  NonMetadataOptions & {
    readonly batchSize?: undefined;
  };

export type BatchWorkerOptions = BaseWorkerOptions &
  NonTransactionalOptions &
  NonMetadataOptions & {
    readonly batchSize: number;
  };

export type MetadataSingleWorkerOptions = BaseWorkerOptions &
  NonTransactionalOptions &
  MetadataOptions & {
    readonly batchSize?: undefined;
  };

export type MetadataBatchWorkerOptions = BaseWorkerOptions &
  NonTransactionalOptions &
  MetadataOptions & {
    readonly batchSize: number;
  };

export type TransactionalSingleWorkerOptions = BaseWorkerOptions &
  TransactionalOptions &
  NonMetadataOptions & {
    readonly batchSize?: undefined;
  };

export type TransactionalBatchWorkerOptions = BaseWorkerOptions &
  TransactionalOptions &
  NonMetadataOptions & {
    readonly batchSize: number;
  };

export type TransactionalMetadataSingleWorkerOptions = BaseWorkerOptions &
  TransactionalOptions &
  MetadataOptions & {
    readonly batchSize?: undefined;
  };

export type TransactionalMetadataBatchWorkerOptions = BaseWorkerOptions &
  TransactionalOptions &
  MetadataOptions & {
    readonly batchSize: number;
  };

export type WorkerOptions =
  | SingleWorkerOptions
  | BatchWorkerOptions
  | MetadataSingleWorkerOptions
  | MetadataBatchWorkerOptions
  | TransactionalSingleWorkerOptions
  | TransactionalBatchWorkerOptions
  | TransactionalMetadataSingleWorkerOptions
  | TransactionalMetadataBatchWorkerOptions;

export type SingleWorkerHandler<Payload, A, E, R> = (
  job: TypedJob<Payload>,
) => Effect.Effect<A, E, R>;

export type BatchWorkerHandler<Payload, A, E, R> = (
  jobs: ReadonlyArray<TypedJob<Payload>>,
) => Effect.Effect<A, E, R>;

export type MetadataSingleWorkerHandler<Payload, A, E, R> = (
  job: TypedJobWithMetadata<Payload>,
) => Effect.Effect<A, E, R>;

export type MetadataBatchWorkerHandler<Payload, A, E, R> = (
  jobs: ReadonlyArray<TypedJobWithMetadata<Payload>>,
) => Effect.Effect<A, E, R>;

export type TransactionalSingleWorkerHandler<Payload, A, E, R> = (
  job: TypedJob<Payload>,
  transaction: PgBossTransaction,
) => Effect.Effect<A, E, R>;

export type TransactionalBatchWorkerHandler<Payload, A, E, R> = (
  jobs: ReadonlyArray<TypedJob<Payload>>,
  transaction: PgBossTransaction,
) => Effect.Effect<A, E, R>;

export type TransactionalMetadataSingleWorkerHandler<Payload, A, E, R> = (
  job: TypedJobWithMetadata<Payload>,
  transaction: PgBossTransaction,
) => Effect.Effect<A, E, R>;

export type TransactionalMetadataBatchWorkerHandler<Payload, A, E, R> = (
  jobs: ReadonlyArray<TypedJobWithMetadata<Payload>>,
  transaction: PgBossTransaction,
) => Effect.Effect<A, E, R>;

export interface Worker<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  R,
> {
  readonly queue: Queue<Name, Payload>;
  readonly toLayer: Layer.Layer<never, PgBossError, PgBossInternal | R>;
}

type PgBossJobLike = Omit<PgBossJob, "data"> & { data: unknown };

type WorkerKind = "single" | "batch";

interface WorkerConfig<J, A, E, R> {
  readonly kind: WorkerKind;
  readonly options: WorkOptions;
  readonly decode: (
    job: PgBossJob,
  ) => Effect.Effect<J, WorkerJobPayloadSchemaError>;
  readonly handler: (
    jobs: ReadonlyArray<J>,
    transaction: PgBossTransaction | undefined,
  ) => Effect.Effect<A, E, R>;
}

const decodeJob = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  J extends PgBossJobLike,
>(
  queue: Queue<Name, Payload>,
  job: J,
): Effect.Effect<
  Omit<J, "data"> & { data: Payload["Type"] },
  WorkerJobPayloadSchemaError
> =>
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

const isJobWithMetadata = (job: PgBossJob): job is PgBossJobWithMetadata =>
  "state" in job && "priority" in job && "retryLimit" in job;

const decodeMetadataJob = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
>(
  queue: Queue<Name, Payload>,
  job: PgBossJob,
): Effect.Effect<
  TypedJobWithMetadata<Payload["Type"]>,
  WorkerJobPayloadSchemaError
> => {
  if (!isJobWithMetadata(job)) {
    return Effect.die(
      new Error(`pg-boss did not include metadata for job ${job.id}`),
    );
  }
  return decodeJob(queue, job);
};

const singleJobHandler = <J, A, E, R>(
  queueName: string,
  handler: (job: J) => Effect.Effect<A, E, R>,
) => {
  return (jobs: ReadonlyArray<J>): Effect.Effect<A, E, R> => {
    const job = jobs[0];
    if (!job) {
      return Effect.die(
        new Error(`pg-boss returned no jobs for queue ${queueName}`),
      );
    }
    return handler(job);
  };
};

const singleTransactionalJobHandler = <J, A, E, R>(
  queueName: string,
  handler: (job: J, transaction: PgBossTransaction) => Effect.Effect<A, E, R>,
) => {
  return (
    jobs: ReadonlyArray<J>,
    transaction: PgBossTransaction,
  ): Effect.Effect<A, E, R> => {
    const job = jobs[0];
    if (!job) {
      return Effect.die(
        new Error(`pg-boss returned no jobs for queue ${queueName}`),
      );
    }
    return handler(job, transaction);
  };
};

const makeWorker = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  J,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  config: WorkerConfig<J, A, E, R>,
): Worker<Name, Payload, R> => {
  const handleJobs = (
    jobs: PgBossJob[],
    transaction: PgBossTransaction | undefined,
  ) =>
    Effect.gen(function* () {
      const decoded = yield* Effect.forEach(jobs, config.decode);
      return yield* config.handler(decoded, transaction);
    });

  const workOptions: WorkOptions =
    config.kind === "single"
      ? { ...config.options, batchSize: 1 }
      : config.options;

  const toLayer = Layer.effectDiscard(
    Effect.acquireRelease(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        return yield* work(queue.name, handleJobs, workOptions);
      }),
      (workerId) =>
        Effect.gen(function* () {
          const { offWork } = yield* PgBossInternal;
          yield* offWork(queue.name, { id: workerId, wait: true });
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logError("failed to stop worker", cause),
          ),
        ),
    ),
  );

  return { queue, toLayer } as const;
};

const makeNonTransactionalWorker = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  J,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  kind: WorkerKind,
  options: WorkOptions,
  decode: (job: PgBossJob) => Effect.Effect<J, WorkerJobPayloadSchemaError>,
  handler: (jobs: ReadonlyArray<J>) => Effect.Effect<A, E, R>,
): Worker<Name, Payload, R> =>
  makeWorker(queue, {
    kind,
    options,
    decode,
    handler: (jobs) => handler(jobs),
  });

const makeTransactionalWorker = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  J,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  kind: WorkerKind,
  options: WorkOptions,
  decode: (job: PgBossJob) => Effect.Effect<J, WorkerJobPayloadSchemaError>,
  handler: (
    jobs: ReadonlyArray<J>,
    transaction: PgBossTransaction,
  ) => Effect.Effect<A, E, R>,
): Worker<Name, Payload, R> =>
  makeWorker(queue, {
    kind,
    options,
    decode,
    handler: (jobs, transaction) => {
      if (!transaction) {
        return Effect.die(new Error("pg-boss did not provide a transaction"));
      }
      return handler(jobs, transaction);
    },
  });

type DefaultArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  handler: SingleWorkerHandler<Payload["Type"], A, E, R>,
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
  handler: SingleWorkerHandler<Payload["Type"], A, E, R>,
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
  handler: BatchWorkerHandler<Payload["Type"], A, E, R>,
];

type MetadataSingleArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: MetadataSingleWorkerOptions,
  handler: MetadataSingleWorkerHandler<Payload["Type"], A, E, R>,
];

type MetadataBatchArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: MetadataBatchWorkerOptions,
  handler: MetadataBatchWorkerHandler<Payload["Type"], A, E, R>,
];

type TransactionalSingleArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: TransactionalSingleWorkerOptions,
  handler: TransactionalSingleWorkerHandler<Payload["Type"], A, E, R>,
];

type TransactionalBatchArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: TransactionalBatchWorkerOptions,
  handler: TransactionalBatchWorkerHandler<Payload["Type"], A, E, R>,
];

type TransactionalMetadataSingleArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: TransactionalMetadataSingleWorkerOptions,
  handler: TransactionalMetadataSingleWorkerHandler<Payload["Type"], A, E, R>,
];

type TransactionalMetadataBatchArgs<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
> = [
  queue: Queue<Name, Payload>,
  options: TransactionalMetadataBatchWorkerOptions,
  handler: TransactionalMetadataBatchWorkerHandler<Payload["Type"], A, E, R>,
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
  | BatchArgs<Name, Payload, A, E, R>
  | MetadataSingleArgs<Name, Payload, A, E, R>
  | MetadataBatchArgs<Name, Payload, A, E, R>
  | TransactionalSingleArgs<Name, Payload, A, E, R>
  | TransactionalBatchArgs<Name, Payload, A, E, R>
  | TransactionalMetadataSingleArgs<Name, Payload, A, E, R>
  | TransactionalMetadataBatchArgs<Name, Payload, A, E, R>;

const isDefaultArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is DefaultArgs<Name, Payload, A, E, R> => args.length === 2;

const isSingleArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is SingleArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize === undefined &&
  args[1].transactional !== true &&
  args[1].includeMetadata !== true;

const isBatchArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is BatchArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize !== undefined &&
  args[1].transactional !== true &&
  args[1].includeMetadata !== true;

const isMetadataSingleArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is MetadataSingleArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize === undefined &&
  args[1].transactional !== true &&
  args[1].includeMetadata === true;

const isMetadataBatchArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is MetadataBatchArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize !== undefined &&
  args[1].transactional !== true &&
  args[1].includeMetadata === true;

const isTransactionalSingleArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is TransactionalSingleArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize === undefined &&
  args[1].transactional === true &&
  args[1].includeMetadata !== true;

const isTransactionalBatchArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is TransactionalBatchArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize !== undefined &&
  args[1].transactional === true &&
  args[1].includeMetadata !== true;

const isTransactionalMetadataSingleArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is TransactionalMetadataSingleArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize === undefined &&
  args[1].transactional === true &&
  args[1].includeMetadata === true;

const isTransactionalMetadataBatchArgs = <
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  args: WorkerArgs<Name, Payload, A, E, R>,
): args is TransactionalMetadataBatchArgs<Name, Payload, A, E, R> =>
  args.length === 3 &&
  args[1].batchSize !== undefined &&
  args[1].transactional === true &&
  args[1].includeMetadata === true;

function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  handler: SingleWorkerHandler<Payload["Type"], A, E, R>,
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
  handler: SingleWorkerHandler<Payload["Type"], A, E, R>,
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
  handler: BatchWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: MetadataSingleWorkerOptions,
  handler: MetadataSingleWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: MetadataBatchWorkerOptions,
  handler: MetadataBatchWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: TransactionalSingleWorkerOptions,
  handler: TransactionalSingleWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: TransactionalBatchWorkerOptions,
  handler: TransactionalBatchWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: TransactionalMetadataSingleWorkerOptions,
  handler: TransactionalMetadataSingleWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(
  queue: Queue<Name, Payload>,
  options: TransactionalMetadataBatchWorkerOptions,
  handler: TransactionalMetadataBatchWorkerHandler<Payload["Type"], A, E, R>,
): Worker<Name, Payload, R>;
function make<
  Name extends string,
  Payload extends Schema.Codec<unknown, object | null>,
  A,
  E,
  R,
>(...args: WorkerArgs<Name, Payload, A, E, R>): Worker<Name, Payload, R> {
  if (isTransactionalMetadataBatchArgs(args)) {
    const [queue, options, handler] = args;
    return makeTransactionalWorker(
      queue,
      "batch",
      options,
      (job) => decodeMetadataJob(queue, job),
      handler,
    );
  }

  if (isTransactionalMetadataSingleArgs(args)) {
    const [queue, options, handler] = args;
    return makeTransactionalWorker(
      queue,
      "single",
      options,
      (job) => decodeMetadataJob(queue, job),
      singleTransactionalJobHandler(queue.name, handler),
    );
  }

  if (isTransactionalBatchArgs(args)) {
    const [queue, options, handler] = args;
    return makeTransactionalWorker(
      queue,
      "batch",
      options,
      (job) => decodeJob(queue, job),
      handler,
    );
  }

  if (isTransactionalSingleArgs(args)) {
    const [queue, options, handler] = args;
    return makeTransactionalWorker(
      queue,
      "single",
      options,
      (job) => decodeJob(queue, job),
      singleTransactionalJobHandler(queue.name, handler),
    );
  }

  if (isMetadataBatchArgs(args)) {
    const [queue, options, handler] = args;
    return makeNonTransactionalWorker(
      queue,
      "batch",
      options,
      (job) => decodeMetadataJob(queue, job),
      handler,
    );
  }

  if (isMetadataSingleArgs(args)) {
    const [queue, options, handler] = args;
    return makeNonTransactionalWorker(
      queue,
      "single",
      options,
      (job) => decodeMetadataJob(queue, job),
      singleJobHandler(queue.name, handler),
    );
  }

  if (isBatchArgs(args)) {
    const [queue, options, handler] = args;
    return makeNonTransactionalWorker(
      queue,
      "batch",
      options,
      (job) => decodeJob(queue, job),
      handler,
    );
  }

  if (isSingleArgs(args)) {
    const [queue, options, handler] = args;
    return makeNonTransactionalWorker(
      queue,
      "single",
      options,
      (job) => decodeJob(queue, job),
      singleJobHandler(queue.name, handler),
    );
  }

  if (isDefaultArgs(args)) {
    const [queue, handler] = args;
    return makeNonTransactionalWorker(
      queue,
      "single",
      {},
      (job) => decodeJob(queue, job),
      singleJobHandler(queue.name, handler),
    );
  }

  throw new Error("invalid Worker.make arguments");
}

export const Worker = { make };
