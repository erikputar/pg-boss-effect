import { Effect, Layer, Schema } from "effect";
import type {
  Db,
  Job,
  JobWithMetadata,
  PgBossRuntimeError,
  PgBossTransaction,
  QueueResult,
  QueueSchedule,
  QueueScheduleError,
  SendOptions,
  WorkOptions,
} from "pgboss-effect";
import { PgBoss, Queue, Worker } from "pgboss-effect";

const Payload = Schema.Struct({ message: Schema.String });

const queue = Queue.make("consumer-queue", { payload: Payload });

const scheduledQueue = Queue.make("scheduled-consumer-queue", {
  payload: Payload,
  schedules: {
    daily: {
      cron: "0 0 * * *",
      data: { message: "scheduled" },
      options: { tz: "UTC" },
    },
  },
});

const worker = Worker.make(queue, (job) =>
  Effect.log(`processing ${job.data.message}`),
);

const metadataWorker = Worker.make(queue, { includeMetadata: true }, (job) =>
  Effect.sync(() => job.state),
);

const batchMetadataWorker = Worker.make(
  queue,
  { batchSize: 2, includeMetadata: true },
  (jobs) => Effect.sync(() => jobs.map((job) => job.priority)),
);

const transactionalWorker = Worker.make(
  queue,
  { transactional: true },
  (job, transaction) =>
    Effect.gen(function* () {
      const rows = yield* transaction.executeSql("SELECT $1", [
        job.data.message,
      ]);
      return rows.length;
    }),
);

const transactionalMetadataWorker = Worker.make(
  queue,
  { transactional: true, includeMetadata: true },
  (job, transaction) =>
    Effect.gen(function* () {
      yield* transaction.executeSql("SELECT $1", [job.state]);
    }),
);

const batchTransactionalWorker = Worker.make(
  queue,
  { batchSize: 2, transactional: true },
  (jobs, transaction) =>
    Effect.gen(function* () {
      yield* transaction.executeSql("SELECT $1", [jobs.length]);
    }),
);

const invalidScheduledQueue = Queue.make("invalid-scheduled-queue", {
  payload: Payload,
  schedules: {
    daily: {
      cron: "0 0 * * *",
      // @ts-expect-error schedule data must match the queue payload
      data: { wrong: true },
    },
  },
});

const invalidMetadataWorker = Worker.make(queue, (job) =>
  // @ts-expect-error metadata fields require includeMetadata: true
  Effect.sync(() => job.state),
);

// @ts-expect-error transactionTimeoutSeconds requires transactional: true
const invalidTransactionTimeout: SingleWorkerOptions = {
  transactionTimeoutSeconds: 60,
};

// @ts-expect-error perJobResults is not supported
const invalidPerJobResults: BatchWorkerOptions = {
  batchSize: 2,
  perJobResults: true,
};

const typedJob: Job | undefined = undefined;
const typedJobWithMetadata: JobWithMetadata | undefined = undefined;
const typedQueueResult: QueueResult | undefined = undefined;
const typedSendOptions: SendOptions | undefined = undefined;
const typedWorkOptions: WorkOptions | undefined = undefined;
const typedDb: Db | undefined = undefined;
const typedTransaction: PgBossTransaction | undefined = undefined;
const typedSchedule: QueueSchedule<{ message: string }> | undefined = undefined;

const PgBossLive = PgBoss.layer({
  connectionString: "postgresql://pgboss:pgboss@localhost:5435/pgboss",
});

const PgBossFromStringLive = PgBoss.layer(
  "postgresql://pgboss:pgboss@localhost:5435/pgboss",
);

const QueueLive = queue.toLayer.pipe(Layer.provideMerge(PgBossLive));

const WorkerLive = worker.toLayer.pipe(
  Layer.provide(queue.toLayer),
  Layer.provideMerge(PgBossLive),
);

const TransactionalWorkerLive: Layer.Layer<
  never,
  QueueScheduleError | PgBossRuntimeError,
  never
> = transactionalWorker.toLayer.pipe(
  Layer.provide(queue.toLayer),
  Layer.provideMerge(PgBossLive),
);

const send = queue.send({ message: "hello" }).pipe(Effect.provide(QueueLive));

const scheduleProgram = Effect.all([
  scheduledQueue.getSchedules(),
  scheduledQueue.getSchedule("daily"),
  scheduledQueue.unschedule("daily"),
]);

const previewSchedules = Effect.gen(function* () {
  const pgBoss = yield* PgBoss;
  return yield* pgBoss.previewSchedule("0 0 * * *", { count: 2 });
});

const listInstances = Effect.gen(function* () {
  const pgBoss = yield* PgBoss;
  return yield* pgBoss.getInstances();
});

const workerProgram = Layer.launch(WorkerLive);
const transactionalWorkerProgram = Layer.launch(TransactionalWorkerLive);

export {
  PgBossFromStringLive,
  batchMetadataWorker,
  batchTransactionalWorker,
  invalidMetadataWorker,
  invalidPerJobResults,
  invalidScheduledQueue,
  invalidTransactionTimeout,
  listInstances,
  metadataWorker,
  previewSchedules,
  scheduleProgram,
  scheduledQueue,
  send,
  transactionalMetadataWorker,
  transactionalWorker,
  transactionalWorkerProgram,
  typedDb,
  typedJob,
  typedJobWithMetadata,
  typedQueueResult,
  typedSchedule,
  typedSendOptions,
  typedTransaction,
  typedWorkOptions,
  workerProgram,
};
