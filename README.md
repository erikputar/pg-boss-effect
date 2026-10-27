# pgboss-effect

Simple and type-safe pg-boss queues and workers in Effect.

## Requirements

- Node.js >= 22.12
- Effect 4
- pg-boss >= 12.32

## Install

```sh
pnpm add pgboss-effect effect pg-boss
```

This package is ESM-only.

## Define a queue

```ts
import { Schema } from "effect";
import { Queue } from "pgboss-effect";

export const EmailPayload = Schema.Struct({
  to: Schema.String,
  subject: Schema.String,
});

export const EmailsQueue = Queue.make("emails", {
  payload: EmailPayload,
  queueOptions: { retryLimit: 3 },
});
```

The payload schema encodes jobs when sending and decodes them in workers.

## Send a job

```ts
import { Effect, Layer } from "effect";
import { PgBoss } from "pgboss-effect";

import { EmailsQueue } from "./emails";

const PgBossLive = PgBoss.layer({
  connectionString: "postgresql://postgres:postgres@localhost:5432/app",
});

const QueueLive = EmailsQueue.toLayer.pipe(Layer.provideMerge(PgBossLive));

const program = Effect.gen(function* () {
  const jobId = yield* EmailsQueue.send({
    to: "erik@example.com",
    subject: "Hello",
  });

  yield* Effect.log(`queued ${jobId}`);
}).pipe(Effect.provide(QueueLive));

Effect.runFork(program);
```

`EmailsQueue.toLayer` creates the queue. `PgBoss.layer` owns the pg-boss
instance and its lifetime.

## Run a worker

```ts
import { Effect, Layer } from "effect";
import { PgBoss, Worker } from "pgboss-effect";

import { EmailsQueue } from "./emails";

const EmailWorker = Worker.make(
  EmailsQueue,
  { pollingIntervalSeconds: 1 },
  (job) =>
    Effect.gen(function* () {
      yield* Effect.log(`sending ${job.data.subject} to ${job.data.to}`);
    }),
);

const PgBossLive = PgBoss.layer({
  connectionString: "postgresql://postgres:postgres@localhost:5432/app",
});

const WorkerLive = EmailWorker.toLayer.pipe(
  Layer.provide(EmailsQueue.toLayer),
  Layer.provideMerge(PgBossLive),
);

Effect.runFork(Layer.launch(WorkerLive));
```

## Job metadata

Pass `includeMetadata: true` to receive pg-boss job metadata in the handler:

```ts
const EmailWorker = Worker.make(EmailsQueue, { includeMetadata: true }, (job) =>
  Effect.log(`${job.state} ${job.priority}`),
);
```

Handlers get `TypedJobWithMetadata`, which adds fields such as `state`,
`priority`, `retryLimit`, and `startAfter` to `TypedJob`.

## Transactions

`transactional: true` runs the handler and the job's completion in one
pg-boss transaction. The handler receives `PgBossTransaction` as its second
argument:

```ts
import { Worker } from "pgboss-effect";

const ChargeWorker = Worker.make(
  ChargesQueue,
  { transactional: true },
  (job, tx) =>
    Effect.gen(function* () {
      yield* tx.executeSql(
        "INSERT INTO ledger (customer, amount) VALUES ($1, $2)",
        [job.data.customer, job.data.amount],
      );

      yield* ReceiptsQueue.send({ customer: job.data.customer }, { db: tx.db });
    }),
);
```

Batch handlers receive it as `(jobs, tx)`.

`tx.executeSql` returns rows and maps failures to `PgBossError`. `tx.db` is the
raw pg-boss `Db`, so it can be passed as the `db` option to `queue.send` (and to
pg-boss APIs you call directly).

The reverse direction also works: pg-boss ORM adapters such as `fromDrizzle`,
`fromKysely`, `fromKnex`, and `fromPrisma` can be passed to `queue.send` to
enqueue a job inside your application's own transaction:

```ts
yield* EmailsQueue.send(data, { db: fromDrizzle(tx, sql) });
```

A transactional worker needs a database connection pg-boss can open a
transaction on (the built-in pool supports this). It cannot be combined with
`perJobResults`, and some backends do not support it together with heartbeats.
Each in-flight handler holds a pool connection, so size the pool accordingly.
Use `transactionTimeoutSeconds` to bound the database-side transaction.

## Events and errors

`PgBoss` exposes queue reads and event streams:

```ts
import { Effect, Stream } from "effect";
import { PgBoss } from "pgboss-effect";

const logErrors = Effect.gen(function* () {
  const pgBoss = yield* PgBoss;

  yield* pgBoss.errors.pipe(
    Stream.runForEach((error) => Effect.logError(error)),
    Effect.forkScoped,
  );
});
```

`forkScoped` ties the stream to the current `Scope`, so run this inside a scoped
layer or `Effect.scoped`.

- `PgBoss.errors` and `PgBoss.warnings` are stream shortcuts.
- `PgBoss.events` has streams for `error`, `warning`, `wip`, `stopped`, `bam`,
  and `flow`.
- `getQueues` and `getQueue` read queue state.

## API

- `PgBoss.layer(options)` — pg-boss instance layer
- `Queue.make(name, { payload, queueOptions })`
- `queue.send(data, options?)`
- `queue.toLayer` — creates the queue
- `Worker.make(queue, options?, handler)`
- `worker.toLayer` — registers the worker
- `PgBossTransaction` — transaction handle passed to transactional workers

pg-boss public types such as `Job`, `JobWithMetadata`, `SendOptions`,
`WorkOptions`, `QueueResult`, and `Db` are re-exported unchanged. The pg-boss
`Queue` and `PgBoss` types are not re-exported because they collide with this
package's exports; import them directly from `pg-boss`.

The pg-boss internals used by `Queue` and `Worker` are not exported.

## Releases

See [docs/release.md](https://github.com/erikputar/pg-boss-effect/blob/main/docs/release.md)
for the release process.

## License

MIT
