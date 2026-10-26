# pgboss-effect

Opinionated way of doing pg-boss queues and workers in Effect applications.

## Requirements

- Node.js >= 22.12
- Effect 4
- pg-boss 12

## Install

```sh
pnpm add pgboss-effect effect pg-boss
```

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

The pg-boss internals used by `Queue` and `Worker` are not exported.

## License

MIT
