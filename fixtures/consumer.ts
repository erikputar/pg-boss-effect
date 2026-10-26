import { Effect, Layer, Schema } from "effect";
import { PgBoss, Queue, Worker } from "pgboss-effect";

const Payload = Schema.Struct({ message: Schema.String });

const queue = Queue.make("consumer-queue", { payload: Payload });

const worker = Worker.make(queue, (job) =>
  Effect.log(`processing ${job.data.message}`),
);

const PgBossLive = PgBoss.layer({
  connectionString: "postgresql://pgboss:pgboss@localhost:5435/pgboss",
});

const QueueLive = queue.toLayer.pipe(Layer.provideMerge(PgBossLive));

const WorkerLive = worker.toLayer.pipe(
  Layer.provide(queue.toLayer),
  Layer.provideMerge(PgBossLive),
);

const send = Effect.scoped(
  queue.send({ message: "hello" }).pipe(Effect.provide(QueueLive)),
);

const workerProgram = Effect.scoped(Layer.launch(WorkerLive));

export { send, workerProgram };
