import { layer } from "@effect/vitest";
import { Deferred, Effect, Layer, Schema } from "effect";
import { expect } from "vitest";

import { PgBoss, QueueNotFoundError } from "./pgBoss";
import { Queue, type TypedJob } from "./queue";
import { Worker } from "./worker";
import { randomUUID } from "node:crypto";

const TestLayer = PgBoss.layer({
  connectionString:
    process.env.PGBOSS_TEST_DATABASE_URL ??
    "postgresql://pgboss:pgboss@localhost:5435/pgboss",
  schedule: false,
  supervise: false,
});

const TestPayload = Schema.Struct({
  when: Schema.DateFromString,
  label: Schema.String,
});

const roundTripQueue = Queue.make("roundtrip", {
  payload: TestPayload,
  queueOptions: { retryLimit: 0 },
});

layer(TestLayer, { excludeTestServices: true })("pgboss-effect", (it) => {
  it.effect("round-trips a transformed payload through send and work", () =>
    Effect.gen(function* () {
      const processed =
        yield* Deferred.make<TypedJob<typeof TestPayload.Type>>();
      const WorkerLayer = Worker.make(
        roundTripQueue,
        { pollingIntervalSeconds: 0.5 },
        (job) => Deferred.succeed(processed, job),
      ).toLayer.pipe(Layer.provide(roundTripQueue.toLayer));

      yield* Effect.scoped(Effect.provide(Effect.void, WorkerLayer));

      const sent = new Date("2024-06-01T12:00:00.000Z");
      const jobId = yield* roundTripQueue.send({
        when: sent,
        label: "hello",
      });
      expect(jobId).not.toBeNull();
      expect(typeof jobId).toBe("string");

      const job = yield* Deferred.await(processed).pipe(
        Effect.timeout("10 seconds"),
        Effect.catchTag("TimeoutError", () => Effect.succeed(null)),
      );
      expect(job).not.toBeNull();
      if (job !== null) {
        expect(job.data.when).toEqual(sent);
        expect(job.data.label).toBe("hello");
        expect(typeof job.id).toBe("string");
      }
    }),
  );

  it.effect("maps a missing queue to QueueNotFoundError", () =>
    Effect.gen(function* () {
      const missingQueue = Queue.make(`missing-${randomUUID()}`, {
        payload: TestPayload,
      });
      const error = yield* Effect.flip(
        missingQueue.send({ when: new Date(), label: "missing" }),
      );

      expect(error).toBeInstanceOf(QueueNotFoundError);
      if (error instanceof QueueNotFoundError) {
        expect(error.queue).toBe(missingQueue.name);
        expect(error.cause.message).toBe(
          `Queue ${missingQueue.name} does not exist`,
        );
      }
    }),
  );

  it.effect("toLayer is idempotent", () =>
    Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.provide(Effect.void, Layer.fresh(roundTripQueue.toLayer)),
      );
      yield* Effect.scoped(
        Effect.provide(Effect.void, Layer.fresh(roundTripQueue.toLayer)),
      );

      const { getQueue } = yield* PgBoss;
      const queue = yield* getQueue("roundtrip");
      expect(queue?.name).toBe("roundtrip");
      expect(queue?.retryLimit).toBe(0);
    }),
  );
});
