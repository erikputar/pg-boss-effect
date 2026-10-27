import { layer } from "@effect/vitest";
import { Deferred, Effect, Layer, Schema } from "effect";
import { expect } from "vitest";

import {
  DeadLetterQueueNotFoundError,
  InvalidScheduleError,
  PgBossDatabaseError,
} from "./errors";
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

const TxPayload = Schema.Struct({ label: Schema.String });

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

      yield* Effect.scoped(
        Effect.gen(function* () {
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
        }).pipe(Effect.provide(WorkerLayer)),
      );
    }),
  );

  it.effect("runs a transactional worker with a usable transaction", () =>
    Effect.gen(function* () {
      const rowsDeferred = yield* Deferred.make<ReadonlyArray<unknown>>();
      const txQueue = Queue.make(`tx-${randomUUID()}`, {
        payload: TxPayload,
        queueOptions: { retryLimit: 0 },
      });
      const WorkerLayer = Worker.make(
        txQueue,
        { transactional: true, pollingIntervalSeconds: 0.5 },
        (job, transaction) =>
          Effect.gen(function* () {
            const rows = yield* transaction.executeSql(
              "SELECT $1::text AS label",
              [job.data.label],
            );
            yield* Deferred.succeed(rowsDeferred, rows);
            return rows;
          }),
      ).toLayer.pipe(Layer.provide(txQueue.toLayer));

      yield* Effect.scoped(
        Effect.gen(function* () {
          const jobId = yield* txQueue.send({ label: "transactional" });
          expect(jobId).not.toBeNull();

          const rows = yield* Deferred.await(rowsDeferred).pipe(
            Effect.timeout("10 seconds"),
            Effect.catchTag("TimeoutError", () => Effect.succeed(null)),
          );
          expect(rows).toEqual([{ label: "transactional" }]);
        }).pipe(Effect.provide(WorkerLayer)),
      );
    }),
  );

  it.effect("registers and manages queue schedules", () =>
    Effect.gen(function* () {
      const scheduledQueue = Queue.make(`scheduled-${randomUUID()}`, {
        payload: TxPayload,
        schedules: {
          daily: {
            cron: "0 0 * * *",
            data: { label: "daily" },
            options: { tz: "UTC" },
          },
        },
      });

      yield* Effect.scoped(Effect.provide(Effect.void, scheduledQueue.toLayer));

      const jobId = yield* scheduledQueue.send({ label: "direct" });
      expect(jobId).not.toBeNull();

      const schedules = yield* scheduledQueue.getSchedules();
      expect(schedules).toHaveLength(1);
      expect(schedules[0]?.key).toBe("daily");
      expect(schedules[0]?.data).toEqual({ label: "daily" });

      const daily = yield* scheduledQueue.getSchedule("daily");
      expect(daily?.cron).toBe("0 0 * * *");

      yield* scheduledQueue.unschedule("daily");
      expect(yield* scheduledQueue.getSchedules()).toHaveLength(0);
    }),
  );

  it.effect("maps invalid schedule expressions to InvalidScheduleError", () =>
    Effect.gen(function* () {
      const invalidQueue = Queue.make(`invalid-schedule-${randomUUID()}`, {
        payload: TxPayload,
        schedules: {
          bad: {
            cron: "not a cron",
            data: { label: "invalid" },
          },
        },
      });

      const error = yield* Effect.flip(
        Effect.scoped(Effect.provide(Effect.void, invalidQueue.toLayer)),
      );

      expect(error).toBeInstanceOf(InvalidScheduleError);
    }),
  );

  it.effect("maps invalid previews to InvalidScheduleError", () =>
    Effect.gen(function* () {
      const { previewSchedule } = yield* PgBoss;
      const error = yield* Effect.flip(previewSchedule(""));

      expect(error).toBeInstanceOf(InvalidScheduleError);
    }),
  );

  it.effect("maps a missing dead letter queue during send", () =>
    Effect.gen(function* () {
      const queue = Queue.make(`dlq-${randomUUID()}`, {
        payload: TxPayload,
        queueOptions: { retryLimit: 0 },
      });
      const missingDeadLetter = `missing-dlq-${randomUUID()}`;

      yield* Effect.scoped(Effect.provide(Effect.void, queue.toLayer));

      const error = yield* Effect.flip(
        queue.send({ label: "dead letter" }, { deadLetter: missingDeadLetter }),
      );

      expect(error).toBeInstanceOf(DeadLetterQueueNotFoundError);
      if (error instanceof DeadLetterQueueNotFoundError) {
        expect(error.queue).toBe(queue.name);
        expect(error.deadLetterQueue).toBe(missingDeadLetter);
      }
    }),
  );

  it.effect("maps a missing dead letter queue during queue creation", () =>
    Effect.gen(function* () {
      const missingDeadLetter = `missing-dlq-${randomUUID()}`;
      const queue = Queue.make(`dlq-create-${randomUUID()}`, {
        payload: TxPayload,
        queueOptions: { deadLetter: missingDeadLetter },
      });

      const error = yield* Effect.flip(
        Effect.scoped(Effect.provide(Effect.void, queue.toLayer)),
      );

      expect(error).toBeInstanceOf(DeadLetterQueueNotFoundError);
      if (error instanceof DeadLetterQueueNotFoundError) {
        expect(error.queue).toBe(queue.name);
        expect(error.deadLetterQueue).toBe(missingDeadLetter);
      }
    }),
  );

  it.effect("maps transaction SQL failures to PgBossDatabaseError", () =>
    Effect.gen(function* () {
      const received = yield* Deferred.make<PgBossDatabaseError>();
      const txQueue = Queue.make(`tx-error-${randomUUID()}`, {
        payload: TxPayload,
        queueOptions: { retryLimit: 0 },
      });
      const missingTable = `missing_${randomUUID().replaceAll("-", "_")}`;
      const WorkerLayer = Worker.make(
        txQueue,
        { transactional: true, pollingIntervalSeconds: 0.5 },
        (_job, transaction) =>
          Effect.gen(function* () {
            const error = yield* Effect.flip(
              transaction.executeSql(`SELECT * FROM ${missingTable}`),
            );
            yield* Deferred.succeed(received, error);
          }),
      ).toLayer.pipe(Layer.provide(txQueue.toLayer));

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* txQueue.send({ label: "database error" });

          const error = yield* Deferred.await(received).pipe(
            Effect.timeout("10 seconds"),
            Effect.catchTag("TimeoutError", () => Effect.succeed(null)),
          );
          expect(error).toBeInstanceOf(PgBossDatabaseError);
          if (error instanceof PgBossDatabaseError) {
            expect(error.code).toBe("42P01");
          }
        }).pipe(Effect.provide(WorkerLayer)),
      );
    }),
  );

  it.effect("previews schedules", () =>
    Effect.gen(function* () {
      const { previewSchedule } = yield* PgBoss;
      const occurrences = yield* previewSchedule("0 0 * * *", { count: 2 });

      expect(occurrences).toHaveLength(2);
      expect(occurrences.every((date) => date instanceof Date)).toBe(true);
    }),
  );

  it.effect("returns registered pg-boss instances", () =>
    Effect.gen(function* () {
      const { getInstances } = yield* PgBoss;
      const instances = yield* getInstances();

      expect(instances.length).toBeGreaterThan(0);
      expect(instances.some((instance) => instance.live)).toBe(true);
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
