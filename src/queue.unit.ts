import { describe, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import type { Db, Schedule } from "pg-boss";
import { expect } from "vitest";

import { PgBossInternal } from "./pgBossInternal";
import { Queue, QueuePayloadSchemaError } from "./queue";

type PgBossFakeStateShape = {
  readonly createQueueCalls: Array<[string, unknown?]>;
  readonly sendCalls: Array<[string, unknown?, unknown?]>;
  readonly scheduleCalls: Array<[string, string, unknown?, unknown?]>;
  readonly unscheduleCalls: Array<[string, string | undefined]>;
  readonly getSchedulesCalls: Array<[string | undefined, string | undefined]>;
  readonly getScheduleCalls: Array<[string, string | undefined]>;
  readonly getSchedulesResult: Schedule[];
  readonly getScheduleResult: Schedule | null;
};

class PgBossFakeState extends Context.Service<
  PgBossFakeState,
  PgBossFakeStateShape
>()("PgBossFakeState") {}

const PgBossState = Layer.sync(PgBossFakeState, () => ({
  createQueueCalls: [],
  sendCalls: [],
  scheduleCalls: [],
  unscheduleCalls: [],
  getSchedulesCalls: [],
  getScheduleCalls: [],
  getSchedulesResult: [],
  getScheduleResult: null,
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
      send: (name: string, data?: unknown, options?: unknown) =>
        Effect.sync(() => {
          state.sendCalls.push([name, data, options]);
          return "job-id";
        }),
      schedule: (
        name: string,
        cron: string,
        data?: unknown,
        options?: unknown,
      ) =>
        Effect.sync(() => {
          state.scheduleCalls.push([name, cron, data, options]);
        }),
      unschedule: (name: string, key?: string) =>
        Effect.sync(() => {
          state.unscheduleCalls.push([name, key]);
        }),
      getSchedules: (name?: string, key?: string) =>
        Effect.sync(() => {
          state.getSchedulesCalls.push([name, key]);
          return state.getSchedulesResult;
        }),
      getSchedule: (name: string, key?: string) =>
        Effect.sync(() => {
          state.getScheduleCalls.push([name, key]);
          return state.getScheduleResult;
        }),
      work: () => Effect.succeed("worker-id"),
    });
  }),
).pipe(Layer.provideMerge(PgBossState));

const TestPayload = Schema.Struct({
  hello: Schema.String,
  shouldFail: Schema.optionalKey(Schema.Boolean),
});

const DatePayload = Schema.Struct({ when: Schema.DateFromString });

const CodePayload = Schema.Struct({
  code: Schema.String.pipe(Schema.check(Schema.isPattern(/^[A-Z]{2,}$/))),
});

const exampleQueue = Queue.make("example", {
  payload: TestPayload,
  queueOptions: { retryLimit: 0 },
});

const timedQueue = Queue.make("timed", { payload: DatePayload });

const validatedQueue = Queue.make("validated", { payload: CodePayload });

const scheduledQueue = Queue.make("scheduled", {
  payload: DatePayload,
  schedules: {
    daily: {
      cron: "0 0 * * *",
      data: { when: new Date("2024-01-01T00:00:00.000Z") },
      options: { tz: "UTC" },
    },
  },
});

const invalidScheduleQueue = Queue.make("invalid-schedule", {
  payload: CodePayload,
  schedules: {
    bad: {
      cron: "0 0 * * *",
      data: { code: "nope!" },
    },
  },
});

const provideFake = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(PgBossFake));

describe("Queue.make", () => {
  it("carries the name, payload schema, and queue options", () => {
    expect(exampleQueue.name).toBe("example");
    expect(exampleQueue.payloadSchema).toBe(TestPayload);
    expect(exampleQueue.queueOptions).toEqual({ retryLimit: 0 });
    expect(exampleQueue.schedules).toBeUndefined();
    expect(exampleQueue.toLayer).toBeDefined();
    expect(exampleQueue.send).toBeTypeOf("function");
  });

  it("carries schedule definitions", () => {
    expect(scheduledQueue.schedules).toEqual({
      daily: {
        cron: "0 0 * * *",
        data: { when: new Date("2024-01-01T00:00:00.000Z") },
        options: { tz: "UTC" },
      },
    });
  });
});

describe("send", () => {
  it.effect("sends the encoded payload to pg-boss", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const jobId = yield* exampleQueue.send({ hello: "world" });

        expect(jobId).toBe("job-id");
        expect(state.sendCalls).toEqual([
          ["example", { hello: "world" }, undefined],
        ]);
      }),
    ),
  );

  it.effect("encodes schema transformations before sending", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        yield* timedQueue.send({ when: new Date("2024-01-01T00:00:00.000Z") });

        expect(state.sendCalls).toEqual([
          ["timed", { when: "2024-01-01T00:00:00.000Z" }, undefined],
        ]);
      }),
    ),
  );

  it.effect("passes send options through", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        yield* exampleQueue.send(
          { hello: "world" },
          { retryLimit: 3, retryDelay: 5 },
        );

        expect(state.sendCalls).toEqual([
          ["example", { hello: "world" }, { retryLimit: 3, retryDelay: 5 }],
        ]);
      }),
    ),
  );

  it.effect("forwards a transaction through send options", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        const db: Db = {
          executeSql: () => Promise.resolve({ rows: [] }),
        };

        yield* exampleQueue.send({ hello: "world" }, { db });

        expect(state.sendCalls).toEqual([
          ["example", { hello: "world" }, { db }],
        ]);
      }),
    ),
  );

  it.effect(
    "fails with QueuePayloadSchemaError when the payload is invalid",
    () =>
      provideFake(
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            validatedQueue.send({ code: "nope!" }),
          );

          expect(error).toBeInstanceOf(QueuePayloadSchemaError);
        }),
      ),
  );
});

describe("toLayer", () => {
  it.effect("creates the queue with its options", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        yield* Effect.scoped(Effect.provide(Effect.void, exampleQueue.toLayer));

        expect(state.createQueueCalls).toEqual([
          ["example", { retryLimit: 0 }],
        ]);
      }),
    ),
  );

  it.effect("registers schedules after creating the queue", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;
        yield* Effect.scoped(
          Effect.provide(Effect.void, scheduledQueue.toLayer),
        );

        expect(state.createQueueCalls).toEqual([["scheduled", undefined]]);
        expect(state.scheduleCalls).toEqual([
          [
            "scheduled",
            "0 0 * * *",
            { when: "2024-01-01T00:00:00.000Z" },
            { tz: "UTC", key: "daily" },
          ],
        ]);
      }),
    ),
  );

  it.effect(
    "fails with QueuePayloadSchemaError for invalid schedule data",
    () =>
      provideFake(
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            Effect.scoped(
              Effect.provide(Effect.void, invalidScheduleQueue.toLayer),
            ),
          );

          expect(error).toBeInstanceOf(QueuePayloadSchemaError);
        }),
      ),
  );
});

describe("schedules", () => {
  it.effect("reads and unschedules this queue's schedules", () =>
    provideFake(
      Effect.gen(function* () {
        const state = yield* PgBossFakeState;

        const schedules = yield* exampleQueue.getSchedules();
        const schedule = yield* exampleQueue.getSchedule("daily");
        yield* exampleQueue.unschedule("daily");

        expect(schedules).toEqual([]);
        expect(schedule).toBeNull();
        expect(state.getSchedulesCalls).toEqual([["example", undefined]]);
        expect(state.getScheduleCalls).toEqual([["example", "daily"]]);
        expect(state.unscheduleCalls).toEqual([["example", "daily"]]);
      }),
    ),
  );
});
