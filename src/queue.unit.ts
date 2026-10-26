import { describe, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import { expect } from "vitest";

import { PgBossInternal } from "./pgBossInternal";
import { Queue, QueuePayloadSchemaError } from "./queue";

type PgBossFakeStateShape = {
  readonly createQueueCalls: Array<[string, unknown?]>;
  readonly sendCalls: Array<[string, unknown?, unknown?]>;
};

class PgBossFakeState extends Context.Service<
  PgBossFakeState,
  PgBossFakeStateShape
>()("PgBossFakeState") {}

const PgBossState = Layer.sync(PgBossFakeState, () => ({
  createQueueCalls: [],
  sendCalls: [],
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

const provideFake = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(PgBossFake));

describe("Queue.make", () => {
  it("carries the name, payload schema, and queue options", () => {
    expect(exampleQueue.name).toBe("example");
    expect(exampleQueue.payloadSchema).toBe(TestPayload);
    expect(exampleQueue.queueOptions).toEqual({ retryLimit: 0 });
    expect(exampleQueue.toLayer).toBeDefined();
    expect(exampleQueue.send).toBeTypeOf("function");
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
});
