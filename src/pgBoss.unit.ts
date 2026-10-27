import { Deferred, Effect, Stream } from "effect";
import type { Db, Job as PgBossJob, QueueResult, WorkOptions } from "pg-boss";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  PgBoss,
  PgBossError,
  type PgBossOptions,
  QueueNotFoundError,
} from "./pgBoss";
import { PgBossInternal } from "./pgBossInternal";

type WorkHandler = (jobs: PgBossJob[], db?: Db) => Promise<unknown>;
type Listener = (...args: unknown[]) => void;

const { fakePgBoss, PgBossMock } = vi.hoisted(() => {
  class PgBossMock {
    constructor(options: PgBossOptions) {
      fakePgBoss.constructorOptions = options;
    }

    on(event: string, listener: Listener) {
      fakePgBoss.onCalls.push([event, listener]);
      const listeners = fakePgBoss.listeners.get(event) ?? new Set<Listener>();
      listeners.add(listener);
      fakePgBoss.listeners.set(event, listeners);
      return this;
    }

    off(event: string, listener: Listener) {
      fakePgBoss.offCalls.push([event, listener]);
      fakePgBoss.listeners.get(event)?.delete(listener);
      return this;
    }

    start() {
      fakePgBoss.startCalls += 1;
      return Promise.resolve(this);
    }

    stop() {
      fakePgBoss.stopCalls += 1;
      return Promise.resolve();
    }

    createQueue(name: string, options?: unknown) {
      fakePgBoss.createQueueCalls.push([name, options]);
      if (fakePgBoss.createQueueError !== undefined) {
        return Promise.reject(fakePgBoss.createQueueError);
      }
      return Promise.resolve();
    }

    send(name: string, data?: unknown, options?: unknown) {
      fakePgBoss.sendCalls.push([name, data, options]);
      if (fakePgBoss.sendError !== undefined) {
        return Promise.reject(fakePgBoss.sendError);
      }
      return Promise.resolve("job-id");
    }

    work(name: string, handler: WorkHandler): Promise<string>;
    work(
      name: string,
      options: WorkOptions,
      handler: WorkHandler,
    ): Promise<string>;
    work(
      name: string,
      handlerOrOptions: WorkHandler | WorkOptions,
      maybeHandler?: WorkHandler,
    ) {
      const handler =
        typeof handlerOrOptions === "function"
          ? handlerOrOptions
          : maybeHandler;
      const options =
        typeof handlerOrOptions === "function" ? undefined : handlerOrOptions;
      if (!handler) {
        return Promise.reject(new Error("missing work handler"));
      }
      fakePgBoss.workCalls.push([name, options]);
      fakePgBoss.workHandler = handler;
      return Promise.resolve("worker-id");
    }

    getQueues(names?: string[]) {
      fakePgBoss.getQueuesCalls.push([names]);
      if (fakePgBoss.getQueuesError !== undefined) {
        return Promise.reject(fakePgBoss.getQueuesError);
      }
      return Promise.resolve(fakePgBoss.getQueuesResult);
    }

    getQueue(name: string) {
      fakePgBoss.getQueueCalls.push([name]);
      if (fakePgBoss.getQueueError !== undefined) {
        return Promise.reject(fakePgBoss.getQueueError);
      }
      return Promise.resolve(fakePgBoss.getQueueResult);
    }
  }

  const fakePgBoss = {
    listeners: new Map<string, Set<Listener>>(),
    constructorOptions: undefined as PgBossOptions | undefined,
    onCalls: [] as Array<[string, Listener]>,
    offCalls: [] as Array<[string, Listener]>,
    startCalls: 0,
    stopCalls: 0,
    createQueueCalls: [] as Array<[string, unknown?]>,
    createQueueError: undefined as unknown,
    sendCalls: [] as Array<[string, unknown?, unknown?]>,
    sendError: undefined as unknown,
    workCalls: [] as Array<[string, WorkOptions | undefined]>,
    workHandler: undefined as WorkHandler | undefined,
    getQueuesCalls: [] as Array<[string[] | undefined]>,
    getQueuesError: undefined as unknown,
    getQueuesResult: [] as QueueResult[],
    getQueueCalls: [] as Array<[string]>,
    getQueueError: undefined as unknown,
    getQueueResult: null as QueueResult | null,

    emit(event: string, ...args: unknown[]) {
      for (const listener of this.listeners.get(event) ?? []) {
        listener(...args);
      }
    },

    reset() {
      this.listeners = new Map();
      this.constructorOptions = undefined;
      this.onCalls = [];
      this.offCalls = [];
      this.startCalls = 0;
      this.stopCalls = 0;
      this.createQueueCalls = [];
      this.createQueueError = undefined;
      this.sendCalls = [];
      this.sendError = undefined;
      this.workCalls = [];
      this.workHandler = undefined;
      this.getQueuesCalls = [];
      this.getQueuesError = undefined;
      this.getQueuesResult = [];
      this.getQueueCalls = [];
      this.getQueueError = undefined;
      this.getQueueResult = null;
    },
  };

  return { fakePgBoss, PgBossMock };
});

vi.mock("pg-boss", () => ({ PgBoss: PgBossMock }));

const objectOptions: PgBossOptions = {
  user: "pgboss",
  password: "pgboss",
  host: "localhost",
  port: 5435,
  database: "pgboss",
};

const connectionStringOptions: PgBossOptions = {
  connectionString: "postgresql://user:pass@localhost:5432/db",
};

const runWithPgBoss = <A, E>(
  effect: Effect.Effect<A, E, PgBoss | PgBossInternal>,
  options: PgBossOptions = objectOptions,
) =>
  Effect.runPromise(
    Effect.scoped(effect.pipe(Effect.provide(PgBoss.layer(options)))),
  );

const invokeWork = (jobs: PgBossJob[], db?: Db) =>
  Effect.gen(function* () {
    const handler = fakePgBoss.workHandler;
    if (!handler) {
      return yield* Effect.die(new Error("missing work handler"));
    }
    return yield* Effect.promise(() => handler(jobs, db));
  });

const fakeJob: PgBossJob = {
  id: "job-1",
  name: "test-queue",
  data: { hello: "world" },
  expireInSeconds: 900,
  heartbeatSeconds: null,
  retryCount: 0,
  signal: new AbortController().signal,
};

afterEach(() => {
  fakePgBoss.reset();
  vi.clearAllMocks();
});

describe("PgBoss", () => {
  it("constructs pg-boss with the provided object options", async () => {
    await runWithPgBoss(Effect.void);

    expect(fakePgBoss.constructorOptions).toEqual(objectOptions);
  });

  it("constructs pg-boss with the connection string options", async () => {
    await runWithPgBoss(Effect.void, connectionStringOptions);

    expect(fakePgBoss.constructorOptions).toEqual(connectionStringOptions);
  });

  it("registers every event listener", async () => {
    await runWithPgBoss(Effect.void);

    const events = fakePgBoss.onCalls.map(([event]) => event);
    expect(events).toEqual([
      "error",
      "warning",
      "wip",
      "stopped",
      "bam",
      "flow",
    ]);
  });

  it("removes every event listener on scope close", async () => {
    await runWithPgBoss(Effect.void);

    const events = fakePgBoss.offCalls.map(([event]) => event);
    expect(events).toEqual([
      "flow",
      "bam",
      "stopped",
      "wip",
      "warning",
      "error",
    ]);
  });

  it("starts on acquire and stops on release", async () => {
    await runWithPgBoss(Effect.void);

    expect(fakePgBoss.startCalls).toBe(1);
    expect(fakePgBoss.stopCalls).toBe(1);
  });

  it("createQueue calls through to pg-boss", async () => {
    await runWithPgBoss(
      Effect.gen(function* () {
        const { createQueue } = yield* PgBossInternal;
        yield* createQueue("test-queue", { retryLimit: 3 });
      }),
    );

    expect(fakePgBoss.createQueueCalls).toEqual([
      ["test-queue", { retryLimit: 3 }],
    ]);
  });

  it("createQueue maps failures to PgBossError", async () => {
    fakePgBoss.createQueueError = new Error("boom");

    const error = await runWithPgBoss(
      Effect.flip(
        Effect.gen(function* () {
          const { createQueue } = yield* PgBossInternal;
          yield* createQueue("test-queue");
        }),
      ),
    );

    expect(error).toBeInstanceOf(PgBossError);
  });

  it("send calls through to pg-boss and returns the job id", async () => {
    const jobId = await runWithPgBoss(
      Effect.gen(function* () {
        const { send } = yield* PgBossInternal;
        return yield* send("test-queue", { hello: "world" });
      }),
    );

    expect(jobId).toBe("job-id");
    expect(fakePgBoss.sendCalls).toEqual([
      ["test-queue", { hello: "world" }, undefined],
    ]);
  });

  it("maps a missing queue to QueueNotFoundError", async () => {
    fakePgBoss.sendError = new Error("Queue test-queue does not exist");

    const error = await runWithPgBoss(
      Effect.flip(
        Effect.gen(function* () {
          const { send } = yield* PgBossInternal;
          return yield* send("test-queue", { hello: "world" });
        }),
      ),
    );

    expect(error).toBeInstanceOf(QueueNotFoundError);
  });

  it("maps other send failures to PgBossError", async () => {
    fakePgBoss.sendError = new Error("boom");

    const error = await runWithPgBoss(
      Effect.flip(
        Effect.gen(function* () {
          const { send } = yield* PgBossInternal;
          return yield* send("test-queue", { hello: "world" });
        }),
      ),
    );

    expect(error).toBeInstanceOf(PgBossError);
  });

  it("work registers a worker and resolves the worker id", async () => {
    const workerId = await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        return yield* work("test-queue", () => Effect.void);
      }),
    );

    expect(workerId).toBe("worker-id");
    expect(fakePgBoss.workCalls).toEqual([["test-queue", undefined]]);
    expect(typeof fakePgBoss.workHandler).toBe("function");
  });

  it("work passes worker options through", async () => {
    await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        yield* work("test-queue", () => Effect.void, {
          pollingIntervalSeconds: 0.5,
        });
      }),
    );

    expect(fakePgBoss.workCalls).toEqual([
      ["test-queue", { pollingIntervalSeconds: 0.5 }],
    ]);
    expect(typeof fakePgBoss.workHandler).toBe("function");
  });

  it("work passes all jobs to the handler", async () => {
    const result = await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        yield* work("test-queue", (jobs) =>
          Effect.succeed(jobs.map((job) => job.data)),
        );

        return yield* invokeWork([
          fakeJob,
          { ...fakeJob, id: "job-2", data: { hello: "again" } },
        ]);
      }),
    );

    expect(result).toEqual([{ hello: "world" }, { hello: "again" }]);
  });

  it("work provides a transaction to transactional workers", async () => {
    const executeSql = vi.fn().mockResolvedValue({ rows: [{ answer: 42 }] });
    const db: Db = { executeSql };

    const result = await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        yield* work(
          "test-queue",
          (_jobs, transaction) => {
            if (!transaction) {
              return Effect.die(new Error("missing transaction"));
            }
            return transaction.executeSql("SELECT 42");
          },
          { transactional: true },
        );

        return yield* invokeWork([fakeJob], db);
      }),
    );

    expect(result).toEqual([{ answer: 42 }]);
    expect(executeSql).toHaveBeenCalledWith("SELECT 42", undefined);
  });

  it("maps transaction SQL failures to PgBossError", async () => {
    const cause = new Error("boom");
    const db: Db = {
      executeSql: () => Promise.reject(cause),
    };

    const result = await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        yield* work(
          "test-queue",
          (_jobs, transaction) =>
            transaction
              ? transaction.executeSql("SELECT 1").pipe(Effect.flip)
              : Effect.die(new Error("missing transaction")),
          { transactional: true },
        );

        return yield* invokeWork([fakeJob], db);
      }),
    );

    expect(result).toBeInstanceOf(PgBossError);
    if (result instanceof PgBossError) {
      expect(result.cause).toBe(cause);
    }
  });

  it("work fails the job when the handler effect fails", async () => {
    const exit = await runWithPgBoss(
      Effect.gen(function* () {
        const { work } = yield* PgBossInternal;
        yield* work("test-queue", () => Effect.fail(new Error("boom")));

        return yield* Effect.exit(invokeWork([fakeJob]));
      }),
    );

    expect(exit._tag).toBe("Failure");
  });

  it("streams error events", async () => {
    const error = await runWithPgBoss(
      Effect.gen(function* () {
        const pgBoss = yield* PgBoss;
        const received = yield* Deferred.make<Error>();
        yield* pgBoss.events.error.pipe(
          Stream.runForEach((event) => Deferred.succeed(received, event)),
          Effect.forkChild,
        );
        yield* Effect.sleep("10 millis");
        fakePgBoss.emit("error", new Error("boom"));
        return yield* Deferred.await(received);
      }),
    );

    expect(error.message).toBe("boom");
  });

  it("streams warning events", async () => {
    const warning = await runWithPgBoss(
      Effect.gen(function* () {
        const pgBoss = yield* PgBoss;
        const received = yield* Deferred.make<unknown>();
        yield* pgBoss.warnings.pipe(
          Stream.runForEach((event) => Deferred.succeed(received, event)),
          Effect.forkChild,
        );
        yield* Effect.sleep("10 millis");
        fakePgBoss.emit("warning", { message: "watch out", data: {} });
        return yield* Deferred.await(received);
      }),
    );

    expect(warning).toEqual({ message: "watch out", data: {} });
  });

  it("getQueues returns all queues", async () => {
    fakePgBoss.getQueuesResult = [
      {
        name: "example",
        policy: "standard",
        retryLimit: 2,
        retryDelay: 0,
        retryBackoff: false,
        expireInSeconds: 900,
        retentionSeconds: 604800,
        deleteAfterSeconds: 604800,
        partition: false,
        notify: false,
        deferredCount: 0,
        blockedCount: 0,
        queuedCount: 5,
        readyCount: 5,
        activeCount: 0,
        failedCount: 0,
        totalCount: 5,
        completedDelta: 0,
        failedDelta: 0,
        createdDelta: 0,
        deltaSeconds: null,
        deltaOn: null,
        waitBins: null,
        runBins: null,
        readyOldestSeconds: null,
        singletonsActive: null,
        table: "job_common",
        createdOn: new Date(),
        updatedOn: new Date(),
      },
    ];

    const queues = await runWithPgBoss(
      Effect.gen(function* () {
        const { getQueues } = yield* PgBoss;
        return yield* getQueues();
      }),
    );

    expect(queues).toHaveLength(1);
    expect(queues[0]?.name).toBe("example");
    expect(fakePgBoss.getQueuesCalls).toEqual([[undefined]]);
  });

  it("getQueues passes names through", async () => {
    await runWithPgBoss(
      Effect.gen(function* () {
        const { getQueues } = yield* PgBoss;
        yield* getQueues(["example"]);
      }),
    );

    expect(fakePgBoss.getQueuesCalls).toEqual([[["example"]]]);
  });

  it("getQueue returns the queue", async () => {
    fakePgBoss.getQueueResult = {
      name: "example",
      policy: "standard",
      retryLimit: 2,
      retryDelay: 0,
      retryBackoff: false,
      expireInSeconds: 900,
      retentionSeconds: 604800,
      deleteAfterSeconds: 604800,
      partition: false,
      notify: false,
      deferredCount: 0,
      blockedCount: 0,
      queuedCount: 1,
      readyCount: 1,
      activeCount: 0,
      failedCount: 0,
      totalCount: 1,
      completedDelta: 0,
      failedDelta: 0,
      createdDelta: 0,
      deltaSeconds: null,
      deltaOn: null,
      waitBins: null,
      runBins: null,
      readyOldestSeconds: null,
      singletonsActive: null,
      table: "job_common",
      createdOn: new Date(),
      updatedOn: new Date(),
    };

    const queue = await runWithPgBoss(
      Effect.gen(function* () {
        const { getQueue } = yield* PgBoss;
        return yield* getQueue("example");
      }),
    );

    expect(queue?.name).toBe("example");
    expect(fakePgBoss.getQueueCalls).toEqual([["example"]]);
  });

  it("getQueue returns null for a missing queue", async () => {
    const queue = await runWithPgBoss(
      Effect.gen(function* () {
        const { getQueue } = yield* PgBoss;
        return yield* getQueue("missing");
      }),
    );

    expect(queue).toBeNull();
  });

  it("getQueues maps failures to PgBossError", async () => {
    fakePgBoss.getQueuesError = new Error("boom");

    const error = await runWithPgBoss(
      Effect.flip(
        Effect.gen(function* () {
          const { getQueues } = yield* PgBoss;
          yield* getQueues();
        }),
      ),
    );

    expect(error).toBeInstanceOf(PgBossError);
  });

  it("getQueue maps failures to PgBossError", async () => {
    fakePgBoss.getQueueError = new Error("boom");

    const error = await runWithPgBoss(
      Effect.flip(
        Effect.gen(function* () {
          const { getQueue } = yield* PgBoss;
          yield* getQueue("example");
        }),
      ),
    );

    expect(error).toBeInstanceOf(PgBossError);
  });
});
