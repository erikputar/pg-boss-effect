import { Context, Data, Effect, PubSub, type Scope, Stream } from "effect";
import {
  type ConstructorOptions,
  type Db,
  type OffWorkOptions,
  type PgBossEventMap,
  type Job as PgBossJob,
  type PreviewScheduleOptions,
  type Queue,
  type QueueResult,
  type Schedule,
  type ScheduleOptions,
  type SendOptions,
  type Warning,
  type WorkOptions,
  PgBoss as _PgBoss,
} from "pg-boss";

import {
  DeadLetterQueueNotFoundError,
  InvalidScheduleError,
  PgBossConfigError,
  PgBossMigrationsRequiredError,
  PgBossNotInstalledError,
  deadLetterQueueFromCause,
  fromDatabaseError,
  isInvalidScheduleCause,
} from "./errors";
import type { PgBossTransaction } from "./transaction";

export class PgBossError extends Data.TaggedError("PgBossError")<{
  cause: unknown;
}> {}

export class QueueNotFoundError extends Data.TaggedError("QueueNotFoundError")<{
  queue: string;
  cause: Error;
}> {}

export type PgBossRuntimeError =
  | PgBossError
  | PgBossConfigError
  | PgBossNotInstalledError
  | PgBossMigrationsRequiredError;

const isQueueNotFound = (cause: unknown, queue: string): cause is Error =>
  cause instanceof Error &&
  (("code" in cause && cause.code === "PGBOSS_QUEUE_NOT_FOUND") ||
    cause.message === `Queue ${queue} does not exist` ||
    cause.message === `Queue ${queue} not found`);

const isNotInstalled = (cause: unknown): cause is Error =>
  cause instanceof Error && cause.message.includes("pg-boss is not installed");

const isMigrationsRequired = (cause: unknown): cause is Error =>
  cause instanceof Error &&
  cause.message.includes("pg-boss database requires migrations");

const startError = (cause: unknown): PgBossRuntimeError => {
  if (isNotInstalled(cause)) {
    return new PgBossNotInstalledError({ cause });
  }
  if (isMigrationsRequired(cause)) {
    return new PgBossMigrationsRequiredError({ cause });
  }
  return new PgBossError({ cause });
};

export type PgBossOptions = ConstructorOptions;

export type PgBossEventPayload<E extends keyof PgBossEventMap> =
  PgBossEventMap[E] extends readonly [infer Payload, ...ReadonlyArray<unknown>]
    ? Payload
    : undefined;

export type PgBossEvents = {
  readonly [E in keyof PgBossEventMap]: Stream.Stream<PgBossEventPayload<E>>;
};

type WipPayload = PgBossEventMap["wip"][0];
type BamPayload = PgBossEventMap["bam"][0];
type FlowPayload = PgBossEventMap["flow"][0];

export interface PgBossService {
  readonly getQueues: (
    names?: string[],
  ) => Effect.Effect<QueueResult[], PgBossError>;

  readonly getQueue: (
    name: string,
  ) => Effect.Effect<QueueResult | null, PgBossError>;

  readonly events: PgBossEvents;

  readonly errors: Stream.Stream<Error>;

  readonly warnings: Stream.Stream<Warning>;

  readonly previewSchedule: (
    expression: string,
    options?: PreviewScheduleOptions,
  ) => Effect.Effect<ReadonlyArray<Date>, InvalidScheduleError>;
}

export interface PgBossInternalService {
  readonly createQueue: (
    name: string,
    options?: Omit<Queue, "name">,
  ) => Effect.Effect<void, PgBossError | DeadLetterQueueNotFoundError>;

  readonly send: (
    name: string,
    data?: object | null,
    options?: SendOptions,
  ) => Effect.Effect<
    string | null,
    PgBossError | QueueNotFoundError | DeadLetterQueueNotFoundError
  >;

  readonly work: <A, E, R>(
    queueName: string,
    handler: (
      jobs: PgBossJob[],
      transaction: PgBossTransaction | undefined,
    ) => Effect.Effect<A, E, R>,
    options?: WorkOptions,
  ) => Effect.Effect<string, PgBossError, R>;

  readonly schedule: (
    name: string,
    cron: string,
    data?: object | null,
    options?: ScheduleOptions,
  ) => Effect.Effect<
    void,
    PgBossError | QueueNotFoundError | InvalidScheduleError
  >;

  readonly unschedule: (
    name: string,
    key?: string,
  ) => Effect.Effect<void, PgBossError>;

  readonly getSchedules: (
    name?: string,
    key?: string,
  ) => Effect.Effect<Schedule[], PgBossError>;

  readonly getSchedule: (
    name: string,
    key?: string,
  ) => Effect.Effect<Schedule | null, PgBossError>;

  readonly offWork: (
    name: string,
    options?: OffWorkOptions,
  ) => Effect.Effect<void, PgBossError>;
}

export class PgBossInternal extends Context.Service<
  PgBossInternal,
  PgBossInternalService
>()("PgBossInternal") {}

export interface PgBossRuntime {
  readonly public: PgBossService;
  readonly internal: PgBossInternalService;
}

export const makeRuntime = (
  options: PgBossOptions,
): Effect.Effect<PgBossRuntime, PgBossRuntimeError, Scope.Scope> =>
  Effect.gen(function* () {
    const context = yield* Effect.context();
    const pgBoss = yield* Effect.try({
      try: () => new _PgBoss(options),
      catch: (cause) => new PgBossConfigError({ cause }),
    });

    const errorHub = yield* Effect.acquireRelease(
      PubSub.unbounded<Error>(),
      PubSub.shutdown,
    );
    const warningHub = yield* Effect.acquireRelease(
      PubSub.unbounded<Warning>(),
      PubSub.shutdown,
    );
    const wipHub = yield* Effect.acquireRelease(
      PubSub.unbounded<WipPayload>(),
      PubSub.shutdown,
    );
    const stoppedHub = yield* Effect.acquireRelease(
      PubSub.unbounded<undefined>(),
      PubSub.shutdown,
    );
    const bamHub = yield* Effect.acquireRelease(
      PubSub.unbounded<BamPayload>(),
      PubSub.shutdown,
    );
    const flowHub = yield* Effect.acquireRelease(
      PubSub.unbounded<FlowPayload>(),
      PubSub.shutdown,
    );

    const publish = <A>(hub: PubSub.PubSub<A>, value: A) => {
      Effect.runForkWith(context)(
        PubSub.publish(hub, value).pipe(Effect.ignore),
      );
    };

    const errorListener = (error: Error) => publish(errorHub, error);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("error", errorListener)),
      () => Effect.sync(() => pgBoss.off("error", errorListener)),
    );

    const warningListener = (warning: Warning) => publish(warningHub, warning);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("warning", warningListener)),
      () => Effect.sync(() => pgBoss.off("warning", warningListener)),
    );

    const wipListener = (data: WipPayload) => publish(wipHub, data);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("wip", wipListener)),
      () => Effect.sync(() => pgBoss.off("wip", wipListener)),
    );

    const stoppedListener = () => publish(stoppedHub, undefined);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("stopped", stoppedListener)),
      () => Effect.sync(() => pgBoss.off("stopped", stoppedListener)),
    );

    const bamListener = (data: BamPayload) => publish(bamHub, data);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("bam", bamListener)),
      () => Effect.sync(() => pgBoss.off("bam", bamListener)),
    );

    const flowListener = (data: FlowPayload) => publish(flowHub, data);
    yield* Effect.acquireRelease(
      Effect.sync(() => pgBoss.on("flow", flowListener)),
      () => Effect.sync(() => pgBoss.off("flow", flowListener)),
    );

    yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () => pgBoss.start(),
        catch: (cause) => startError(cause),
      }).pipe(Effect.tap(Effect.logInfo("pg-boss started"))),
      () =>
        Effect.tryPromise({
          try: () => pgBoss.stop(),
          catch: (cause) => new PgBossError({ cause }),
        }).pipe(Effect.tap(Effect.logInfo("pg-boss stopped")), Effect.orDie),
    );

    const createQueue = (name: string, queueOptions?: Omit<Queue, "name">) =>
      Effect.tryPromise({
        try: () => pgBoss.createQueue(name, queueOptions),
        catch: (cause) => {
          const deadLetterQueue = queueOptions?.deadLetter;
          if (
            deadLetterQueue !== undefined &&
            isQueueNotFound(cause, deadLetterQueue)
          ) {
            return new DeadLetterQueueNotFoundError({
              queue: name,
              deadLetterQueue,
              cause,
            });
          }
          return new PgBossError({ cause });
        },
      });

    const send = (
      name: string,
      data?: object | null,
      sendOptions?: SendOptions,
    ) =>
      Effect.tryPromise({
        try: () => pgBoss.send(name, data, sendOptions),
        catch: (cause) => {
          if (isQueueNotFound(cause, name)) {
            return new QueueNotFoundError({ queue: name, cause });
          }
          const deadLetterQueue = deadLetterQueueFromCause(cause);
          if (deadLetterQueue !== undefined) {
            return new DeadLetterQueueNotFoundError({
              queue: name,
              deadLetterQueue,
              cause: cause instanceof Error ? cause : new Error(String(cause)),
            });
          }
          return new PgBossError({ cause });
        },
      });

    const getQueues = (names?: string[]) =>
      Effect.tryPromise({
        try: () => pgBoss.getQueues(names),
        catch: (cause) => new PgBossError({ cause }),
      });

    const getQueue = (name: string) =>
      Effect.tryPromise({
        try: () => pgBoss.getQueue(name),
        catch: (cause) => new PgBossError({ cause }),
      });

    const schedule = (
      name: string,
      cron: string,
      data?: object | null,
      scheduleOptions?: ScheduleOptions,
    ) =>
      Effect.tryPromise({
        try: () => pgBoss.schedule(name, cron, data, scheduleOptions),
        catch: (cause) => {
          if (isQueueNotFound(cause, name)) {
            return new QueueNotFoundError({ queue: name, cause });
          }
          if (isInvalidScheduleCause(cause)) {
            return new InvalidScheduleError({ expression: cron, cause });
          }
          return new PgBossError({ cause });
        },
      });

    const unschedule = (name: string, key?: string) =>
      Effect.tryPromise({
        try: () => pgBoss.unschedule(name, key),
        catch: (cause) => new PgBossError({ cause }),
      });

    const getSchedules = (name?: string, key?: string) =>
      Effect.tryPromise({
        try: () => pgBoss.getSchedules(name, key),
        catch: (cause) => new PgBossError({ cause }),
      });

    const getSchedule = (name: string, key?: string) =>
      Effect.tryPromise({
        try: () => pgBoss.getSchedule(name, key),
        catch: (cause) => new PgBossError({ cause }),
      });

    const previewSchedule = (
      expression: string,
      previewOptions?: PreviewScheduleOptions,
    ) =>
      Effect.try({
        try: () => pgBoss.previewSchedule(expression, previewOptions),
        catch: (cause) => new InvalidScheduleError({ expression, cause }),
      });

    const makeTransaction = (db: Db): PgBossTransaction => ({
      db,
      executeSql: (text, values) =>
        Effect.tryPromise({
          try: () => db.executeSql(text, values ? [...values] : undefined),
          catch: (cause) => fromDatabaseError(cause),
        }).pipe(Effect.map((result) => result.rows)),
    });

    const work = <A, E, R>(
      queueName: string,
      handler: (
        jobs: PgBossJob[],
        transaction: PgBossTransaction | undefined,
      ) => Effect.Effect<A, E, R>,
      workOptions?: WorkOptions,
    ) =>
      Effect.gen(function* () {
        const workContext = yield* Effect.context<R>();
        return yield* Effect.tryPromise({
          try: () => {
            const cb = (jobs: PgBossJob[], db?: Db) =>
              Effect.runPromiseWith(workContext)(
                handler(jobs, db ? makeTransaction(db) : undefined),
              );
            return workOptions
              ? pgBoss.work(queueName, workOptions, cb)
              : pgBoss.work(queueName, cb);
          },
          catch: (cause) => new PgBossError({ cause }),
        });
      });

    const events: PgBossEvents = {
      error: Stream.fromPubSub(errorHub),
      warning: Stream.fromPubSub(warningHub),
      wip: Stream.fromPubSub(wipHub),
      stopped: Stream.fromPubSub(stoppedHub),
      bam: Stream.fromPubSub(bamHub),
      flow: Stream.fromPubSub(flowHub),
    };

    const offWork = (name: string, offWorkOptions?: OffWorkOptions) =>
      Effect.tryPromise({
        try: () => pgBoss.offWork(name, offWorkOptions),
        catch: (cause) => new PgBossError({ cause }),
      });

    const internal: PgBossInternalService = {
      createQueue,
      send,
      work,
      schedule,
      unschedule,
      getSchedules,
      getSchedule,
      offWork,
    };

    const publicService: PgBossService = {
      getQueues,
      getQueue,
      events,
      errors: events.error,
      warnings: events.warning,
      previewSchedule,
    };

    return { internal, public: publicService };
  }).pipe(Effect.annotateLogs({ where: "PgBoss" }));
