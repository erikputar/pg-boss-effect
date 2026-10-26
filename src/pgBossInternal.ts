import { Context, Data, Effect, PubSub, type Scope, Stream } from "effect";
import {
  type ConstructorOptions,
  type PgBossEventMap,
  type Job as PgBossJob,
  type Queue,
  type QueueResult,
  type SendOptions,
  type Warning,
  type WorkOptions,
  PgBoss as _PgBoss,
} from "pg-boss";

export type { QueueResult } from "pg-boss";

export class PgBossError extends Data.TaggedError("PgBossError")<{
  cause: unknown;
}> {}

export class QueueNotFoundError extends Data.TaggedError("QueueNotFoundError")<{
  queue: string;
  cause: Error;
}> {}

const isQueueNotFound = (cause: unknown, queue: string): cause is Error =>
  cause instanceof Error && cause.message === `Queue ${queue} does not exist`;

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
}

export interface PgBossInternalService {
  readonly createQueue: (
    name: string,
    options?: Omit<Queue, "name">,
  ) => Effect.Effect<void, PgBossError>;

  readonly send: (
    name: string,
    data?: object | null,
    options?: SendOptions,
  ) => Effect.Effect<string | null, PgBossError | QueueNotFoundError>;

  readonly work: <A, E, R>(
    queueName: string,
    handler: (jobs: PgBossJob[]) => Effect.Effect<A, E, R>,
    options?: WorkOptions,
  ) => Effect.Effect<string, PgBossError, R>;
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
): Effect.Effect<PgBossRuntime, PgBossError, Scope.Scope> =>
  Effect.gen(function* () {
    const context = yield* Effect.context();
    const pgBoss = new _PgBoss(options);

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
        catch: (cause) => new PgBossError({ cause }),
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
        catch: (cause) => new PgBossError({ cause }),
      });

    const send = (
      name: string,
      data?: object | null,
      sendOptions?: SendOptions,
    ) =>
      Effect.tryPromise({
        try: () => pgBoss.send(name, data, sendOptions),
        catch: (cause) =>
          isQueueNotFound(cause, name)
            ? new QueueNotFoundError({ queue: name, cause })
            : new PgBossError({ cause }),
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

    const work = <A, E, R>(
      queueName: string,
      handler: (jobs: PgBossJob[]) => Effect.Effect<A, E, R>,
      workOptions?: WorkOptions,
    ) =>
      Effect.gen(function* () {
        const workContext = yield* Effect.context<R>();
        return yield* Effect.tryPromise({
          try: () => {
            const cb = (jobs: PgBossJob[]) =>
              Effect.runPromiseWith(workContext)(handler(jobs));
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

    const internal: PgBossInternalService = {
      createQueue,
      send,
      work,
    };

    const publicService: PgBossService = {
      getQueues,
      getQueue,
      events,
      errors: events.error,
      warnings: events.warning,
    };

    return { internal, public: publicService };
  }).pipe(Effect.annotateLogs({ where: "PgBoss" }));
