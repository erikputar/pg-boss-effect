import { Data } from "effect";

export class DeadLetterQueueNotFoundError extends Data.TaggedError(
  "DeadLetterQueueNotFoundError",
)<{
  queue: string;
  deadLetterQueue: string | null;
  cause: Error;
}> {}

export class InvalidScheduleError extends Data.TaggedError(
  "InvalidScheduleError",
)<{
  expression: string;
  cause: unknown;
}> {}

export class PgBossConfigError extends Data.TaggedError("PgBossConfigError")<{
  cause: unknown;
}> {}

export class PgBossNotInstalledError extends Data.TaggedError(
  "PgBossNotInstalledError",
)<{
  cause: unknown;
}> {}

export class PgBossMigrationsRequiredError extends Data.TaggedError(
  "PgBossMigrationsRequiredError",
)<{
  cause: unknown;
}> {}

export class PgBossDatabaseError extends Data.TaggedError(
  "PgBossDatabaseError",
)<{
  cause: unknown;
  code?: string;
  constraint?: string;
  detail?: string;
  table?: string;
  column?: string;
}> {}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const getString = (value: unknown, key: string): string | undefined => {
  if (!isRecord(value) || !(key in value)) {
    return undefined;
  }
  const field = value[key];
  return typeof field === "string" ? field : undefined;
};

const getDatabaseCode = (cause: unknown): string | undefined => {
  const code = getString(cause, "code");
  if (code !== undefined) {
    return code;
  }
  if (!isRecord(cause) || !("errno" in cause)) {
    return undefined;
  }
  const errno = cause.errno;
  if (typeof errno === "string") {
    return errno;
  }
  return typeof errno === "number" ? String(errno) : undefined;
};

export const fromDatabaseError = (cause: unknown): PgBossDatabaseError =>
  new PgBossDatabaseError({
    cause,
    code: getDatabaseCode(cause),
    constraint:
      getString(cause, "constraint") ?? getString(cause, "constraint_name"),
    detail: getString(cause, "detail"),
    table: getString(cause, "table"),
    column: getString(cause, "column"),
  });

export const deadLetterQueueFromCause = (
  cause: unknown,
): string | null | undefined => {
  if (!(cause instanceof Error)) {
    return undefined;
  }
  if (cause.message === "Dead letter queue does not exist") {
    return null;
  }
  const match = /^Dead letter queue (.+) does not exist$/.exec(cause.message);
  return match?.[1];
};

export const isInvalidScheduleCause = (cause: unknown): cause is Error => {
  if (!(cause instanceof Error)) {
    return false;
  }
  if (!("code" in cause)) {
    return true;
  }
  return cause.code === "ERR_ASSERTION" || cause.code === undefined;
};
