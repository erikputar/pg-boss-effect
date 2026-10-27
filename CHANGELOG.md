# pgboss-effect

## 0.2.0

### Minor Changes

- 8d1cab5: Add typed dead-letter, schedule, startup/config, and transaction database errors, and stop workers when their layer scope closes.
- 8d1cab5: Add transactional workers with a `PgBossTransaction` passed to the handler, support `includeMetadata` workers, and re-export pg-boss public types unchanged. Worker options no longer accept unsupported settings silently. The pg-boss peer requirement is now ^12.32.0 for transactional workers.
- 8d1cab5: Allow `PgBoss.layer` to accept a connection string directly and add `PgBoss.getInstances` for cross-process worker and instance health.
- 8d1cab5: Add declarative queue schedules, schedule reads/removal on queues, and `PgBoss.previewSchedule`.

## 0.1.2

### Patch Changes

- 118c341: Update the package description and README.

## 0.1.1

### Patch Changes

- 6d63381: Document that the package is ESM-only.

## 0.1.0

### Minor Changes

- Initial release. Typed queues and workers for pg-boss, with a scoped `PgBoss` layer, schema-encoded payloads, single and batch workers, and error/event streams.
- Install with `pnpm add pgboss-effect effect pg-boss`.
