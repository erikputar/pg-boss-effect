---
"pgboss-effect": minor
---

Add transactional workers with a `PgBossTransaction` passed to the handler, support `includeMetadata` workers, and re-export pg-boss public types unchanged. Worker options no longer accept unsupported settings silently. The pg-boss peer requirement is now ^12.32.0 for transactional workers.
