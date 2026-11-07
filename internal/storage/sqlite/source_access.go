package sqlite

// Source-side access is an independent ceiling, not a replacement for the
// project's allowlist. Apply it BEFORE LIMIT so private channels do not
// exhaust another project's bounded selection/refill/Ready page.
// Every caller uses the code-owned `src` alias and binds the project ID.
const sourceProjectAccessClause = ` AND (
 json_type(src.config_json,'$.access.project_ids') IS NULL
 OR json_type(src.config_json,'$.access.project_ids')='null'
 OR EXISTS(SELECT 1 FROM json_each(src.config_json,'$.access.project_ids') WHERE value=?)
)`
