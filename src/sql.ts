/**
 * SQL schema and entity-per-row storage helpers
 *
 * Uses entity-per-row pattern to minimize memory pressure on the DO.
 * Each entity is stored as its own row rather than a large JSON blob.
 */

import type {
  EntityManifest,
  DescriptionContext,
  JobPhase,
  JobState,
  RelationshipRef,
} from './types';

/**
 * Initialize SQL schema
 */
export function initSchema(sql: SqlStorage): void {
  // Create entities table - each entity is its own row
  sql.exec(`
    CREATE TABLE IF NOT EXISTS entities (
      id TEXT PRIMARY KEY,
      predicate TEXT,
      manifest TEXT NOT NULL,
      is_target INTEGER NOT NULL DEFAULT 0
    )
  `);

  // Create job state table
  sql.exec(`
    CREATE TABLE IF NOT EXISTS describe_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);
}

/**
 * Store the target entity
 */
export function storeTarget(sql: SqlStorage, entity: EntityManifest): void {
  sql.exec(
    `INSERT OR REPLACE INTO entities (id, predicate, manifest, is_target) VALUES (?, NULL, ?, 1)`,
    entity.id,
    JSON.stringify(entity)
  );
}

/**
 * Store a related entity with its predicate
 */
export function storeRelated(
  sql: SqlStorage,
  id: string,
  predicate: string,
  entity: EntityManifest
): void {
  sql.exec(
    `INSERT OR REPLACE INTO entities (id, predicate, manifest, is_target) VALUES (?, ?, ?, 0)`,
    id,
    predicate,
    JSON.stringify(entity)
  );
}

/**
 * Read all entities from SQL and build context
 */
export function readAllEntities(sql: SqlStorage): DescriptionContext {
  // Read target
  const targetRow = sql
    .exec(`SELECT manifest FROM entities WHERE is_target = 1 LIMIT 1`)
    .one();

  if (!targetRow) {
    throw new Error('No target entity found in SQL');
  }

  const target = JSON.parse(targetRow.manifest as string) as EntityManifest;

  // Read related entities
  const relatedRows = sql
    .exec(`SELECT predicate, manifest FROM entities WHERE is_target = 0`)
    .toArray();

  const related = relatedRows.map((row) => ({
    predicate: row.predicate as string,
    manifest: JSON.parse(row.manifest as string) as EntityManifest,
  }));

  return { target, related };
}

/**
 * Get current job state
 */
export function getJobState(sql: SqlStorage): JobState {
  const rows = sql.exec(`SELECT key, value FROM describe_state`).toArray();

  const state: Record<string, string> = {};
  for (const row of rows) {
    state[row.key as string] = row.value as string;
  }

  return {
    phase: (state.phase as JobPhase) || 'FETCH_TARGET',
    nextBatchIndex: parseInt(state.nextBatchIndex || '0', 10),
    totalRelationships: parseInt(state.totalRelationships || '0', 10),
    relationshipIds: state.relationshipIds
      ? (JSON.parse(state.relationshipIds) as RelationshipRef[])
      : [],
  };
}

/**
 * Set job state (partial update)
 */
export function setJobState(sql: SqlStorage, updates: Partial<JobState>): void {
  for (const [key, value] of Object.entries(updates)) {
    const stringValue =
      typeof value === 'object' ? JSON.stringify(value) : String(value);
    sql.exec(
      `INSERT OR REPLACE INTO describe_state (key, value) VALUES (?, ?)`,
      key,
      stringValue
    );
  }
}

/**
 * Clear all state (cleanup after completion)
 */
export function clearState(sql: SqlStorage): void {
  sql.exec(`DELETE FROM entities`);
  sql.exec(`DELETE FROM describe_state`);
}

/**
 * Get count of stored related entities
 */
export function getRelatedCount(sql: SqlStorage): number {
  const row = sql
    .exec(`SELECT COUNT(*) as count FROM entities WHERE is_target = 0`)
    .one();
  return (row?.count as number) || 0;
}
