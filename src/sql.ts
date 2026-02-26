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
  ContentRef,
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

  // Create content refs table - stores Gemini file URIs (not content itself)
  sql.exec(`
    CREATE TABLE IF NOT EXISTS content_refs (
      key TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      content_type TEXT,
      size INTEGER,
      file_uri TEXT,
      reason TEXT
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
    contentKeys: state.contentKeys
      ? (JSON.parse(state.contentKeys) as string[])
      : [],
    contentIndex: parseInt(state.contentIndex || '0', 10),
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
  sql.exec(`DELETE FROM content_refs`);
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

/**
 * Store a content reference (Gemini file URI)
 */
export function storeContentRef(sql: SqlStorage, ref: ContentRef): void {
  sql.exec(
    `INSERT OR REPLACE INTO content_refs (key, status, content_type, size, file_uri, reason) VALUES (?, ?, ?, ?, ?, ?)`,
    ref.key,
    ref.status,
    ref.contentType || null,
    ref.size || null,
    ref.fileUri || null,
    ref.reason || null
  );
}

/**
 * Read all content references
 */
export function readContentRefs(sql: SqlStorage): ContentRef[] {
  const rows = sql.exec(`SELECT * FROM content_refs`).toArray();
  return rows.map((row) => ({
    key: row.key as string,
    status: row.status as ContentRef['status'],
    contentType: row.content_type as string | undefined,
    size: row.size as number | undefined,
    fileUri: row.file_uri as string | undefined,
    reason: row.reason as string | undefined,
  }));
}

/**
 * Read only successful content references (with file URIs)
 */
export function readSuccessfulContentRefs(sql: SqlStorage): ContentRef[] {
  const rows = sql
    .exec(`SELECT * FROM content_refs WHERE status = 'success'`)
    .toArray();
  return rows.map((row) => ({
    key: row.key as string,
    status: 'success' as const,
    contentType: row.content_type as string,
    size: row.size as number,
    fileUri: row.file_uri as string,
  }));
}
