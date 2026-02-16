/**
 * Context gathering utilities
 *
 * Handles fetching the target entity and batch fetching related entities.
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { EntityManifest, RelationshipRef } from './types';

/**
 * Fetch target entity (no expand - just get relationship IDs)
 */
export async function fetchTarget(
  client: ArkeClient,
  entityId: string
): Promise<EntityManifest> {
  const { data, error } = await client.api.GET('/entities/{id}', {
    params: { path: { id: entityId } },
  });

  if (error || !data) {
    throw new Error(`Failed to fetch target entity ${entityId}: ${JSON.stringify(error)}`);
  }

  return {
    id: data.id,
    type: data.type,
    cid: data.cid,
    properties: data.properties as Record<string, unknown>,
    relationships: data.relationships as EntityManifest['relationships'],
  };
}

/**
 * Extract relationship IDs from an entity
 *
 * @param relationships - Entity relationships array
 * @param predicates - Optional filter to specific predicates
 * @param maxRelationships - Maximum number of relationships to include
 */
export function extractRelationshipIds(
  relationships: EntityManifest['relationships'],
  predicates?: string[],
  maxRelationships?: number
): RelationshipRef[] {
  if (!relationships || relationships.length === 0) {
    return [];
  }

  // Filter by predicates if specified
  let filtered = relationships;
  if (predicates && predicates.length > 0) {
    const predicateSet = new Set(predicates);
    filtered = relationships.filter((r) => predicateSet.has(r.predicate));
  }

  // Map to RelationshipRef
  const refs = filtered.map((r) => ({
    id: r.peer,
    predicate: r.predicate,
  }));

  // Apply max limit
  if (maxRelationships && refs.length > maxRelationships) {
    return refs.slice(0, maxRelationships);
  }

  return refs;
}

/**
 * Batch fetch entities using the batch-get endpoint
 *
 * @param client - Arke client
 * @param ids - Entity IDs to fetch
 * @returns Map of entity ID to manifest (missing entities are omitted)
 */
export async function batchFetchEntities(
  client: ArkeClient,
  ids: string[]
): Promise<Map<string, EntityManifest>> {
  if (ids.length === 0) {
    return new Map();
  }

  // POST /entities/batch-get expects { ids: string[] }
  const { data, error } = await (client.api.POST as Function)('/entities/batch-get', {
    body: { ids },
  });

  if (error) {
    console.error('[context] Batch fetch error:', error);
    // Return empty map on error - caller will handle missing entities
    return new Map();
  }

  const result = new Map<string, EntityManifest>();

  // Response is { entities: EntityManifest[] }
  const entities = (data as { entities?: unknown[] })?.entities || [];

  for (const entity of entities) {
    const e = entity as EntityManifest;
    if (e && e.id) {
      result.set(e.id, {
        id: e.id,
        type: e.type,
        cid: e.cid,
        properties: e.properties || {},
        relationships: e.relationships,
      });
    }
  }

  return result;
}

/**
 * Get the next batch of relationship IDs to fetch
 *
 * @param relationshipIds - All relationship IDs
 * @param nextBatchIndex - Starting index for next batch
 * @param batchSize - Number of entities per batch
 */
export function getNextBatch(
  relationshipIds: RelationshipRef[],
  nextBatchIndex: number,
  batchSize: number
): RelationshipRef[] {
  return relationshipIds.slice(nextBatchIndex, nextBatchIndex + batchSize);
}

/**
 * Check if there are more batches to fetch
 */
export function hasMoreBatches(
  totalRelationships: number,
  nextBatchIndex: number
): boolean {
  return nextBatchIndex < totalRelationships;
}
