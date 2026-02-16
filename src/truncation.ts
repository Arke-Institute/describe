/**
 * Progressive tax truncation algorithm
 *
 * Implements proportional truncation across all text fields to fit
 * within token budget. Larger fields are truncated more (proportionally).
 */

import type {
  DescriptionContext,
  TruncationStats,
  TruncationConfig,
  EntityManifest,
} from './types';

/**
 * Protected fields that are never truncated
 */
const PROTECTED_FIELDS = new Set([
  'id',
  'cid',
  'type',
  'label',
  'predicate',
  'peer',
  'peer_type',
  'peer_label',
  'created_at',
  'updated_at',
]);

/**
 * Minimum characters per field (don't truncate below this)
 */
const MIN_CHARS_PER_FIELD = 300;

/**
 * Estimate tokens from text (conservative: 1 token ≈ 3 chars)
 */
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 3);
}

/**
 * Calculate available token budget for context
 */
export function calculateBudget(config: TruncationConfig): number {
  const rawAvailable =
    config.contextWindowTokens -
    config.systemPromptTokens -
    config.maxOutputTokens;

  return Math.floor(rawAvailable * config.safetyMargin);
}

/**
 * Truncate text at word boundary
 */
export function truncateAtWordBoundary(text: string, maxChars: number): string {
  if (text.length <= maxChars) {
    return text;
  }

  let truncated = text.slice(0, maxChars);

  // Try to break at a word boundary (80% threshold)
  const lastSpace = truncated.lastIndexOf(' ');
  if (lastSpace > maxChars * 0.8) {
    truncated = truncated.slice(0, lastSpace);
  }

  return truncated + '\n... [truncated]';
}

/**
 * Represents a truncatable field in the context
 */
interface TruncatableField {
  entityId: string;
  field: string;
  chars: number;
  isTarget: boolean;
}

/**
 * Collect all truncatable text fields from context
 */
function collectTruncatableFields(context: DescriptionContext): TruncatableField[] {
  const fields: TruncatableField[] = [];

  // Helper to add fields from an entity
  function addEntityFields(entity: EntityManifest, isTarget: boolean): void {
    for (const [key, value] of Object.entries(entity.properties)) {
      if (PROTECTED_FIELDS.has(key)) continue;
      if (typeof value !== 'string') continue;
      if (value.length < MIN_CHARS_PER_FIELD) continue;

      fields.push({
        entityId: entity.id,
        field: key,
        chars: value.length,
        isTarget,
      });
    }
  }

  // Add target fields
  addEntityFields(context.target, true);

  // Add related entity fields
  for (const related of context.related) {
    addEntityFields(related.manifest, false);
  }

  return fields;
}

/**
 * Estimate total tokens in context
 */
function estimateContextTokens(context: DescriptionContext): number {
  let totalChars = 0;

  // Count target
  totalChars += JSON.stringify(context.target).length;

  // Count related
  for (const related of context.related) {
    totalChars += JSON.stringify(related).length;
  }

  return estimateTokens(totalChars.toString()) + Math.ceil(totalChars / 3);
}

/**
 * Apply progressive tax truncation to fit within token budget
 *
 * The algorithm:
 * 1. Estimate current token usage
 * 2. If under budget, return unchanged
 * 3. Calculate deficit (how much we need to cut)
 * 4. Collect all truncatable fields
 * 5. Apply proportional "tax" - larger fields pay more
 * 6. Truncate each field to its new target size
 */
export function applyProgressiveTax(
  context: DescriptionContext,
  config: TruncationConfig
): { context: DescriptionContext; stats: TruncationStats } {
  const targetTokens = calculateBudget(config);
  const beforeTokens = estimateContextTokens(context);

  // If under budget, return unchanged
  if (beforeTokens <= targetTokens) {
    return {
      context,
      stats: {
        truncated: false,
        beforeTokens,
        afterTokens: beforeTokens,
        fieldsProtected: 0,
        fieldsTruncated: 0,
      },
    };
  }

  // Collect truncatable fields
  const fields = collectTruncatableFields(context);

  if (fields.length === 0) {
    // Nothing to truncate
    return {
      context,
      stats: {
        truncated: false,
        beforeTokens,
        afterTokens: beforeTokens,
        fieldsProtected: 0,
        fieldsTruncated: 0,
      },
    };
  }

  // Calculate deficit
  const deficitTokens = beforeTokens - targetTokens;
  const deficitChars = deficitTokens * 3; // Convert to chars

  // Total truncatable chars
  const totalTruncatableChars = fields.reduce((sum, f) => sum + f.chars, 0);

  // Calculate target size for each field (proportional tax)
  const truncationMap = new Map<string, number>();
  let fieldsProtected = 0;
  let fieldsTruncated = 0;

  for (const field of fields) {
    const proportion = field.chars / totalTruncatableChars;
    const tax = Math.ceil(proportion * deficitChars);
    const targetChars = Math.max(MIN_CHARS_PER_FIELD, field.chars - tax);

    const key = `${field.entityId}:${field.field}`;

    if (targetChars >= field.chars) {
      // No truncation needed for this field
      fieldsProtected++;
    } else {
      truncationMap.set(key, targetChars);
      fieldsTruncated++;
    }
  }

  // Apply truncation to create new context
  const newContext = applyTruncationMap(context, truncationMap);
  const afterTokens = estimateContextTokens(newContext);

  return {
    context: newContext,
    stats: {
      truncated: true,
      beforeTokens,
      afterTokens,
      fieldsProtected,
      fieldsTruncated,
    },
  };
}

/**
 * Apply truncation map to create new context with truncated fields
 */
function applyTruncationMap(
  context: DescriptionContext,
  truncationMap: Map<string, number>
): DescriptionContext {
  // Deep clone and truncate
  const newTarget = truncateEntity(context.target, truncationMap);

  const newRelated = context.related.map((r) => ({
    predicate: r.predicate,
    manifest: truncateEntity(r.manifest, truncationMap),
  }));

  return {
    target: newTarget,
    related: newRelated,
  };
}

/**
 * Truncate fields in an entity based on truncation map
 */
function truncateEntity(
  entity: EntityManifest,
  truncationMap: Map<string, number>
): EntityManifest {
  const newProperties: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(entity.properties)) {
    const mapKey = `${entity.id}:${key}`;
    const targetChars = truncationMap.get(mapKey);

    if (targetChars !== undefined && typeof value === 'string') {
      newProperties[key] = truncateAtWordBoundary(value, targetChars);
    } else {
      newProperties[key] = value;
    }
  }

  return {
    ...entity,
    properties: newProperties,
  };
}
