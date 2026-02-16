/**
 * Job Processing Logic for Description Generation
 *
 * Implements a DO state machine with phases:
 * 1. FETCH_TARGET - Fetch target entity and extract relationship IDs
 * 2. FETCH_BATCH - Batch fetch related entities (100 at a time)
 * 3. GENERATE - Build context, truncate, call LLM, update entity
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger, KladosRequest, Output } from '@arke-institute/rhiza';
import type { Env, DescribeConfig } from './types';
import {
  initSchema,
  storeTarget,
  storeRelated,
  readAllEntities,
  getJobState,
  setJobState,
  clearState,
} from './sql';
import {
  fetchTarget,
  extractRelationshipIds,
  batchFetchEntities,
  getNextBatch,
  hasMoreBatches,
} from './context';
import { applyProgressiveTax, estimateTokens } from './truncation';
import { buildSystemPrompt, buildUserPrompt, estimateSystemPromptTokens } from './prompts';
import { callGemini, parseDescribeResult } from './gemini';

export interface ProcessContext {
  request: KladosRequest;
  client: ArkeClient;
  logger: KladosLogger;
  sql: SqlStorage;
  env: Env;
}

export interface ProcessResult {
  outputs?: Output[];
  reschedule?: boolean;
}

/**
 * Parse configuration from request input
 */
function parseConfig(input: Record<string, unknown> | undefined): Required<DescribeConfig> {
  const props = input || {};

  return {
    update_label: (props.update_label as boolean) ?? false,
    max_relationships: (props.max_relationships as number) ?? 1000,
    predicates: (props.predicates as string[]) ?? [],
    batch_size: (props.batch_size as number) ?? 100,
    context_window_tokens: (props.context_window_tokens as number) ?? 128000,
    max_output_tokens: (props.max_output_tokens as number) ?? 8000,
    safety_margin: (props.safety_margin as number) ?? 0.8,
    style: (props.style as 'concise' | 'detailed' | 'academic' | 'casual') ?? 'detailed',
    custom_instructions: (props.custom_instructions as string) ?? '',
    focus: (props.focus as string) ?? '',
  };
}

/**
 * Update entity with generated description
 */
async function updateEntity(
  client: ArkeClient,
  entityId: string,
  result: { description: string; title?: string; label?: string },
  config: DescribeConfig
): Promise<void> {
  // Get current tip for CAS-safe update
  const { data: tip, error: tipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: entityId } },
  });

  if (tipError || !tip) {
    throw new Error(`Failed to get tip for ${entityId}: ${JSON.stringify(tipError)}`);
  }

  // Build update properties
  const properties: Record<string, unknown> = {
    description: result.description,
    description_generated_at: new Date().toISOString(),
    description_model: 'gemini-2.5-flash',
  };

  if (result.title) {
    properties.title = result.title;
  }

  if (config.update_label && result.label) {
    properties.label = result.label;
  }

  // Update entity
  const { error: updateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: entityId } },
    body: {
      expect_tip: tip.cid,
      properties,
    },
  });

  if (updateError) {
    throw new Error(`Failed to update entity: ${JSON.stringify(updateError)}`);
  }
}

/**
 * Process a description generation job
 *
 * State machine:
 * - FETCH_TARGET: Fetch target entity, extract relationship IDs
 * - FETCH_BATCH: Fetch next batch of related entities
 * - GENERATE: Build context, truncate, call LLM, update entity
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger, sql, env } = ctx;
  const config = parseConfig(request.input);

  // Initialize schema on first run
  initSchema(sql);

  const state = getJobState(sql);

  switch (state.phase) {
    case 'FETCH_TARGET': {
      logger.info('Fetching target entity');

      if (!request.target_entity) {
        throw new Error('No target_entity in request');
      }

      // Fetch target entity
      const target = await fetchTarget(client, request.target_entity);
      storeTarget(sql, target);

      // Extract relationship IDs
      const relIds = extractRelationshipIds(
        target.relationships,
        config.predicates.length > 0 ? config.predicates : undefined,
        config.max_relationships
      );

      setJobState(sql, {
        phase: relIds.length > 0 ? 'FETCH_BATCH' : 'GENERATE',
        nextBatchIndex: 0,
        totalRelationships: relIds.length,
        relationshipIds: relIds,
      });

      logger.info('Fetched target', {
        id: target.id,
        type: target.type,
        relationshipsToFetch: relIds.length,
      });

      return { reschedule: true };
    }

    case 'FETCH_BATCH': {
      const { nextBatchIndex, totalRelationships, relationshipIds } = state;

      // Get next batch
      const batch = getNextBatch(relationshipIds, nextBatchIndex, config.batch_size);

      if (batch.length > 0) {
        logger.info('Fetching batch', {
          batchIndex: nextBatchIndex,
          batchSize: batch.length,
          total: totalRelationships,
        });

        // Batch fetch entities
        const ids = batch.map((r) => r.id);
        const entities = await batchFetchEntities(client, ids);

        // Store each entity
        for (const rel of batch) {
          const entity = entities.get(rel.id);
          if (entity) {
            storeRelated(sql, rel.id, rel.predicate, entity);
          }
        }

        logger.info('Fetched batch', {
          fetched: entities.size,
          failed: batch.length - entities.size,
        });
      }

      const newIndex = nextBatchIndex + batch.length;

      if (hasMoreBatches(totalRelationships, newIndex)) {
        // More batches to fetch
        setJobState(sql, { nextBatchIndex: newIndex });
        logger.info('Progress', {
          fetched: newIndex,
          total: totalRelationships,
        });
        return { reschedule: true };
      }

      // All fetched, move to generation
      setJobState(sql, { phase: 'GENERATE' });
      logger.info('All entities fetched, generating description');
      return { reschedule: true };
    }

    case 'GENERATE': {
      // Read all entities from SQL
      const context = readAllEntities(sql);

      logger.info('Building context', {
        targetId: context.target.id,
        relatedCount: context.related.length,
      });

      // Calculate token budget
      const systemPromptTokens = estimateSystemPromptTokens(config);
      const truncationConfig = {
        contextWindowTokens: config.context_window_tokens!,
        maxOutputTokens: config.max_output_tokens!,
        systemPromptTokens,
        safetyMargin: config.safety_margin!,
      };

      // Apply progressive tax truncation
      const { context: truncatedContext, stats } = applyProgressiveTax(
        context,
        truncationConfig
      );

      if (stats.truncated) {
        logger.info('Applied truncation', {
          beforeTokens: stats.beforeTokens,
          afterTokens: stats.afterTokens,
          fieldsTruncated: stats.fieldsTruncated,
        });
      }

      // Build prompts
      const systemPrompt = buildSystemPrompt(config);
      const userPrompt = buildUserPrompt(truncatedContext, config);

      logger.info('Calling Gemini', {
        systemPromptLength: systemPrompt.length,
        userPromptLength: userPrompt.length,
      });

      // Call Gemini
      const geminiResponse = await callGemini(env.GEMINI_API_KEY, systemPrompt, userPrompt);

      logger.info('Gemini response', {
        tokens: geminiResponse.tokens,
        cost: `$${geminiResponse.cost_usd.toFixed(4)}`,
      });

      // Parse result
      const result = parseDescribeResult(geminiResponse.content);

      // Update entity
      await updateEntity(client, request.target_entity!, result, config);

      logger.success('Description generated', {
        descriptionLength: result.description.length,
        hasTitle: !!result.title,
        hasLabel: !!result.label,
      });

      // Cleanup and return
      clearState(sql);
      return { outputs: [request.target_entity!] };
    }

    default:
      throw new Error(`Unknown phase: ${state.phase}`);
  }
}
