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
import type { Env, DescribeConfig, ContentMetadata } from './types';
import {
  initSchema,
  storeTarget,
  storeRelated,
  readAllEntities,
  getJobState,
  setJobState,
  clearState,
  storeContentRef,
  readContentRefs,
  readSuccessfulContentRefs,
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
import { callGemini, streamToGeminiFiles, callGeminiMultimodal } from './gemini';

export interface ProcessContext {
  request: KladosRequest;
  client: ArkeClient;
  logger: KladosLogger;
  sql: SqlStorage;
  env: Env;
  /** Network-specific auth token (from getKladosConfig) */
  authToken: string;
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
    include_content: (props.include_content as boolean) ?? true,
    content_keys: (props.content_keys as string[]) ?? [],
    max_content_size: (props.max_content_size as number) ?? 50 * 1024 * 1024, // 50MB
    context_window_tokens: (props.context_window_tokens as number) ?? 128000,
    max_output_tokens: (props.max_output_tokens as number) ?? 8000,
    safety_margin: (props.safety_margin as number) ?? 0.8,
    style: (props.style as 'concise' | 'detailed' | 'academic' | 'casual') ?? 'detailed',
    custom_instructions: (props.custom_instructions as string) ?? '',
    focus: (props.focus as string) ?? '',
  };
}

/**
 * Get max size limit based on content type
 */
function getMaxSizeForType(contentType: string, configMax: number): number {
  // PDF has stricter limit in Gemini
  if (contentType === 'application/pdf') {
    return Math.min(configMax, 50 * 1024 * 1024); // 50MB max for PDFs
  }
  // Images can be up to 100MB
  if (contentType.startsWith('image/')) {
    return Math.min(configMax, 100 * 1024 * 1024); // 100MB max for images
  }
  return configMax;
}

/**
 * Format bytes as human-readable string
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

const CAS_MAX_RETRIES = 5;
const CAS_BASE_DELAY_MS = 100;

/**
 * Update entity with generated description (with CAS retry)
 */
async function updateEntity(
  client: ArkeClient,
  entityId: string,
  result: { description: string; title?: string; label?: string },
  config: DescribeConfig
): Promise<void> {
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

  // CAS retry loop
  for (let attempt = 0; attempt < CAS_MAX_RETRIES; attempt++) {
    // Get current tip for CAS-safe update
    const { data: tip, error: tipError } = await client.api.GET('/entities/{id}/tip', {
      params: { path: { id: entityId } },
    });

    if (tipError || !tip) {
      throw new Error(`Failed to get tip for ${entityId}: ${JSON.stringify(tipError)}`);
    }

    // Update entity
    const { error: updateError } = await client.api.PUT('/entities/{id}', {
      params: { path: { id: entityId } },
      body: {
        expect_tip: tip.cid,
        properties,
      },
    });

    if (!updateError) {
      return; // Success
    }

    // Check if CAS failure (retryable)
    const errorStr = JSON.stringify(updateError);
    if (errorStr.includes('CAS failure') && attempt < CAS_MAX_RETRIES - 1) {
      const delay = CAS_BASE_DELAY_MS * Math.pow(2, attempt);
      console.log(`[describe] CAS conflict on attempt ${attempt + 1}, retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      continue;
    }

    throw new Error(`Failed to update entity: ${errorStr}`);
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
  const { request, client, logger, sql, env, authToken } = ctx;
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

      // Extract content keys to upload
      // content property must be an object map of ContentMetadata, not a plain string
      let contentKeys: string[] = [];
      if (config.include_content && target.properties.content && typeof target.properties.content === 'object') {
        const contentMap = target.properties.content as Record<string, ContentMetadata>;
        // Validate entries have content_type (filters out non-ContentMetadata values)
        const allKeys = Object.keys(contentMap).filter(k => contentMap[k]?.content_type);
        // Filter to requested keys if specified, otherwise use all
        contentKeys = config.content_keys.length > 0
          ? allKeys.filter(k => config.content_keys.includes(k))
          : allKeys;
      }

      // Determine next phase
      let nextPhase: 'UPLOAD_CONTENT' | 'FETCH_BATCH' | 'GENERATE';
      if (contentKeys.length > 0) {
        nextPhase = 'UPLOAD_CONTENT';
      } else if (relIds.length > 0) {
        nextPhase = 'FETCH_BATCH';
      } else {
        nextPhase = 'GENERATE';
      }

      setJobState(sql, {
        phase: nextPhase,
        nextBatchIndex: 0,
        totalRelationships: relIds.length,
        relationshipIds: relIds,
        contentKeys,
        contentIndex: 0,
      });

      logger.info('Fetched target', {
        id: target.id,
        type: target.type,
        relationshipsToFetch: relIds.length,
        contentKeysToUpload: contentKeys.length,
      });

      return { reschedule: true };
    }

    case 'UPLOAD_CONTENT': {
      const { contentKeys, contentIndex, totalRelationships, relationshipIds } = state;
      const key = contentKeys[contentIndex];

      // Read target to get content metadata
      const context = readAllEntities(sql);
      const contentMap = context.target.properties.content as Record<string, ContentMetadata> | undefined;
      const contentMeta = contentMap?.[key];

      if (!contentMeta) {
        logger.info('Content not found', { key });
        storeContentRef(sql, { key, status: 'not_found', reason: 'No content at this key' });
      } else {
        const maxSize = getMaxSizeForType(contentMeta.content_type, config.max_content_size);

        if (contentMeta.size > maxSize) {
          logger.info('Content too large, skipping', {
            key,
            size: formatBytes(contentMeta.size),
            limit: formatBytes(maxSize),
          });
          storeContentRef(sql, {
            key,
            status: 'too_large',
            contentType: contentMeta.content_type,
            size: contentMeta.size,
            reason: `${formatBytes(contentMeta.size)} exceeds ${formatBytes(maxSize)} limit`,
          });
        } else {
          // Stream upload to Gemini Files API
          logger.info('Uploading content', {
            key,
            size: formatBytes(contentMeta.size),
            type: contentMeta.content_type,
          });

          try {
            const arkeBase = 'https://arke-v1.arke.institute'; // TODO: get from config
            const result = await streamToGeminiFiles(
              authToken,
              env.GEMINI_API_KEY,
              arkeBase,
              request.target_entity!,
              key,
              contentMeta.content_type,
              contentMeta.size
            );

            storeContentRef(sql, {
              key,
              status: 'success',
              contentType: contentMeta.content_type,
              size: contentMeta.size,
              fileUri: result.fileUri,
            });

            logger.info('Content uploaded', { key, fileUri: result.fileUri });
          } catch (error) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            logger.error('Content upload failed', { key, error: errorMsg });
            storeContentRef(sql, {
              key,
              status: 'error',
              contentType: contentMeta.content_type,
              size: contentMeta.size,
              reason: errorMsg,
            });
          }
        }
      }

      // Move to next content or next phase
      const newIndex = contentIndex + 1;
      if (newIndex < contentKeys.length) {
        setJobState(sql, { contentIndex: newIndex });
        logger.info('Content progress', { uploaded: newIndex, total: contentKeys.length });
        return { reschedule: true };
      }

      // All content processed, move to next phase
      const nextPhase = totalRelationships > 0 ? 'FETCH_BATCH' : 'GENERATE';
      setJobState(sql, { phase: nextPhase });
      logger.info('Content upload complete, moving to', { phase: nextPhase });
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

      // Read content refs (uploaded to Gemini Files API)
      const allContentRefs = readContentRefs(sql);
      const successfulContent = readSuccessfulContentRefs(sql);
      const skippedContent = allContentRefs.filter(r => r.status !== 'success');

      logger.info('Building context', {
        targetId: context.target.id,
        relatedCount: context.related.length,
        contentFiles: successfulContent.length,
        contentSkipped: skippedContent.length,
      });

      // Log skipped content
      for (const skipped of skippedContent) {
        logger.info('Content skipped', {
          key: skipped.key,
          status: skipped.status,
          reason: skipped.reason,
        });
      }

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
      let userPrompt = buildUserPrompt(truncatedContext, config);

      // Add content info to prompt if we have files
      if (successfulContent.length > 0) {
        const contentInfo = successfulContent
          .map(c => `- **${c.key}**: ${c.contentType} (${formatBytes(c.size!)})`)
          .join('\n');
        userPrompt = `## ATTACHED CONTENT\nThe following files are attached and included for analysis:\n${contentInfo}\n\n${userPrompt}`;
      }

      // Add skipped content note if any
      if (skippedContent.length > 0) {
        const skippedInfo = skippedContent
          .map(c => `- **${c.key}**: ${c.reason}`)
          .join('\n');
        userPrompt += `\n\n## CONTENT NOT ANALYZED\nThe following content could not be included:\n${skippedInfo}`;
      }

      let geminiResponse;
      let result;
      const schemaOptions = { includeLabel: !!config.update_label };

      if (successfulContent.length > 0) {
        // Use multimodal API with file references
        const fileRefs = successfulContent.map(c => ({
          fileUri: c.fileUri!,
          mimeType: c.contentType!,
        }));

        logger.info('Calling Gemini (multimodal)', {
          systemPromptLength: systemPrompt.length,
          userPromptLength: userPrompt.length,
          fileCount: fileRefs.length,
        });

        const jsonResult = await callGeminiMultimodal(
          env.GEMINI_API_KEY,
          systemPrompt,
          userPrompt,
          fileRefs,
          schemaOptions
        );
        geminiResponse = jsonResult.response;
        result = jsonResult.result;
      } else {
        // Text-only API
        logger.info('Calling Gemini (text-only)', {
          systemPromptLength: systemPrompt.length,
          userPromptLength: userPrompt.length,
        });

        const jsonResult = await callGemini(
          env.GEMINI_API_KEY,
          systemPrompt,
          userPrompt,
          schemaOptions
        );
        geminiResponse = jsonResult.response;
        result = jsonResult.result;
      }

      logger.info('Gemini response', {
        tokens: geminiResponse.tokens,
        cost: `$${geminiResponse.cost_usd.toFixed(4)}`,
      });

      // Update entity
      await updateEntity(client, request.target_entity!, result, config);

      logger.success('Description generated', {
        descriptionLength: result.description.length,
        hasTitle: !!result.title,
        hasLabel: !!result.label,
        multimodal: successfulContent.length > 0,
      });

      // Cleanup and return
      clearState(sql);
      return { outputs: [request.target_entity!] };
    }

    default:
      throw new Error(`Unknown phase: ${state.phase}`);
  }
}
