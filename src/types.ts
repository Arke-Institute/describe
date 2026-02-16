/**
 * Type definitions for the describe worker
 */

/**
 * Worker environment bindings
 */
export interface Env {
  AGENT_ID: string;
  AGENT_VERSION: string;
  ARKE_AGENT_KEY: string;
  GEMINI_API_KEY: string;
  VERIFICATION_TOKEN?: string;
  ARKE_VERIFY_AGENT_ID?: string;
  KLADOS_JOB: DurableObjectNamespace;
}

/**
 * Configuration for description generation
 * Passed via request.properties
 */
export interface DescribeConfig {
  // Output control
  update_label?: boolean; // Also update label property (default: false)

  // Context gathering
  max_relationships?: number; // Max relationships to fetch (default: 1000)
  predicates?: string[]; // Filter to specific predicates (default: all)
  batch_size?: number; // Entities per batch-get call (default: 100)

  // Token budget
  context_window_tokens?: number; // Model context window (default: 128000)
  max_output_tokens?: number; // Reserved for output (default: 8000)
  safety_margin?: number; // Buffer ratio (default: 0.8 = 20% margin)

  // Prompt customization
  style?: 'concise' | 'detailed' | 'academic' | 'casual';
  custom_instructions?: string; // Additional LLM instructions
  focus?: string; // What aspect to emphasize
}

/**
 * Default configuration values
 */
export const DEFAULT_CONFIG: Required<DescribeConfig> = {
  update_label: false,
  max_relationships: 1000,
  predicates: [],
  batch_size: 100,
  context_window_tokens: 128000,
  max_output_tokens: 8000,
  safety_margin: 0.8,
  style: 'detailed',
  custom_instructions: '',
  focus: '',
};

/**
 * Entity manifest from the API
 */
export interface EntityManifest {
  id: string;
  type: string;
  cid?: string;
  properties: Record<string, unknown>;
  relationships?: Array<{
    predicate: string;
    peer: string;
    peer_label?: string;
    peer_type?: string;
  }>;
}

/**
 * Description context - target entity with related entities
 */
export interface DescriptionContext {
  target: EntityManifest;
  related: Array<{
    predicate: string;
    manifest: EntityManifest;
  }>;
}

/**
 * Result from the LLM
 */
export interface DescribeResult {
  description: string;
  title?: string;
  label?: string;
}

/**
 * Job phases for DO state machine
 */
export type JobPhase = 'FETCH_TARGET' | 'FETCH_BATCH' | 'GENERATE' | 'DONE';

/**
 * Relationship ID with predicate for batch fetching
 */
export interface RelationshipRef {
  id: string;
  predicate: string;
}

/**
 * Job state stored in SQL
 */
export interface JobState {
  phase: JobPhase;
  nextBatchIndex: number;
  totalRelationships: number;
  relationshipIds: RelationshipRef[];
}

/**
 * Truncation statistics
 */
export interface TruncationStats {
  truncated: boolean;
  beforeTokens: number;
  afterTokens: number;
  fieldsProtected: number;
  fieldsTruncated: number;
}

/**
 * Truncation configuration
 */
export interface TruncationConfig {
  contextWindowTokens: number;
  maxOutputTokens: number;
  systemPromptTokens: number;
  safetyMargin: number;
}
