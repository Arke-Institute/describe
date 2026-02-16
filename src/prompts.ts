/**
 * Prompt templates and formatting for description generation
 */

import type { DescriptionContext, DescribeConfig } from './types';

/**
 * Style instruction presets
 */
const STYLE_INSTRUCTIONS: Record<string, string> = {
  concise: 'Be brief. 2-3 sentences maximum. Focus on the most important facts.',
  detailed: 'Be thorough. Include all relevant details, relationships, and context.',
  academic: 'Use formal academic language. Be precise and reference relationships explicitly.',
  casual: 'Use conversational language. Be engaging and accessible.',
};

/**
 * Build the system prompt with style instructions
 */
export function buildSystemPrompt(config: DescribeConfig): string {
  const styleInstruction = config.style
    ? STYLE_INSTRUCTIONS[config.style] || STYLE_INSTRUCTIONS.detailed
    : STYLE_INSTRUCTIONS.detailed;

  const customInstructions = config.custom_instructions
    ? `\n\nAdditional instructions: ${config.custom_instructions}`
    : '';

  return `You are an archivist writing clear, factual descriptions for entities in a knowledge graph.

Your descriptions should:
- Describe what the entity IS and what it CONTAINS
- Provide relevant context (dates, places, people, institutions)
- Synthesize information from related entities when relevant
- Be factual and objective - avoid speculation

ENTITY LINKS: You may reference related entities using this format: [Display Label](arke:<entity-id>)
IMPORTANT: Only create arke: links to entities that are ACTUALLY PROVIDED in the context below.
Do NOT invent or hallucinate entity IDs. If something is mentioned but not provided as an entity
(e.g., an author name in a property), just write it as plain text without a link.

Style: ${styleInstruction}${customInstructions}

Output valid JSON with these fields:
{
  "title": "Human-readable title for the entity",
  "description": "The generated description (markdown supported)"${config.update_label ? ',\n  "label": "Concise label for the entity (2-5 words)"' : ''}
}`;
}

/**
 * Format an entity manifest as JSON for the prompt
 */
function formatEntityManifest(entity: { id: string; type: string; properties: Record<string, unknown> }): string {
  // Include key fields but not the full manifest to save tokens
  const simplified = {
    id: entity.id,
    type: entity.type,
    ...entity.properties,
  };

  return JSON.stringify(simplified, null, 2);
}

/**
 * Build the user prompt with formatted context
 */
export function buildUserPrompt(
  context: DescriptionContext,
  config: DescribeConfig
): string {
  const lines: string[] = [];

  // Target entity section
  lines.push('## TARGET ENTITY');
  lines.push('');
  lines.push(formatEntityManifest(context.target));
  lines.push('');

  // Related entities section
  if (context.related.length > 0) {
    lines.push(`## RELATED ENTITIES (${context.related.length} total)`);
    lines.push('');

    for (const related of context.related) {
      const label = (related.manifest.properties.label as string) || related.manifest.id;
      lines.push(`### ${label}`);
      lines.push(`**Relationship:** Target \`${related.predicate}\` → this entity`);
      lines.push('');
      lines.push(formatEntityManifest(related.manifest));
      lines.push('');
    }
  }

  // Focus instruction if provided
  if (config.focus) {
    lines.push(`## FOCUS`);
    lines.push(`Please emphasize: ${config.focus}`);
    lines.push('');
  }

  return lines.join('\n');
}

/**
 * Estimate tokens in the system prompt (for budget calculation)
 */
export function estimateSystemPromptTokens(config: DescribeConfig): number {
  const prompt = buildSystemPrompt(config);
  return Math.ceil(prompt.length / 3); // Conservative estimate
}
