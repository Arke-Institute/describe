/**
 * Gemini API client with retry logic
 *
 * Adapted from kg-extractor for description generation.
 * Uses temperature 0.7 for balanced creativity/consistency.
 */

import type { DescribeResult } from './types';

const MODEL = 'gemini-2.5-flash';
const MAX_RETRIES = 3;
const REQUEST_TIMEOUT_MS = 120000; // 2 minutes
const BASE_DELAY_MS = 15000;
const MAX_DELAY_MS = 120000;

// Cost tracking (for logging)
const INPUT_COST_PER_MILLION = 0.10;
const OUTPUT_COST_PER_MILLION = 0.40;

/**
 * Response from Gemini API call
 */
export interface GeminiResponse {
  content: string;
  tokens: number;
  prompt_tokens: number;
  completion_tokens: number;
  cost_usd: number;
}

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Parse the Gemini API response
 */
function parseGeminiResponse(data: unknown): GeminiResponse {
  const response = data as {
    candidates?: Array<{
      content?: {
        parts?: Array<{ text?: string; thought?: boolean }>;
      };
      finishReason?: string;
    }>;
    usageMetadata?: {
      promptTokenCount?: number;
      candidatesTokenCount?: number;
      totalTokenCount?: number;
    };
  };

  // Extract text content, filtering out "thought" parts
  const parts = response.candidates?.[0]?.content?.parts || [];
  const content = parts
    .filter((p) => !p.thought && p.text)
    .map((p) => p.text)
    .join('');

  // Extract usage metadata
  const usage = response.usageMetadata || {};
  const promptTokens = usage.promptTokenCount || 0;
  const completionTokens = usage.candidatesTokenCount || 0;
  const totalTokens = usage.totalTokenCount || promptTokens + completionTokens;

  // Calculate cost
  const cost =
    (promptTokens / 1_000_000) * INPUT_COST_PER_MILLION +
    (completionTokens / 1_000_000) * OUTPUT_COST_PER_MILLION;

  return {
    content,
    tokens: totalTokens,
    prompt_tokens: promptTokens,
    completion_tokens: completionTokens,
    cost_usd: cost,
  };
}

/**
 * Call Gemini API with JSON mode and retry logic
 */
export async function callGemini(
  apiKey: string,
  systemPrompt: string,
  userPrompt: string
): Promise<GeminiResponse> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent?key=${apiKey}`;

  const body = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
    generationConfig: {
      maxOutputTokens: 8192, // Enough for descriptions
      temperature: 0.7,
      responseMimeType: 'application/json',
    },
  };

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`[Gemini] Attempt ${attempt + 1}/${MAX_RETRIES + 1}...`);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      // Handle rate limits and server errors with retry
      if (response.status === 429 || response.status >= 500) {
        const errorText = await response.text();
        console.error(`[Gemini] Error ${response.status}: ${errorText}`);

        if (attempt < MAX_RETRIES) {
          const delay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
          console.log(`[Gemini] Retrying in ${delay}ms...`);
          await sleep(delay);
          continue;
        }
        throw new Error(`Gemini API error (${response.status}): ${errorText}`);
      }

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Gemini API error (${response.status}): ${errorText}`);
      }

      const data = await response.json();

      // Log finish reason for debugging
      const finishReason = (data as { candidates?: Array<{ finishReason?: string }> })
        .candidates?.[0]?.finishReason;
      console.log(`[Gemini] Finish reason: ${finishReason}`);

      // Warn if output may be truncated
      if (finishReason === 'MAX_TOKENS') {
        console.warn('[Gemini] WARNING: Output truncated due to max tokens limit');
      }

      return parseGeminiResponse(data);
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      if (attempt < MAX_RETRIES) {
        const delay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
        console.log(`[Gemini] Error: ${lastError.message}, retrying in ${delay}ms...`);
        await sleep(delay);
      }
    }
  }

  throw lastError || new Error('Gemini request failed');
}

/**
 * Parse the JSON response from Gemini into a DescribeResult
 */
export function parseDescribeResult(content: string): DescribeResult {
  try {
    const parsed = JSON.parse(content);

    return {
      description: parsed.description || '',
      title: parsed.title,
      label: parsed.label,
    };
  } catch (e) {
    console.error('[Gemini] Failed to parse JSON response:', e);
    // Fallback: treat the whole content as description
    return {
      description: content,
    };
  }
}
