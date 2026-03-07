/**
 * Gemini API client with retry logic
 *
 * Adapted from kg-extractor for description generation.
 * Uses temperature 0.7 for balanced creativity/consistency.
 * Uses responseSchema for guaranteed valid JSON output.
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
 * JSON Schema for the describe result.
 * When provided via responseSchema, Gemini guarantees valid JSON output.
 */
function getDescribeSchema(includeLabel: boolean) {
  const properties: Record<string, unknown> = {
    title: {
      type: 'STRING',
      description: 'Human-readable title for the entity',
    },
    description: {
      type: 'STRING',
      description: 'The generated description (markdown supported)',
    },
  };

  const required = ['title', 'description'];

  if (includeLabel) {
    properties.label = {
      type: 'STRING',
      description: 'Concise label for the entity (2-5 words)',
    };
    required.push('label');
  }

  return {
    type: 'OBJECT',
    properties,
    required,
  };
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
 * Parse the JSON response from Gemini into a DescribeResult.
 * With responseSchema, JSON.parse should always succeed.
 * Fallback regex extraction kept as a safety net.
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
    // This should not happen with responseSchema, but log if it does
    console.error('[Gemini] Unexpected JSON parse failure with responseSchema:', e);
    console.error('[Gemini] Raw content:', content.slice(0, 500));

    // Last-resort regex extraction
    const descMatch = content.match(/"description"\s*:\s*"((?:[^"\\]|\\.)*)"/);
    const titleMatch = content.match(/"title"\s*:\s*"((?:[^"\\]|\\.)*)"/);

    if (descMatch) {
      console.warn('[Gemini] Recovered fields via regex extraction');
      const unescape = (s: string) => s.replace(/\\"/g, '"').replace(/\\n/g, '\n').replace(/\\\\/g, '\\');
      return {
        description: unescape(descMatch[1]),
        title: titleMatch ? unescape(titleMatch[1]) : undefined,
      };
    }

    return { description: content };
  }
}

/**
 * Shared retry loop for Gemini API calls (HTTP-level retries).
 * Handles rate limits, server errors, and timeouts.
 */
async function callGeminiWithRetry(
  url: string,
  body: unknown,
  label: string
): Promise<GeminiResponse> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`[Gemini] ${label} attempt ${attempt + 1}/${MAX_RETRIES + 1}...`);

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
 * Call Gemini API with structured JSON output (text-only).
 * Uses responseSchema to guarantee valid JSON.
 */
export async function callGemini(
  apiKey: string,
  systemPrompt: string,
  userPrompt: string,
  options?: { includeLabel?: boolean }
): Promise<{ response: GeminiResponse; result: DescribeResult }> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent?key=${apiKey}`;

  const body = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
    generationConfig: {
      maxOutputTokens: 8192,
      temperature: 0.7,
      responseMimeType: 'application/json',
      responseSchema: getDescribeSchema(options?.includeLabel ?? false),
    },
  };

  const response = await callGeminiWithRetry(url, body, 'Text');
  const result = parseDescribeResult(response.content);

  return { response, result };
}

/**
 * Stream content from Arke → Gemini Files API using FixedLengthStream
 *
 * This enables true streaming without buffering the entire file in memory.
 * The FixedLengthStream allows us to set Content-Length while still streaming.
 *
 * @param arkeKey - Arke API key for fetching content
 * @param geminiKey - Gemini API key for uploading
 * @param arkeBase - Arke API base URL
 * @param entityId - Entity ID to fetch content from
 * @param contentKey - Content key within the entity
 * @param contentType - MIME type of the content
 * @param knownSize - Known size in bytes (from entity metadata)
 */
export async function streamToGeminiFiles(
  arkeKey: string,
  geminiKey: string,
  arkeBase: string,
  entityId: string,
  contentKey: string,
  contentType: string,
  knownSize: number
): Promise<{ fileUri: string; uploadedBytes: number }> {
  // 1. Start resumable upload session with Gemini
  const startResponse = await fetch(
    `https://generativelanguage.googleapis.com/upload/v1beta/files?key=${geminiKey}`,
    {
      method: 'POST',
      headers: {
        'X-Goog-Upload-Protocol': 'resumable',
        'X-Goog-Upload-Command': 'start',
        'X-Goog-Upload-Header-Content-Length': String(knownSize),
        'X-Goog-Upload-Header-Content-Type': contentType,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        file: { displayName: contentKey },
      }),
    }
  );

  if (!startResponse.ok) {
    const err = await startResponse.text();
    throw new Error(`Gemini upload start failed: ${startResponse.status} - ${err}`);
  }

  // Get the upload URL from response header
  const uploadUrl = startResponse.headers.get('X-Goog-Upload-URL');
  if (!uploadUrl) {
    throw new Error('No upload URL returned from Gemini');
  }

  console.log(`[Gemini] Got upload URL, fetching content from Arke...`);

  // 2. Fetch content stream from Arke
  const arkeResponse = await fetch(
    `${arkeBase}/entities/${entityId}/content?key=${encodeURIComponent(contentKey)}`,
    { headers: { Authorization: `ApiKey ${arkeKey}` } }
  );

  if (!arkeResponse.ok) {
    const err = await arkeResponse.text();
    throw new Error(`Arke content fetch failed: ${arkeResponse.status} - ${err}`);
  }

  if (!arkeResponse.body) {
    throw new Error('Arke response has no body');
  }

  console.log(`[Gemini] Streaming ${knownSize} bytes to Gemini...`);

  // 3. Create FixedLengthStream for Content-Length with streaming
  const { readable, writable } = new FixedLengthStream(knownSize);

  // 4. Pipe Arke response → FixedLengthStream (runs in background)
  const pipePromise = arkeResponse.body.pipeTo(writable).catch((err) => {
    console.error('[Gemini] Pipe error:', err);
    throw err;
  });

  // 5. Upload to Gemini using the FixedLengthStream's readable side
  const uploadResponse = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      'Content-Length': String(knownSize),
      'X-Goog-Upload-Offset': '0',
      'X-Goog-Upload-Command': 'upload, finalize',
    },
    body: readable,
  });

  // Wait for pipe to complete
  await pipePromise;

  if (!uploadResponse.ok) {
    const err = await uploadResponse.text();
    throw new Error(`Gemini upload failed: ${uploadResponse.status} - ${err}`);
  }

  // 6. Parse response to get file URI
  const result = (await uploadResponse.json()) as { file: { uri: string; name: string } };

  console.log(`[Gemini] Upload complete: ${result.file.uri}`);

  return {
    fileUri: result.file.uri,
    uploadedBytes: knownSize,
  };
}

/**
 * Call Gemini with multimodal content (text + files).
 * Uses responseSchema to guarantee valid JSON.
 */
export async function callGeminiMultimodal(
  apiKey: string,
  systemPrompt: string,
  userPrompt: string,
  fileRefs: Array<{ fileUri: string; mimeType: string }>,
  options?: { includeLabel?: boolean }
): Promise<{ response: GeminiResponse; result: DescribeResult }> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent?key=${apiKey}`;

  // Build parts array: files first, then text
  const parts: Array<{ file_data?: { file_uri: string; mime_type: string }; text?: string }> = [];

  for (const ref of fileRefs) {
    parts.push({
      file_data: {
        file_uri: ref.fileUri,
        mime_type: ref.mimeType,
      },
    });
  }

  parts.push({ text: userPrompt });

  const body = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: [{ role: 'user', parts }],
    generationConfig: {
      maxOutputTokens: 8192,
      temperature: 0.7,
      responseMimeType: 'application/json',
      responseSchema: getDescribeSchema(options?.includeLabel ?? false),
    },
  };

  const response = await callGeminiWithRetry(url, body, `Multimodal (${fileRefs.length} files)`);
  const result = parseDescribeResult(response.content);

  return { response, result };
}
