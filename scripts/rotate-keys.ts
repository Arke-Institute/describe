#!/usr/bin/env npx tsx
/**
 * Rotate klados API keys for dual-network deployment
 *
 * Creates new API keys for both test and main network kladoi,
 * updates Cloudflare Worker secrets, and updates state files.
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... npx tsx scripts/rotate-keys.ts
 *
 * Requires:
 *   - ARKE_USER_KEY environment variable
 *   - wrangler authenticated to correct account
 *   - .klados-state.json and .klados-state.prod.json files
 */

import { execSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const API_BASE = 'https://arke-v1.arke.institute';

interface StateFile {
  schema_version: number;
  klados_id: string;
  collection_id: string;
  api_key_prefix: string;
  endpoint: string;
  endpoint_verified_at: string;
  config_hash: string;
  registered_at: string;
  updated_at: string;
}

interface KeyCreateResponse {
  id: string;
  key: string;
  prefix: string;
  created_at: string;
  expires_at: string;
  label: string | null;
}

async function createKey(kladosId: string, userKey: string): Promise<KeyCreateResponse> {
  const response = await fetch(`${API_BASE}/kladoi/${kladosId}/keys`, {
    method: 'POST',
    headers: {
      'Authorization': `ApiKey ${userKey}`,
      'Content-Type': 'application/json',
    },
    body: '{}',
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to create key for ${kladosId}: ${error}`);
  }

  return response.json();
}

function setWranglerSecret(name: string, value: string): void {
  console.log(`  Setting secret ${name}...`);
  execSync(`echo "${value}" | wrangler secret put ${name}`, {
    stdio: ['pipe', 'inherit', 'inherit'],
  });
}

function updateStateFile(path: string, newPrefix: string): void {
  const state: StateFile = JSON.parse(readFileSync(path, 'utf-8'));
  state.api_key_prefix = newPrefix;
  state.updated_at = new Date().toISOString();
  writeFileSync(path, JSON.stringify(state, null, 2) + '\n');
  console.log(`  Updated ${path}`);
}

async function main() {
  const userKey = process.env.ARKE_USER_KEY;
  if (!userKey) {
    console.error('Error: ARKE_USER_KEY environment variable required');
    process.exit(1);
  }

  const projectRoot = join(import.meta.dirname, '..');
  const testStatePath = join(projectRoot, '.klados-state.json');
  const mainStatePath = join(projectRoot, '.klados-state.prod.json');

  // Read state files
  const testState: StateFile = JSON.parse(readFileSync(testStatePath, 'utf-8'));
  const mainState: StateFile = JSON.parse(readFileSync(mainStatePath, 'utf-8'));

  console.log('Rotating klados API keys...\n');
  console.log(`Test klados: ${testState.klados_id}`);
  console.log(`Main klados: ${mainState.klados_id}\n`);

  // Create new keys
  console.log('Creating new API keys...');
  const testKey = await createKey(testState.klados_id, userKey);
  console.log(`  Test: ${testKey.prefix}... (expires ${testKey.expires_at})`);

  const mainKey = await createKey(mainState.klados_id, userKey);
  console.log(`  Main: ${mainKey.prefix}... (expires ${mainKey.expires_at})`);

  // Set Cloudflare secrets
  console.log('\nSetting Cloudflare Worker secrets...');
  setWranglerSecret('ARKE_AGENT_KEY_TEST', testKey.key);
  setWranglerSecret('ARKE_AGENT_KEY_MAIN', mainKey.key);

  // Update state files
  console.log('\nUpdating state files...');
  updateStateFile(testStatePath, testKey.prefix);
  updateStateFile(mainStatePath, mainKey.prefix);

  console.log('\nDone! New keys are active.');
  console.log('\nOptional: Revoke old keys via API:');
  console.log(`  DELETE ${API_BASE}/kladoi/${testState.klados_id}/keys/{old_prefix}`);
  console.log(`  DELETE ${API_BASE}/kladoi/${mainState.klados_id}/keys/{old_prefix}`);
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
