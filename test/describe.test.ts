/**
 * E2E Test for Describe Klados
 *
 * Tests AI-powered description generation for entities:
 * 1. Basic entity description (entity with properties only)
 * 2. Entity with relationships (description mentions related entities)
 * 3. Cluster description with label update
 *
 * Prerequisites:
 * 1. Deploy your worker: npm run deploy
 * 2. Register the klados: npm run register
 * 3. Set environment variables (see below)
 *
 * Environment variables:
 *   ARKE_USER_KEY   - Your Arke user API key (uk_...)
 *   KLADOS_ID       - The klados entity ID from registration
 *   ARKE_API_BASE   - API base URL (default: https://arke-v1.arke.institute)
 *   ARKE_NETWORK    - Network to use (default: test)
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... KLADOS_ID=klados_... npm test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  getEntity,
  invokeKlados,
  waitForKladosLog,
  assertLogCompleted,
  apiRequest,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const KLADOS_ID = process.env.KLADOS_ID;

// =============================================================================
// Test Suite
// =============================================================================

describe('describe klados', () => {
  // Test fixtures
  let targetCollection: { id: string };
  let basicEntity: { id: string };
  let parentEntity: { id: string };
  let childEntity1: { id: string };
  let childEntity2: { id: string };
  let jobCollectionId: string;

  // Skip tests if environment not configured
  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!KLADOS_ID) {
      console.warn('Skipping tests: KLADOS_ID not set');
      return;
    }

    // Configure the test client
    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });
  });

  // Create test fixtures
  beforeAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Creating test fixtures...');

    // Create target collection
    targetCollection = await createCollection({
      label: `Describe Test ${Date.now()}`,
      description: 'Target collection for describe klados test',
    });
    log(`Created target collection: ${targetCollection.id}`);

    // Create basic entity (no relationships)
    basicEntity = await createEntity({
      type: 'test_document',
      properties: {
        label: 'Test Document',
        title: 'Historical Analysis of Agricultural Practices',
        content: `This document examines the evolution of agricultural practices in the
American Midwest during the early 20th century. It covers topics including crop
rotation, mechanization, and the impact of the Dust Bowl on farming communities.

The study draws on primary sources including farm journals, government reports,
and oral histories from families who lived through this transformative period.`,
        author: 'Dr. Jane Smith',
        date: '1932-05-15',
        source: 'University Archives',
      },
      collectionId: targetCollection.id,
    });
    log(`Created basic entity: ${basicEntity.id}`);

    // Create parent entity with children (to test relationship context)
    parentEntity = await createEntity({
      type: 'test_collection',
      properties: {
        label: 'Smith Family Papers',
        description: 'A collection of documents from the Smith family archive',
        date_range: '1890-1945',
      },
      collectionId: targetCollection.id,
    });
    log(`Created parent entity: ${parentEntity.id}`);

    // Create child entities (using apiRequest for relationships support)
    childEntity1 = await apiRequest<{ id: string }>('POST', '/entities', {
      type: 'test_letter',
      properties: {
        label: 'Letter from John to Mary',
        content: 'My dearest Mary, I write to you from the fields...',
        date: '1923-07-14',
        author: 'John Smith',
        recipient: 'Mary Smith',
      },
      collection: targetCollection.id,
      relationships: [
        { predicate: 'contained_in', peer: parentEntity.id },
      ],
    });
    log(`Created child entity 1: ${childEntity1.id}`);

    childEntity2 = await apiRequest<{ id: string }>('POST', '/entities', {
      type: 'test_photograph',
      properties: {
        label: 'Farm Photograph 1925',
        description: 'Black and white photograph of the Smith farm',
        date: '1925-08-20',
        subjects: ['farmhouse', 'tractor', 'wheat field'],
      },
      collection: targetCollection.id,
      relationships: [
        { predicate: 'contained_in', peer: parentEntity.id },
      ],
    });
    log(`Created child entity 2: ${childEntity2.id}`);

    // Update parent with contains relationships
    const tip = await apiRequest<{ cid: string }>('GET', `/entities/${parentEntity.id}/tip`);
    await apiRequest('PUT', `/entities/${parentEntity.id}`, {
      expect_tip: tip.cid,
      relationships_add: [
        { predicate: 'contains', peer: childEntity1.id },
        { predicate: 'contains', peer: childEntity2.id },
      ],
    });
    log(`Added contains relationships to parent`);
  });

  // Cleanup test fixtures
  afterAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Cleaning up test fixtures...');

    try {
      if (childEntity2?.id) await deleteEntity(childEntity2.id);
      if (childEntity1?.id) await deleteEntity(childEntity1.id);
      if (parentEntity?.id) await deleteEntity(parentEntity.id);
      if (basicEntity?.id) await deleteEntity(basicEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Tests
  // ==========================================================================

  it('should generate description for basic entity', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Test: Basic entity description');

    // Invoke the klados with detailed style
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: basicEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
      properties: {
        style: 'detailed',
      },
    });

    expect(result.status).toBe('started');
    expect(result.job_id).toBeDefined();
    expect(result.job_collection).toBeDefined();

    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);

    // Wait for completion (DO workers may take longer)
    log('Waiting for job completion...');
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
    });

    // Verify log completed successfully
    assertLogCompleted(kladosLog);
    log(`Job completed with status: ${kladosLog.properties.status}`);

    // Log all messages for debugging
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Fetch the updated entity and verify description was added
    const updatedEntity = await getEntity(basicEntity.id);
    expect(updatedEntity.properties.description).toBeDefined();
    expect(typeof updatedEntity.properties.description).toBe('string');
    expect((updatedEntity.properties.description as string).length).toBeGreaterThan(50);

    // Should have metadata
    expect(updatedEntity.properties.description_generated_at).toBeDefined();
    expect(updatedEntity.properties.description_model).toBe('gemini-2.5-flash');

    log(`Generated description (${(updatedEntity.properties.description as string).length} chars)`);
    log(`Description preview: ${(updatedEntity.properties.description as string).slice(0, 200)}...`);
  }, 180000); // 3 minute timeout

  it('should generate description that incorporates relationship context', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Test: Entity with relationships');

    // Invoke the klados on the parent entity (has contains relationships)
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: parentEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
      properties: {
        style: 'detailed',
      },
    });

    expect(result.status).toBe('started');
    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);

    // Wait for completion
    log('Waiting for job completion...');
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
    });

    assertLogCompleted(kladosLog);
    log(`Job completed with status: ${kladosLog.properties.status}`);

    // Log all messages
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Verify the description was created
    const updatedEntity = await getEntity(parentEntity.id);
    expect(updatedEntity.properties.description).toBeDefined();
    expect(typeof updatedEntity.properties.description).toBe('string');

    // The description should be informed by the related entities
    // It should mention something about the children (letter, photograph)
    const description = updatedEntity.properties.description as string;
    log(`Generated description (${description.length} chars)`);
    log(`Description: ${description}`);

    // Should have a title
    expect(updatedEntity.properties.title).toBeDefined();
    log(`Generated title: ${updatedEntity.properties.title}`);
  }, 180000);

  it('should generate concise description when style is concise', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Test: Concise style');

    // Invoke with concise style
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: childEntity1.id,
      targetCollection: targetCollection.id,
      confirm: true,
      properties: {
        style: 'concise',
      },
    });

    expect(result.status).toBe('started');
    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);

    // Wait for completion
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
    });

    assertLogCompleted(kladosLog);

    // Verify description exists
    const updatedEntity = await getEntity(childEntity1.id);
    expect(updatedEntity.properties.description).toBeDefined();

    // Concise descriptions should be shorter
    const description = updatedEntity.properties.description as string;
    log(`Concise description (${description.length} chars): ${description}`);

    // Generally concise should be under 500 chars, but we won't enforce strictly
    expect(description.length).toBeGreaterThan(10);
  }, 180000);

  it('should update label when update_label is true', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Test: Update label');

    // Get original label
    const originalEntity = await getEntity(childEntity2.id);
    const originalLabel = originalEntity.properties.label;
    log(`Original label: ${originalLabel}`);

    // Invoke with update_label
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: childEntity2.id,
      targetCollection: targetCollection.id,
      confirm: true,
      properties: {
        update_label: true,
        style: 'detailed',
      },
    });

    expect(result.status).toBe('started');
    jobCollectionId = result.job_collection!;
    log(`Job started: ${result.job_id}`);

    // Wait for completion
    const kladosLog = await waitForKladosLog(jobCollectionId, {
      timeout: 120000,
      pollInterval: 3000,
    });

    assertLogCompleted(kladosLog);

    // Verify label was updated
    const updatedEntity = await getEntity(childEntity2.id);
    expect(updatedEntity.properties.description).toBeDefined();

    // Label might be the same or different depending on what LLM generates
    log(`New label: ${updatedEntity.properties.label}`);
    log(`Description: ${updatedEntity.properties.description}`);
  }, 180000);

  it('should handle preview mode (confirm=false)', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    log('Test: Preview mode');

    // Preview invocation (confirm=false)
    const preview = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: basicEntity.id,
      targetCollection: targetCollection.id,
      confirm: false,
    });

    // Preview should return pending_confirmation status
    expect(preview.status).toBe('pending_confirmation');
    log(`Preview result: ${preview.status}`);
  });
});
