#!/usr/bin/env npx tsx
/**
 * Moby Dick Test - NO CLEANUP
 * Entities are kept for inspection
 */

import {
  configureTestClient,
  createCollection,
  getEntity,
  invokeKlados,
  waitForKladosLog,
  assertLogCompleted,
  apiRequest,
  log,
} from '@arke-institute/klados-testing';

const CHAPTER_1 = `Call me Ishmael. Some years ago—never mind how long precisely—having
little or no money in my purse, and nothing particular to interest me on shore, I thought
I would sail about a little and see the watery part of the world. It is a way I have of
driving off the spleen and regulating the circulation. Whenever I find myself growing grim
about the mouth; whenever it is a damp, drizzly November in my soul; whenever I find myself
involuntarily pausing before coffin warehouses, and bringing up the rear of every funeral
I meet; and especially whenever my hypos get such an upper hand of me, that it requires a
strong moral principle to prevent me from deliberately stepping into the street, and
methodically knocking people's hats off—then, I account it high time to get to sea as soon
as I can. This is my substitute for pistol and ball.`;

const CHAPTER_28 = `For several days after leaving Nantucket, nothing above hatches was seen
of Captain Ahab. The mates regularly relieved each other at the watches, and for aught that
could be seen to the contrary, they seemed to be the only commanders of the ship. There
seemed no sign of common bodily illness about him, nor of the recovery from any. He looked
like a man cut away from the stake, when the fire has overrunningly wasted all the limbs
without consuming them. His whole high, broad form, seemed made of solid bronze, and shaped
in an unalterable mould, like Cellini's cast Perseus. Threading its way out from among his
grey hairs, and continuing right down one side of his tawny scorched face and neck, till it
disappeared in his clothing, you saw a slender rod-like mark, lividly whitish.`;

const CHAPTER_41 = `I, Ishmael, was one of that crew; my shouts had gone up with the rest;
my oath had been welded with theirs; and stronger I shouted, and more did I hammer and clinch
my oath, because of the dread in my soul. A wild, mystical, sympathetical feeling was in me;
Ahab's quenchless feud seemed mine. With greedy ears I learned the history of that murderous
monster against whom I and all the others had taken our oaths of violence and revenge. For
some time past, though at intervals only, the unaccompanied, secluded White Whale had haunted
those uncivilized seas mostly frequented by the Sperm Whale fishermen.`;

async function main() {
  if (!process.env.ARKE_USER_KEY || !process.env.KLADOS_ID) {
    console.error('Set ARKE_USER_KEY and KLADOS_ID');
    process.exit(1);
  }

  configureTestClient({
    apiBase: 'https://arke-v1.arke.institute',
    userKey: process.env.ARKE_USER_KEY,
    network: 'test',
  });

  log('Creating Moby Dick collection (NO CLEANUP)...');

  const coll = await createCollection({ label: `Moby Dick (Keep) ${Date.now()}` });
  log(`Collection: ${coll.id}`);

  const novel = await apiRequest<{ id: string }>('POST', '/entities', {
    type: 'literary_work',
    collection: coll.id,
    properties: {
      label: 'Moby-Dick; or, The Whale',
      author: 'Herman Melville',
      publication_year: 1851,
      themes: ['Obsession', 'Revenge', 'Nature vs. Man', 'Fate', 'Good vs. Evil'],
      summary: `The sailor Ishmael narrates the obsessive quest of Ahab, captain of the
whaling ship Pequod, for revenge against Moby Dick, the giant white sperm whale that bit
off his leg. Considered one of the Great American Novels.`,
    },
  });
  log(`Novel: ${novel.id}`);

  const chapters = [
    { num: 1, title: 'Loomings', text: CHAPTER_1 },
    { num: 28, title: 'Ahab', text: CHAPTER_28 },
    { num: 41, title: 'Moby Dick', text: CHAPTER_41 },
  ];

  const chapterIds: string[] = [];
  for (const ch of chapters) {
    const e = await apiRequest<{ id: string }>('POST', '/entities', {
      type: 'chapter',
      collection: coll.id,
      properties: { label: `Chapter ${ch.num}: ${ch.title}`, content: ch.text },
      relationships: [{ predicate: 'part_of', peer: novel.id }],
    });
    chapterIds.push(e.id);
    log(`  Chapter ${ch.num}: ${e.id}`);
  }

  const characters = [
    { name: 'Captain Ahab', desc: 'Monomaniacal captain obsessed with killing Moby Dick. Lost his leg to the whale. Bears a livid white scar from crown to sole.' },
    { name: 'Ishmael', desc: 'The narrator, a thoughtful and philosophical sailor who signs aboard the Pequod. Sole survivor of the final encounter with Moby Dick.' },
    { name: 'Moby Dick', desc: 'The legendary white sperm whale, enormous and infamous for his ferocity. Object of Ahab obsessive quest for vengeance.' },
    { name: 'Queequeg', desc: 'Harpooner from the South Pacific island of Rokovoko. Despite fearsome appearance, he is noble and kind. Ishmael closest friend.' },
  ];

  const charIds: string[] = [];
  for (const c of characters) {
    const e = await apiRequest<{ id: string }>('POST', '/entities', {
      type: 'character',
      collection: coll.id,
      properties: { label: c.name, description: c.desc },
      relationships: [{ predicate: 'appears_in', peer: novel.id }],
    });
    charIds.push(e.id);
    log(`  Character ${c.name}: ${e.id}`);
  }

  // Add relationships to novel
  const tip = await apiRequest<{ cid: string }>('GET', `/entities/${novel.id}/tip`);
  await apiRequest('PUT', `/entities/${novel.id}`, {
    expect_tip: tip.cid,
    relationships_add: [
      ...chapterIds.map(id => ({ predicate: 'contains', peer: id })),
      ...charIds.map(id => ({ predicate: 'features', peer: id })),
    ],
  });
  log(`Added ${chapterIds.length + charIds.length} relationships to novel`);

  // Invoke describe
  log('\nInvoking describe klados...');
  const result = await invokeKlados({
    kladosId: process.env.KLADOS_ID,
    targetEntity: novel.id,
    targetCollection: coll.id,
    confirm: true,
    properties: { style: 'detailed' },
  });
  log(`Job: ${result.job_id}`);

  const kladosLog = await waitForKladosLog(result.job_collection!, {
    timeout: 180000,
    pollInterval: 5000,
  });
  assertLogCompleted(kladosLog);
  log('Job completed!');

  // Show results
  const updated = await getEntity(novel.id);
  console.log('\n' + '='.repeat(60));
  console.log('GENERATED DESCRIPTION');
  console.log('='.repeat(60));
  console.log(`\nNovel ID: ${novel.id}`);
  console.log(`Collection ID: ${coll.id}`);
  console.log(`\nDescription (${(updated.properties.description as string).length} chars):\n`);
  console.log(updated.properties.description);
  console.log('\n' + '='.repeat(60));
  console.log('\n*** ENTITIES KEPT - NO CLEANUP ***');
  console.log(`\nView at: https://arke.institute/test/entities/${novel.id}`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
