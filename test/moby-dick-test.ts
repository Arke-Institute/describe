#!/usr/bin/env npx tsx
/**
 * Rich E2E Test: Moby Dick Collection
 *
 * Creates a literary collection with:
 * - The novel as the main entity with substantial content
 * - Multiple chapters as related entities
 * - Character profiles as related entities
 *
 * Tests the describe klados's ability to:
 * - Handle large text content
 * - Gather context from many relationships
 * - Apply truncation if needed
 * - Synthesize information across related entities
 *
 * Usage:
 *   npx tsx test/moby-dick-test.ts
 */

import {
  configureTestClient,
  createCollection,
  deleteEntity,
  getEntity,
  invokeKlados,
  waitForKladosLog,
  assertLogCompleted,
  apiRequest,
  log,
} from '@arke-institute/klados-testing';

// Configuration
const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const KLADOS_ID = process.env.KLADOS_ID;

// Chapter 1: Loomings (full text)
const CHAPTER_1_TEXT = `Call me Ishmael. Some years ago—never mind how long precisely—having
little or no money in my purse, and nothing particular to interest me
on shore, I thought I would sail about a little and see the watery part
of the world. It is a way I have of driving off the spleen and
regulating the circulation. Whenever I find myself growing grim about
the mouth; whenever it is a damp, drizzly November in my soul; whenever
I find myself involuntarily pausing before coffin warehouses, and
bringing up the rear of every funeral I meet; and especially whenever
my hypos get such an upper hand of me, that it requires a strong moral
principle to prevent me from deliberately stepping into the street, and
methodically knocking people's hats off—then, I account it high time to
get to sea as soon as I can. This is my substitute for pistol and ball.
With a philosophical flourish Cato throws himself upon his sword; I
quietly take to the ship. There is nothing surprising in this. If they
but knew it, almost all men in their degree, some time or other,
cherish very nearly the same feelings towards the ocean with me.

There now is your insular city of the Manhattoes, belted round by
wharves as Indian isles by coral reefs—commerce surrounds it with her
surf. Right and left, the streets take you waterward. Its extreme
downtown is the battery, where that noble mole is washed by waves, and
cooled by breezes, which a few hours previous were out of sight of
land. Look at the crowds of water-gazers there.

Circumambulate the city of a dreamy Sabbath afternoon. Go from Corlears
Hook to Coenties Slip, and from thence, by Whitehall, northward. What
do you see?—Posted like silent sentinels all around the town, stand
thousands upon thousands of mortal men fixed in ocean reveries. Some
leaning against the spiles; some seated upon the pier-heads; some
looking over the bulwarks of ships from China; some high aloft in the
rigging, as if striving to get a still better seaward peep. But these
are all landsmen; of week days pent up in lath and plaster—tied to
counters, nailed to benches, clinched to desks. How then is this? Are
the green fields gone? What do they here?

But look! here come more crowds, pacing straight for the water, and
seemingly bound for a dive. Strange! Nothing will content them but the
extremest limit of the land; loitering under the shady lee of yonder
warehouses will not suffice. No. They must get just as nigh the water
as they possibly can without falling in. And there they stand—miles of
them—leagues. Inlanders all, they come from lanes and alleys, streets
and avenues—north, east, south, and west. Yet here they all unite.`;

// Chapter 28: Ahab (substantial excerpt)
const CHAPTER_28_TEXT = `For several days after leaving Nantucket, nothing above hatches was
seen of Captain Ahab. The mates regularly relieved each other at the
watches, and for aught that could be seen to the contrary, they seemed
to be the only commanders of the ship; only they sometimes issued from
the cabin with orders so sudden and peremptory, that after all it was
plain they but commanded vicariously. Yes, their supreme lord and
dictator was there, though hitherto unseen by any eyes not permitted to
penetrate into the now sacred retreat of the cabin.

There seemed no sign of common bodily illness about him, nor of the
recovery from any. He looked like a man cut away from the stake, when
the fire has overrunningly wasted all the limbs without consuming them,
or taking away one particle from their compacted aged robustness. His
whole high, broad form, seemed made of solid bronze, and shaped in an
unalterable mould, like Cellini's cast Perseus. Threading its way out
from among his grey hairs, and continuing right down one side of his
tawny scorched face and neck, till it disappeared in his clothing, you
saw a slender rod-like mark, lividly whitish. It resembled that
perpendicular seam sometimes made in the straight, lofty trunk of a
great tree, when the upper lightning tearingly darts down it, and
without wrenching a single twig, peels and grooves out the bark from
top to bottom, ere running off into the soil, leaving the tree still
greenly alive, but branded.

So powerfully did the whole grim aspect of Ahab affect me, and the
livid brand which streaked it, that for the first few moments I hardly
noted that not a little of this overbearing grimness was owing to the
barbaric white leg upon which he partly stood.`;

// Chapter 41: Moby Dick (substantial excerpt)
const CHAPTER_41_TEXT = `I, Ishmael, was one of that crew; my shouts had gone up with the rest;
my oath had been welded with theirs; and stronger I shouted, and more
did I hammer and clinch my oath, because of the dread in my soul. A
wild, mystical, sympathetical feeling was in me; Ahab's quenchless feud
seemed mine. With greedy ears I learned the history of that murderous
monster against whom I and all the others had taken our oaths of
violence and revenge.

For some time past, though at intervals only, the unaccompanied,
secluded White Whale had haunted those uncivilized seas mostly
frequented by the Sperm Whale fishermen. But not all of them knew of
his existence; only a few of them, comparatively, had knowingly seen
him; while the number who as yet had actually and knowingly given
battle to him, was small indeed.

No wonder, then, that ever gathering volume from the mere transit over
the widest watery spaces, the outblown rumors of the White Whale did in
the end incorporate with themselves all manner of morbid hints, and
half-formed fœtal suggestions of supernatural agencies, which
eventually invested Moby Dick with new terrors unborrowed from anything
that visibly appears. So that in many cases such a panic did he finally
strike, that few who by those rumors, at least, had heard of the White
Whale, few of those hunters were willing to encounter the perils of his
jaw.`;

// Chapter 42: The Whiteness of the Whale
const CHAPTER_42_TEXT = `What the white whale was to Ahab, has been hinted; what, at times, he
was to me, as yet remains unsaid. Aside from those more obvious
considerations touching Moby Dick, which could not but occasionally
awaken in any man's soul some alarm, there was another thought, or
rather vague, nameless horror concerning him, which at times by its
intensity completely overpowered all the rest; and yet so mystical and
well nigh ineffable was it, that I almost despair of putting it in a
comprehensible form. It was the whiteness of the whale that above all
things appalled me. But how can I hope to explain myself here; and yet,
in some dim, random way, explain myself I must, else all these chapters
might be naught.

Though in many natural objects, whiteness refiningly enhances beauty,
as if imparting some special virtue of its own, as in marbles,
japonicas, and pearls; and though various nations have in some way
recognised a certain royal preeminence in this hue; yet for all these
accumulated associations, with whatever is sweet, and honorable, and
sublime, there yet lurks an elusive something in the innermost idea of
this hue, which strikes more of panic to the soul than that redness
which affrights in blood.`;

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('Error: ARKE_USER_KEY environment variable is required');
    process.exit(1);
  }
  if (!KLADOS_ID) {
    console.error('Error: KLADOS_ID environment variable is required');
    process.exit(1);
  }

  // Configure the test client
  configureTestClient({
    apiBase: ARKE_API_BASE,
    userKey: ARKE_USER_KEY,
    network: NETWORK,
  });

  log('='.repeat(60));
  log('Moby Dick Collection Test');
  log('='.repeat(60));

  // Test fixtures
  let targetCollection: { id: string } | null = null;
  let novelEntity: { id: string } | null = null;
  const chapterEntities: { id: string }[] = [];
  const characterEntities: { id: string }[] = [];

  try {
    // Create collection
    log('\n📚 Creating test collection...');
    targetCollection = await createCollection({
      label: `Moby Dick Test ${Date.now()}`,
      description: 'Test collection for rich describe klados test',
    });
    log(`Created collection: ${targetCollection.id}`);

    // Create the novel entity (target for description)
    log('\n📖 Creating novel entity...');
    novelEntity = await apiRequest<{ id: string }>('POST', '/entities', {
      type: 'literary_work',
      collection: targetCollection.id,
      properties: {
        label: 'Moby-Dick; or, The Whale',
        title: 'Moby-Dick; or, The Whale',
        author: 'Herman Melville',
        publication_year: 1851,
        publisher: 'Harper & Brothers (New York), Richard Bentley (London)',
        genre: ['Novel', 'Adventure fiction', 'Epic', 'Sea story'],
        themes: [
          'Obsession',
          'Revenge',
          'Nature vs. Man',
          'Social status',
          'Good vs. Evil',
          'Fate',
          'Religion',
          'Class and social hierarchy',
        ],
        summary: `Moby-Dick is an 1851 novel by American writer Herman Melville. The book is
the sailor Ishmael's narrative of the obsessive quest of Ahab, captain of the whaling ship
Pequod, for revenge against Moby Dick, the giant white sperm whale that on the ship's
previous voyage bit off Ahab's leg at the knee.`,
        literary_significance: `Considered one of the Great American Novels, Moby-Dick is
renowned for its complex symbolism, rich prose style, and profound philosophical depth.
Though initially a commercial failure, it has since been recognized as a masterpiece of
American literature and a key work of the American Renaissance period.`,
        setting: 'The Atlantic and Pacific Oceans, primarily aboard the whaling ship Pequod',
        narrative_style: 'First-person narrator (Ishmael) with numerous digressions into cetology, history, and philosophy',
      },
    });
    log(`Created novel: ${novelEntity.id}`);

    // Create chapter entities
    log('\n📑 Creating chapter entities...');

    const chapters = [
      {
        number: 1,
        title: 'Loomings',
        text: CHAPTER_1_TEXT,
        summary: 'Ishmael introduces himself and explains his reasons for going to sea, reflecting on the universal human fascination with water and the ocean.',
      },
      {
        number: 28,
        title: 'Ahab',
        text: CHAPTER_28_TEXT,
        summary: 'The first appearance of Captain Ahab on deck, revealing his imposing physical presence and the mysterious scar running down his face.',
      },
      {
        number: 41,
        title: 'Moby Dick',
        text: CHAPTER_41_TEXT,
        summary: 'The crew learns the history of the White Whale and the legends surrounding Moby Dick that have spread through the whaling fleet.',
      },
      {
        number: 42,
        title: 'The Whiteness of the Whale',
        text: CHAPTER_42_TEXT,
        summary: 'Ishmael meditates on why the whiteness of Moby Dick fills him with particular terror, exploring the symbolic meanings of the color white.',
      },
    ];

    for (const chapter of chapters) {
      const chapterEntity = await apiRequest<{ id: string }>('POST', '/entities', {
        type: 'literary_chapter',
        collection: targetCollection.id,
        properties: {
          label: `Chapter ${chapter.number}: ${chapter.title}`,
          chapter_number: chapter.number,
          chapter_title: chapter.title,
          content: chapter.text,
          summary: chapter.summary,
          word_count: chapter.text.split(/\s+/).length,
        },
        relationships: [
          { predicate: 'part_of', peer: novelEntity.id },
        ],
      });
      chapterEntities.push(chapterEntity);
      log(`  Created Chapter ${chapter.number}: ${chapter.title} (${chapterEntity.id})`);
    }

    // Create character entities
    log('\n👤 Creating character entities...');

    const characters = [
      {
        name: 'Captain Ahab',
        role: 'Protagonist/Antagonist',
        description: `Captain of the Pequod. A monomaniacal sea captain obsessed with killing Moby Dick,
the white whale that bit off his leg. His body bears a livid white scar from crown to sole, and he
walks on a prosthetic leg made from a whale's jawbone. A tragic figure driven by vengeance, Ahab
ultimately leads his crew to destruction in pursuit of the whale.`,
        first_appearance: 'Chapter 28',
        fate: 'Killed by Moby Dick, dragged into the ocean by harpoon lines',
      },
      {
        name: 'Ishmael',
        role: 'Narrator',
        description: `A sailor and schoolteacher who serves as the novel's narrator. He signs
aboard the Pequod as a common sailor. Ishmael is thoughtful, philosophical, and provides
commentary on life, death, and the nature of existence. He is the sole survivor of the
Pequod's final encounter with Moby Dick.`,
        first_appearance: 'Chapter 1',
        fate: 'Survives by clinging to a coffin life-buoy',
      },
      {
        name: 'Queequeg',
        role: 'Supporting Character',
        description: `A harpooner from the South Pacific island of Rokovoko (a fictional island).
Despite his fearsome appearance with extensive tattoos, Queequeg is noble and kind. He becomes
Ishmael's closest friend and shares a bed with him at the Spouter-Inn. A skilled whaler and
devout practitioner of his native religion.`,
        first_appearance: 'Chapter 3',
        fate: 'Dies aboard the Pequod',
      },
      {
        name: 'Moby Dick',
        role: 'Symbol/Antagonist',
        description: `The legendary white sperm whale that is the object of Captain Ahab's
obsessive quest. Enormous in size and infamous among whalers for his ferocity and cunning.
Moby Dick bit off Ahab's leg during a previous voyage. The whale represents many things:
nature's indifference, the unknowable, evil, fate, or nothing at all - its meaning is
deliberately ambiguous.`,
        first_appearance: 'Chapter 133 (physically)',
        fate: 'Survives the final confrontation',
      },
      {
        name: 'Starbuck',
        role: 'First Mate',
        description: `The Pequod's first mate, a Nantucket Quaker. Starbuck is the voice of
reason and morality aboard the ship. He alone dares to challenge Ahab's obsessive pursuit
of Moby Dick. Practical, religious, and brave, yet ultimately unable to prevent the tragedy.`,
        first_appearance: 'Chapter 26',
        fate: 'Dies when the Pequod sinks',
      },
    ];

    for (const character of characters) {
      const characterEntity = await apiRequest<{ id: string }>('POST', '/entities', {
        type: 'literary_character',
        collection: targetCollection.id,
        properties: {
          label: character.name,
          name: character.name,
          role: character.role,
          description: character.description,
          first_appearance: character.first_appearance,
          fate: character.fate,
        },
        relationships: [
          { predicate: 'appears_in', peer: novelEntity.id },
        ],
      });
      characterEntities.push(characterEntity);
      log(`  Created Character: ${character.name} (${characterEntity.id})`);
    }

    // Add contains relationships to the novel
    log('\n🔗 Adding relationships to novel...');
    const tip = await apiRequest<{ cid: string }>('GET', `/entities/${novelEntity.id}/tip`);
    const allRelated = [...chapterEntities, ...characterEntities];
    await apiRequest('PUT', `/entities/${novelEntity.id}`, {
      expect_tip: tip.cid,
      relationships_add: [
        ...chapterEntities.map(c => ({ predicate: 'contains', peer: c.id })),
        ...characterEntities.map(c => ({ predicate: 'features', peer: c.id })),
      ],
    });
    log(`Added ${allRelated.length} relationships (${chapterEntities.length} chapters, ${characterEntities.length} characters)`);

    // Now invoke the describe klados on the novel
    log('\n🤖 Invoking describe klados on the novel...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: novelEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
      properties: {
        style: 'detailed',
        max_relationships: 100,
      },
    });

    log(`Job started: ${result.job_id}`);
    log(`Job collection: ${result.job_collection}`);

    // Wait for completion
    log('\n⏳ Waiting for job completion...');
    const kladosLog = await waitForKladosLog(result.job_collection!, {
      timeout: 180000, // 3 minutes
      pollInterval: 5000,
    });

    // Check result
    assertLogCompleted(kladosLog);
    log(`\n✅ Job completed with status: ${kladosLog.properties.status}`);

    // Log all messages
    log('\n📋 Log messages:');
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Get the updated novel entity
    const updatedNovel = await getEntity(novelEntity.id);

    log('\n' + '='.repeat(60));
    log('GENERATED DESCRIPTION');
    log('='.repeat(60));
    log(`\nTitle: ${updatedNovel.properties.title}`);
    log(`\nDescription (${(updatedNovel.properties.description as string).length} chars):`);
    log('\n' + updatedNovel.properties.description);
    log('\n' + '='.repeat(60));

    log(`\n📊 Metadata:`);
    log(`  Generated at: ${updatedNovel.properties.description_generated_at}`);
    log(`  Model: ${updatedNovel.properties.description_model}`);

  } catch (error) {
    console.error('\n❌ Test failed:', error);
    throw error;
  } finally {
    // Cleanup
    log('\n🧹 Cleaning up...');
    try {
      for (const entity of [...characterEntities, ...chapterEntities]) {
        if (entity?.id) await deleteEntity(entity.id);
      }
      if (novelEntity?.id) await deleteEntity(novelEntity.id);
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  }
}

main().catch((error) => {
  console.error('Test failed:', error);
  process.exit(1);
});
