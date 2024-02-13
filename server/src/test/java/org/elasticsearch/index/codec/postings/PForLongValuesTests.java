/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.codec.postings;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class PForLongValuesTests extends LuceneTestCase {

    public void testEncodeDecodeLarge() throws IOException {
        final int numValues = RandomNumbers.randomIntBetween(random(), 50, 1000) * ForUtil.BLOCK_SIZE;
        testEncodeDecode(numValues);
    }

    public void testEncodeDecodeMedium() throws IOException {
        final int numValues = RandomNumbers.randomIntBetween(random(), ForUtil.BLOCK_SIZE, 50 * ForUtil.BLOCK_SIZE);
        testEncodeDecode(numValues);
    }

    public void testEncodeDecodeSmall() throws IOException {
        final int numValues = RandomNumbers.randomIntBetween(random(), 0, ForUtil.BLOCK_SIZE);
        testEncodeDecode(numValues);
    }

    private void testEncodeDecode(int numValues) throws IOException {
        final int[] values = new int[numValues];
        int bpv = TestUtil.nextInt(random(), 1, 31);
        for (int i = 0; i < numValues; ++i) {
            if (random().nextInt(512) == 1) {
                bpv = TestUtil.nextInt(random(), 1, 31);
            }
            values[i] = RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
        }

        PForLongValues.Builder longValuesBuilder = PForLongValues.pForBuilder();
        // encode
        for (int i = 0; i < values.length; i++) {
            assertEquals(i, longValuesBuilder.size());
            longValuesBuilder.add(values[i]);

        }
        assertEquals(values.length, longValuesBuilder.size());
        PForLongValues longValues = longValuesBuilder.build();
        // decode
        assertEquals(values.length, longValues.size());
        PForLongValues.Iterator iterator = longValues.iterator();

        for (int i = 0; i < longValues.size(); ++i) {
            assertTrue(iterator.hasNext());
            assertEquals(values[i], Math.toIntExact(iterator.next()));
        }
        assertFalse(iterator.hasNext());

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> longValuesBuilder.add(random().nextInt(Integer.MAX_VALUE))
        );
        assertEquals("Cannot be used after build()", ex.getMessage());
    }
}
