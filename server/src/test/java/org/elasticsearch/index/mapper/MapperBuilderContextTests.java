/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

public class MapperBuilderContextTests extends ESTestCase {

    public void testRoot() {
        MapperBuilderContext root = MapperBuilderContext.root(false, false);
        assertFalse(root.isSourceSynthetic());
        assertFalse(root.isDataStream());
        assertEquals(MapperService.MergeReason.MAPPING_UPDATE, root.getMergeReason());
        assertFalse(root.isInNestedContext());
    }

    public void testRootWithMergeReason() {
        MapperService.MergeReason mergeReason = randomFrom(MapperService.MergeReason.values());
        MapperBuilderContext root = MapperBuilderContext.root(false, false, mergeReason);
        assertFalse(root.isSourceSynthetic());
        assertFalse(root.isDataStream());
        assertEquals(mergeReason, root.getMergeReason());
        assertFalse(root.isInNestedContext());
    }

    public void testNestedContexts() {
        MapperBuilderContext root = MapperBuilderContext.root(true, false);
        assertTrue(root.isSourceSynthetic());
        assertFalse(root.isDataStream());
        assertEquals(MapperService.MergeReason.MAPPING_UPDATE, root.getMergeReason());
        assertFalse(root.isInNestedContext());
    }
}
