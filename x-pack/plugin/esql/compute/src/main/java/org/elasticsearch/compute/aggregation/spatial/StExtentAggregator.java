/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

abstract class StExtentAggregator {
    public static void combineIntermediate(StExtentState current, int minX, int maxX, int maxY, int minY) {
        current.add(minX, maxX, maxY, minY);
    }

    public static void combineIntermediate(StExtentGroupingState current, int groupId, int minX, int maxX, int maxY, int minY) {
        current.add(groupId, minX, maxX, maxY, minY);
    }

    public static Block evaluateFinal(StExtentState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static Block evaluateFinal(StExtentGroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(selected, driverContext);
    }

    public static void combineStates(StExtentGroupingState current, int groupId, StExtentGroupingState inState, int inPosition) {
        current.add(groupId, inState, inPosition);
    }
}
