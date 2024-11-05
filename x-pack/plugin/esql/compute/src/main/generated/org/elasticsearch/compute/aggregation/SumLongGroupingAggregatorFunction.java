// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.ArithmeticException;
import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sum", ElementType.LONG),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN),
      new IntermediateStateDesc("failed", ElementType.BOOLEAN)  );

  private final LongFallibleArrayState state;

  private final Warnings warnings;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  public SumLongGroupingAggregatorFunction(Warnings warnings, List<Integer> channels,
      LongFallibleArrayState state, DriverContext driverContext) {
    this.warnings = warnings;
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
  }

  public static SumLongGroupingAggregatorFunction create(Warnings warnings, List<Integer> channels,
      DriverContext driverContext) {
    return new SumLongGroupingAggregatorFunction(warnings, channels, new LongFallibleArrayState(driverContext.bigArrays(), SumLongAggregator.init()), driverContext);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds,
      Page page) {
    LongBlock valuesBlock = page.getBlock(channels.get(0));
    LongVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      if (valuesBlock.mayHaveNulls()) {
        state.enableGroupIdTracking(seenGroupIds);
      }
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntVector groups, LongBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (state.hasFailed(groupId)) {
        continue;
      }
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), values.getLong(v)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, LongVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (state.hasFailed(groupId)) {
        continue;
      }
      try {
        state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), values.getLong(groupPosition + positionOffset)));
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.setFailed(groupId);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, LongBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        if (values.isNull(groupPosition + positionOffset)) {
          continue;
        }
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          try {
            state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), values.getLong(v)));
          } catch (ArithmeticException e) {
            warnings.registerException(e);
            state.setFailed(groupId);
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBlock groups, LongVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), values.getLong(groupPosition + positionOffset)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block sumUncast = page.getBlock(channels.get(0));
    if (sumUncast.areAllValuesNull()) {
      return;
    }
    LongVector sum = ((LongBlock) sumUncast).asVector();
    Block seenUncast = page.getBlock(channels.get(1));
    if (seenUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
    Block failedUncast = page.getBlock(channels.get(2));
    if (failedUncast.areAllValuesNull()) {
      return;
    }
    BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
    assert sum.getPositionCount() == seen.getPositionCount() && sum.getPositionCount() == failed.getPositionCount();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      if (failed.getBoolean(groupPosition + positionOffset)) {
        state.setFailed(groupId);
      } else if (seen.getBoolean(groupPosition + positionOffset)) {
        try {
          state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), sum.getLong(groupPosition + positionOffset)));
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
    if (input.getClass() != getClass()) {
      throw new IllegalArgumentException("expected " + getClass() + "; got " + input.getClass());
    }
    LongFallibleArrayState inState = ((SumLongGroupingAggregatorFunction) input).state;
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    if (inState.hasValue(position)) {
      state.set(groupId, SumLongAggregator.combine(state.getOrDefault(groupId), inState.get(position)));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      DriverContext driverContext) {
    blocks[offset] = state.toValuesBlock(selected, driverContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channels=").append(channels);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
