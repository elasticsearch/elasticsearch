/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

/**
 * @deprecated the freeze action is deprecated since 7.x, the freeze step was effectively a noop in 8.x and now we make
 * all steps here a noop that skip to the nextStepKey.
 * We preserve the action and the  names of the steps to avoid
 */
@Deprecated
public class FreezeAction implements LifecycleAction {

    public static final String NAME = "freeze";
    public static final String CONDITIONAL_SKIP_FREEZE_STEP = BranchingStep.NAME + "-freeze-check-prerequisites";

    public static final FreezeAction INSTANCE = new FreezeAction();

    private static final ObjectParser<FreezeAction, Void> PARSER = new ObjectParser<>(NAME, () -> INSTANCE);

    public static FreezeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private FreezeAction() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey preFreezeMergeBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_FREEZE_STEP);
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey freezeStepKey = new StepKey(phase, NAME, "freeze");
        return List.of(
            new NoopStep(preFreezeMergeBranchingKey, nextStepKey),
            new NoopStep(checkNotWriteIndex, nextStepKey),
            new NoopStep(freezeStepKey, nextStepKey)
        );
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
