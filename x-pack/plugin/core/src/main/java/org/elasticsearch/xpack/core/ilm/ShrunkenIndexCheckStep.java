/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

/**
 * Verifies that an index was created through a shrink operation, rather than created some other way.
 * Also checks the name of the index to ensure it aligns with what is expected from an index shrunken via a previous step.
 */
public class ShrunkenIndexCheckStep extends ClusterStateWaitStep {
    public static final String NAME = "is-shrunken-index";
    private static final Logger logger = LogManager.getLogger(ShrunkenIndexCheckStep.class);
    private String shrunkIndexPrefix;

    public ShrunkenIndexCheckStep(StepKey key, StepKey nextStepKey, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata idxMeta = clusterState.getMetadata().index(index);
        if (idxMeta == null) {
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            // Index must have been since deleted, ignore it
            return new Result(false, null);
        }
        String shrunkenIndexSource = IndexMetadata.INDEX_RESIZE_SOURCE_NAME.get(
            clusterState.metadata().index(index).getSettings());
        if (Strings.isNullOrEmpty(shrunkenIndexSource)) {
            throw new IllegalStateException("step[" + NAME + "] is checking an un-shrunken index[" + index.getName() + "]");
        }
        boolean isConditionMet = index.getName().equals(shrunkIndexPrefix + shrunkenIndexSource) &&
                clusterState.metadata().index(shrunkenIndexSource) == null;
        if (isConditionMet) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info(shrunkenIndexSource));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrunkenIndexCheckStep other = (ShrunkenIndexCheckStep) obj;
        return super.equals(obj) &&
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }

    public static final class Info implements ToXContentObject {

        private final String originalIndexName;
        private final String message;

        static final ParseField ORIGINAL_INDEX_NAME = new ParseField("original_index_name");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("shrunken_index_check_step_info",
                a -> new Info((String) a[0]));
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ORIGINAL_INDEX_NAME);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(String originalIndexName) {
            this.originalIndexName = originalIndexName;
            this.message = "Waiting for original index [" + originalIndexName + "] to be deleted";
        }

        public String getOriginalIndexName() {
            return originalIndexName;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(ORIGINAL_INDEX_NAME.getPreferredName(), originalIndexName);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(originalIndexName);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Info other = (Info) obj;
            return Objects.equals(originalIndexName, other.originalIndexName);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
