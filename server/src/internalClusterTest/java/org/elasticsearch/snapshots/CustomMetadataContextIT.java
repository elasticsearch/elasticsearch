/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataSection;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.TestMetadataSection;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CustomMetadataContextIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestMetadataSectionPlugin.class);
    }

    public void testShouldNotRestoreRepositoryMetadata() {
        var repoPath = randomRepoPath();

        logger.info("create repository");
        createRepository("test-repo-1", "fs", repoPath);

        logger.info("create snapshot");
        createFullSnapshot("test-repo-1", "test-snap");
        assertThat(getSnapshot("test-repo-1", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("delete repository");
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-1"));

        logger.info("create another repository");
        createRepository("test-repo-2", "fs", repoPath);

        logger.info("restore snapshot");
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo-2", "test-snap")
            .setRestoreGlobalState(true)
            .setIndices("-*")
            .setWaitForCompletion(true)
            .get();

        logger.info("make sure old repository wasn't restored");
        ActionRequestBuilder<?, ?> builder = clusterAdmin().prepareGetRepositories(TEST_REQUEST_TIMEOUT, "test-repo-1");
        expectThrows(RepositoryMissingException.class, builder);
        assertThat(clusterAdmin().prepareGetRepositories(TEST_REQUEST_TIMEOUT, "test-repo-2").get().repositories().size(), equalTo(1));
    }

    public void testShouldRestoreOnlySnapshotMetadata() throws Exception {
        var repoPath = randomRepoPath();

        logger.info("create repository");
        createRepository("test-repo", "fs", repoPath);

        logger.info("add custom persistent metadata");
        boolean isSnapshotMetadataSet = randomBoolean();
        updateClusterState(currentState -> currentState.copyAndUpdateMetadata(metadataBuilder -> {
            if (isSnapshotMetadataSet) {
                metadataBuilder.putSection(SnapshotMetadata.TYPE, new SnapshotMetadata("before_snapshot_s"));
            }
            metadataBuilder.putSection(ApiMetadata.TYPE, new ApiMetadata("before_snapshot_ns"));
        }));

        logger.info("create snapshot");
        createFullSnapshot("test-repo", "test-snapshot");
        assertThat(getSnapshot("test-repo", "test-snapshot").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("update custom persistent metadata");
        updateClusterState(currentState -> currentState.copyAndUpdateMetadata(metadataBuilder -> {
            if (isSnapshotMetadataSet == false || randomBoolean()) {
                metadataBuilder.putSection(SnapshotMetadata.TYPE, new SnapshotMetadata("after_snapshot_s"));
            } else {
                metadataBuilder.removeSection(SnapshotMetadata.TYPE);
            }
            metadataBuilder.putSection(ApiMetadata.TYPE, new ApiMetadata("after_snapshot_ns"));
        }));

        logger.info("restore snapshot");
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snapshot")
            .setRestoreGlobalState(true)
            .setIndices("-*")
            .setWaitForCompletion(true)
            .get();

        var metadata = clusterAdmin().prepareState().get().getState().getMetadata();
        logger.info("check that custom persistent metadata [{}] is correctly restored", metadata);
        if (isSnapshotMetadataSet) {
            assertThat(metadata.<SnapshotMetadata>section(SnapshotMetadata.TYPE).getData(), equalTo("before_snapshot_s"));
        } else {
            assertThat(metadata.<SnapshotMetadata>section(SnapshotMetadata.TYPE), nullValue());
        }
        assertThat(metadata.<ApiMetadata>section(ApiMetadata.TYPE).getData(), equalTo("after_snapshot_ns"));
    }

    public void testShouldKeepGatewayMetadataAfterRestart() throws Exception {
        logger.info("add custom gateway metadata");
        updateClusterState(currentState -> currentState.copyAndUpdateMetadata(metadataBuilder -> {
            metadataBuilder.putSection(GatewayMetadata.TYPE, new GatewayMetadata("before_restart_s_gw"));
            metadataBuilder.putSection(ApiMetadata.TYPE, new ApiMetadata("before_restart_ns"));
        }));

        logger.info("restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        var metadata = clusterAdmin().prepareState().get().getState().getMetadata();
        logger.info("check that gateway custom metadata [{}] survived full cluster restart", metadata);
        assertThat(metadata.<GatewayMetadata>section(GatewayMetadata.TYPE).getData(), equalTo("before_restart_s_gw"));
        assertThat(metadata.<ApiMetadata>section(ApiMetadata.TYPE), nullValue());
    }

    public void testShouldExposeApiMetadata() throws Exception {
        logger.info("add custom api metadata");
        updateClusterState(currentState -> currentState.copyAndUpdateMetadata(metadataBuilder -> {
            metadataBuilder.putSection(ApiMetadata.TYPE, new ApiMetadata("before_restart_s_gw"));
            metadataBuilder.putSection(NonApiMetadata.TYPE, new NonApiMetadata("before_restart_ns"));
        }));

        var metadata = clusterAdmin().prepareState().get().getState().getMetadata();
        logger.info("check that api custom metadata [{}] is visible via api", metadata);
        assertThat(metadata.<ApiMetadata>section(ApiMetadata.TYPE).getData(), equalTo("before_restart_s_gw"));
        assertThat(metadata.<NonApiMetadata>section(NonApiMetadata.TYPE), nullValue());
    }

    public static class TestMetadataSectionPlugin extends Plugin {

        private final List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>();
        private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

        public TestMetadataSectionPlugin() {
            registerBuiltinWritables();
        }

        private <T extends MetadataSection> void registerMetadataSection(
            String name,
            Writeable.Reader<T> reader,
            Writeable.Reader<NamedDiff<?>> diffReader,
            CheckedFunction<XContentParser, T, IOException> parser
        ) {
            namedWritables.add(new NamedWriteableRegistry.Entry(MetadataSection.class, name, reader));
            namedWritables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, name, diffReader));
            namedXContents.add(new NamedXContentRegistry.Entry(MetadataSection.class, new ParseField(name), parser));
        }

        private void registerBuiltinWritables() {
            Map.<String, Function<String, TestMetadataSection>>of(
                SnapshotMetadata.TYPE,
                SnapshotMetadata::new,
                GatewayMetadata.TYPE,
                GatewayMetadata::new,
                ApiMetadata.TYPE,
                ApiMetadata::new,
                NonApiMetadata.TYPE,
                NonApiMetadata::new
            )
                .forEach(
                    (type, constructor) -> registerMetadataSection(
                        type,
                        in -> TestMetadataSection.readFrom(constructor, in),
                        in -> TestMetadataSection.readDiffFrom(type, in),
                        parser -> TestMetadataSection.fromXContent(constructor, parser)
                    )
                );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return namedWritables;
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return namedXContents;
        }
    }

    private abstract static class ThisTestMetadataSection extends TestMetadataSection {
        private final String type;
        private final EnumSet<Metadata.XContentContext> context;

        ThisTestMetadataSection(String data, String type, EnumSet<Metadata.XContentContext> context) {
            super(data);
            this.type = type;
            this.context = context;
        }

        @Override
        public String getWriteableName() {
            return type;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return context;
        }
    }

    private static class SnapshotMetadata extends ThisTestMetadataSection {
        public static final String TYPE = "test_metadata_scope_snapshot";

        SnapshotMetadata(String data) {
            super(data, TYPE, Metadata.API_AND_SNAPSHOT);
        }
    }

    private static class GatewayMetadata extends ThisTestMetadataSection {
        public static final String TYPE = "test_metadata_scope_gateway";

        GatewayMetadata(String data) {
            super(data, TYPE, Metadata.API_AND_GATEWAY);
        }
    }

    private static class ApiMetadata extends ThisTestMetadataSection {
        public static final String TYPE = "test_metadata_scope_api";

        ApiMetadata(String data) {
            super(data, TYPE, Metadata.API_ONLY);
        }
    }

    private static class NonApiMetadata extends ThisTestMetadataSection {
        public static final String TYPE = "test_metadata_scope_non_api";

        NonApiMetadata(String data) {
            super(data, TYPE, EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT));
        }
    }
}
