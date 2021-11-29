/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.docker.Docker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.elasticsearch.packaging.util.docker.Docker.removeContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runAdditionalContainer;
import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.Docker.verifyContainerInstallation;
import static org.elasticsearch.packaging.util.docker.Docker.waitForElasticsearch;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeTrue;

public class EnrollmentProcessTests extends PackagingTestCase {

    public void test10ArchiveAutoFormCluster() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        assumeTrue("only archives", distribution.isArchive());
        installation = installArchive(sh, distribution(), getRootTempDir().resolve("elasticsearch-node1"), getCurrentVersion(), true);
        verifyArchiveInstallation(installation, distribution());
        setFileSuperuser("test_superuser", "test_superuser_password");
        sh.getEnv().put("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        Shell.Result startFirstNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, List.of(), false)
        );
        assertThat(startFirstNode.isSuccess(), is(true));
        // Verify that the first node was auto-configured for security
        verifySecurityAutoConfigured(installation);
        // Generate a node enrollment token to be subsequently used by the second node
        Shell.Result createTokenResult = installation.executables().createEnrollmentToken.run("-s node");
        assertThat(Strings.isNullOrEmpty(createTokenResult.stdout), is(false));
        final String enrollmentToken = createTokenResult.stdout;
        // installation now points to the second node
        installation = installArchive(sh, distribution(), getRootTempDir().resolve("elasticsearch-node2"), getCurrentVersion(), true);
        // auto-configure security using the enrollment token
        Shell.Result startSecondNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, List.of("--enrollment-token", enrollmentToken), false)
        );
        // ugly hack, wait for the second node to actually start and join the cluster, all of our current tooling expects/assumes
        // a single installation listening on 9200
        // TODO Make our packaging test methods aware of multiple installations, see https://github.com/elastic/elasticsearch/issues/79688
        waitForSecondNode();
        assertThat(startSecondNode.isSuccess(), is(true));
        verifySecurityAutoConfigured(installation);
        // verify that the two nodes formed a cluster
        assertThat(makeRequest("https://localhost:9200/_cluster/health"), containsString("\"number_of_nodes\":2"));
    }

    public void test20DockerAutoFormCluster() throws Exception {
        assumeTrue("only docker", distribution.isDocker());
        // First node
        installation = runContainer(distribution(), builder().envVar("ELASTIC_PASSWORD", "password"));
        verifyContainerInstallation(installation);
        verifySecurityAutoConfigured(installation);
        waitForElasticsearch(installation);
        final String node1ContainerId = Docker.getContainerId();
        String createTokenResult = installation.executables().createEnrollmentToken.run("-s node").stdout;
        assertThat(createTokenResult, not(emptyOrNullString()));
        final List<String> noWarningOutputLines = createTokenResult.lines()
            .filter(line -> line.startsWith("WARNING:") == false)
            .collect(Collectors.toList());
        assertThat(noWarningOutputLines.size(), equalTo(1));
        // installation refers to second node from now on
        installation = runAdditionalContainer(
            distribution(),
            builder().envVar("ENROLLMENT_TOKEN", noWarningOutputLines.get(0)).extraArgs("--publish 9301:9300 --publish 9201:9200")
        );
        // TODO Make our packaging test methods aware of multiple installations, see https://github.com/elastic/elasticsearch/issues/79688
        waitForElasticsearch(installation);
        verifyContainerInstallation(installation);
        verifySecurityAutoConfigured(installation);
        // Allow some time for the second node to join the cluster, we can probably do this more elegantly in
        // https://github.com/elastic/elasticsearch/issues/79688
        assertBusy(() ->
        // verify that the two nodes formed a cluster
        assertThat(makeRequestAsElastic("https://localhost:9200/_cluster/health", "password"), containsString("\"number_of_nodes\":2")),
            20,
            TimeUnit.SECONDS
        );

        // Cleanup the first node that is still running
        removeContainer(node1ContainerId);
    }

    private void waitForSecondNode() {
        int retries = 60;
        while (retries > 0) {
            retries -= 1;
            try (Socket s = new Socket(InetAddress.getLoopbackAddress(), 9201)) {
                return;
            } catch (IOException e) {
                // ignore, only want to establish a connection
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException("Elasticsearch second node did not start listening on 9201");
    }
}
