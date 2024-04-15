/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.ImpersonateOfficialClientTestClient;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class EsqlClientYamlIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlClientYamlIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        if ("false".equals(System.getProperty("tests.version_parameter_unsupported"))) {
            return createParameters();
        }
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : createParameters()) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            try {
                ClientYamlTestSection modified = new ClientYamlTestSection(
                    candidate.getTestSection().getLocation(),
                    candidate.getTestSection().getName(),
                    candidate.getTestSection().getPrerequisiteSection(),
                    candidate.getTestSection().getExecutableSections().stream().map(EsqlClientYamlIT::modifyExecutableSection).toList()
                );
                result.add(new Object[] { new ClientYamlTestCandidate(candidate.getRestTestSuite(), modified) });
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("error modifying " + candidate + ": " + e.getMessage(), e);
            }
        }
        return result;
    }

    private static ExecutableSection modifyExecutableSection(ExecutableSection e) {
        if (false == (e instanceof DoSection)) {
            return e;
        }
        DoSection doSection = (DoSection) e;
        String api = doSection.getApiCallSection().getApi();
        if (false == api.equals("esql.query")) {
            return e;
        }
        for (Map<String, Object> body : doSection.getApiCallSection().getBodies()) {
            body.remove("version");
        }
        return e;
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Override
    protected ClientYamlTestClient initClientYamlTestClient(
        final ClientYamlSuiteRestSpec restSpec,
        final RestClient restClient,
        final List<HttpHost> hosts
    ) {
        if ("true".equals(System.getProperty("tests.version_parameter_unsupported"))) {
            return new ImpersonateOfficialClientTestClient(restSpec, restClient, hosts, this::getClientBuilderWithSniffedHosts, "es=8.13");
        }
        return super.initClientYamlTestClient(restSpec, restClient, hosts);
    }
}
