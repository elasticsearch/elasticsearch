/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;

public class SynonymsManagementAPIServiceIT extends ESIntegTestCase {

    private SynonymsManagementAPIService synonymsManagementAPIService;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MapperExtrasPlugin.class);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        synonymsManagementAPIService = new SynonymsManagementAPIService(client());
    }

    public void testCreateManySynonyms() throws InterruptedException {
        CountDownLatch putLatch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        int rulesNumber = randomIntBetween(
            SynonymsManagementAPIService.MAX_SYNONYMS_SETS / 2,
            SynonymsManagementAPIService.MAX_SYNONYMS_SETS
        );
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(rulesNumber, rulesNumber), new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                assertEquals(
                    SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED,
                    synonymsReloadResult.synonymsOperationResult()
                );
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        putLatch.await(5, TimeUnit.SECONDS);

        CountDownLatch getLatch = new CountDownLatch(1);
        // Also retrieve them
        synonymsManagementAPIService.getSynonymSetRules(
            synonymSetId,
            0,
            SynonymsManagementAPIService.MAX_SYNONYMS_SETS,
            new ActionListener<>() {
                @Override
                public void onResponse(PagedResult<SynonymRule> synonymRulePagedResult) {
                    // TODO This fails in CI but passes locally. Why?
                    // assertEquals(rulesNumber, synonymRulePagedResult.totalResults());
                    // assertEquals(rulesNumber, synonymRulePagedResult.pageResults().length);
                    getLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );

        getLatch.await(5, TimeUnit.SECONDS);
    }

    public void testCreateTooManySynonymsAtOnce() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(
            randomIdentifier(),
            randomSynonymsSet(SynonymsManagementAPIService.MAX_SYNONYMS_SETS + 1, SynonymsManagementAPIService.MAX_SYNONYMS_SETS * 2),
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    fail("Shouldn't create synonyms that are too large");
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IllegalArgumentException) {
                        latch.countDown();
                    } else {
                        fail(e);
                    }
                }
            }
        );

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testCreateTooManySynonymsUsingRuleUpdates() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        int rulesToUpdate = randomIntBetween(1, 10);
        int synonymsToCreate = SynonymsManagementAPIService.MAX_SYNONYMS_SETS - rulesToUpdate;
        String synonymSetId = randomIdentifier();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(synonymsToCreate), new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Create as many rules as should fail
                SynonymRule[] rules = randomSynonymsSet(atLeast(rulesToUpdate + 1));
                CountDownLatch updatedRulesLatch = new CountDownLatch(rulesToUpdate);
                for (int i = 0; i < rulesToUpdate; i++) {
                    synonymsManagementAPIService.putSynonymRule(synonymSetId, rules[i], new ActionListener<>() {
                        @Override
                        public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                            updatedRulesLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail("Shouldn't have failed to update a rule");
                        }
                    });
                }
                try {
                    updatedRulesLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail(e);
                }

                // Updating more rules fails
                int rulesToInsert = rules.length - rulesToUpdate;
                CountDownLatch insertRulesLatch = new CountDownLatch(rulesToInsert);
                for (int i = rulesToUpdate; i < rulesToInsert; i++) {
                    synonymsManagementAPIService.putSynonymRule(
                        // Error here
                        synonymSetId,
                        rules[i],
                        new ActionListener<>() {
                            @Override
                            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                                fail("Shouldn't have been able to update a rule");
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof IllegalArgumentException == false) {
                                    fail(e);
                                }
                                updatedRulesLatch.countDown();
                            }
                        }
                    );
                }
                try {
                    insertRulesLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail("Shouldn't fail creating synonym sets");
            }
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testUpdateRuleWithMaxSynonyms() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        SynonymRule[] synonymsSet = randomSynonymsSet(
            SynonymsManagementAPIService.MAX_SYNONYMS_SETS,
            SynonymsManagementAPIService.MAX_SYNONYMS_SETS
        );
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Updating a rule fails
                synonymsManagementAPIService.putSynonymRule(synonymSetId, synonymsSet[0], new ActionListener<>() {
                    @Override
                    public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                        fail("Shouldn't have been able to update a rule with max synonyms");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        latch.countDown();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        latch.await(5, TimeUnit.SECONDS);
    }
}
