/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public class AliasValidator extends AbstractComponent {

    @Inject
    public AliasValidator(Settings settings) {
        super(settings);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAlias(Alias alias, String index, MetaData metaData) {
        validateAlias(alias.name(), index, alias.indexRouting(), name -> metaData.index(name));
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasMetaData} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasMetaData(AliasMetaData aliasMetaData, String index, MetaData metaData) {
        validateAlias(aliasMetaData.alias(), index, aliasMetaData.indexRouting(), name -> metaData.index(name));
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.elasticsearch.common.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasStandalone(Alias alias) {
        validateAliasStandalone(alias.name(), alias.indexRouting());
        if (Strings.hasLength(alias.filter())) {
            try {
                XContentHelper.convertToMap(XContentFactory.xContent(alias.filter()), alias.filter(), false);
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to parse filter for alias [" + alias.name() + "]", e);
            }
        }
    }

    /**
     * Validate a proposed alias.
     */
    public void validateAlias(String alias, String index, @Nullable String indexRouting, Function<String, IndexMetaData> indexLookup) {
        validateAliasStandalone(alias, indexRouting);

        if (!Strings.hasText(index)) {
            throw new IllegalArgumentException("index name is required");
        }

        IndexMetaData indexNamedSameAsAlias = indexLookup.apply(alias);
        if (indexNamedSameAsAlias != null) {
            throw new InvalidAliasNameException(indexNamedSameAsAlias.getIndex(), alias, "an index exists with the same name as the alias");
        }
    }

    void validateAliasStandalone(String alias, String indexRouting) {
        if (!Strings.hasText(alias)) {
            throw new IllegalArgumentException("alias name is required");
        }
        MetaDataCreateIndexService.validateIndexOrAliasName(alias, InvalidAliasNameException::new);
        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new IllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.QueryShardContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, String filter, QueryShardContext queryShardContext,
            NamedXContentRegistry xContentRegistry) {
        assert queryShardContext != null;
        try (XContentParser parser = XContentFactory.xContent(filter)
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter)) {
            validateAliasFilter(parser, queryShardContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.QueryShardContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, byte[] filter, QueryShardContext queryShardContext,
            NamedXContentRegistry xContentRegistry) {
        assert queryShardContext != null;
        try (XContentParser parser = XContentFactory.xContent(filter)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter)) {
            validateAliasFilter(parser, queryShardContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    private static void validateAliasFilter(XContentParser parser, QueryShardContext queryShardContext) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
        QueryBuilder queryBuilder = Rewriteable.rewrite(parseInnerQueryBuilder, queryShardContext, true);
        queryBuilder.toFilter(queryShardContext);
    }

    /**
     * throws exception if specified alias is attempting to be write_only while this alias points to another index
     *
     * @param aliasName the alias in question
     * @param metaData the Cluster metadata
     * @throws IllegalArgumentException if the alias cannot be write_only
     */
    public void validateAliasWriteOnly(String aliasName, boolean isWriteIndex, MetaData metaData) {
        if (isWriteIndex) {
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(aliasName);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                List<IndexMetaData> writeIndices = alias.getWriteIndices();
                if (writeIndices.size() >= 1) {
                    throw new IllegalArgumentException("Alias [" + aliasName +
                        "] already has a write index set [" + writeIndices.get(0).getIndex().getName() + "]");
                }
            }
        }
    }

    public void validateAliasWriteOnly(String checkAlias, String checkIndex, boolean isWriteIndex, ImmutableOpenMap.Builder<String, IndexMetaData> indices) {
        SortedMap<String, AliasOrIndex> aliasAndIndexLookup = buildAliasAndIndexLookup(indices);
        if (isWriteIndex) {
            AliasOrIndex aliasOrIndex = aliasAndIndexLookup.get(checkAlias);
            if (aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                List<IndexMetaData> writeIndices = alias.getWriteIndices().stream()
                    .filter(i -> i.getIndex().getName().equals(checkIndex) == false).collect(Collectors.toList());
                if (writeIndices.size() >= 1) {
                    throw new IllegalArgumentException("Alias [" + checkAlias +
                        "] already has a write index set [" + writeIndices.get(0).getIndex().getName() + "]");
                }
            }
        }
    }

    public static SortedMap<String, AliasOrIndex> buildAliasAndIndexLookup(ImmutableOpenMap.Builder<String, IndexMetaData> indices) {
        SortedMap<String, AliasOrIndex> aliasAndIndexLookup = new TreeMap<>();
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            AliasOrIndex existing = aliasAndIndexLookup.put(indexMetaData.getIndex().getName(), new AliasOrIndex.Index(indexMetaData));
            assert existing == null : "duplicate for " + indexMetaData.getIndex();

            for (ObjectObjectCursor<String, AliasMetaData> aliasCursor : indexMetaData.getAliases()) {
                AliasMetaData aliasMetaData = aliasCursor.value;
                aliasAndIndexLookup.compute(aliasMetaData.getAlias(), (aliasName, alias) -> {
                    if (alias == null) {
                        return new AliasOrIndex.Alias(aliasMetaData, indexMetaData);
                    } else {
                        assert alias instanceof AliasOrIndex.Alias : alias.getClass().getName();
                        ((AliasOrIndex.Alias) alias).addIndex(indexMetaData);
                        return alias;
                    }
                });
            }
        }
        return aliasAndIndexLookup;
    }

    public static boolean validAliasWriteOnly(String aliasName, boolean isWriteIndex, ImmutableOpenMap.Builder<String, IndexMetaData> indices) {
        SortedMap<String, AliasOrIndex> aliasAndIndexLookup = buildAliasAndIndexLookup(indices);
        if (isWriteIndex) {
            AliasOrIndex aliasOrIndex = aliasAndIndexLookup.get(aliasName);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                List<IndexMetaData> writeIndices = alias.getWriteIndices();
                return writeIndices.isEmpty();
            }
        }
        return true;
    }

    public static boolean validAliasWriteOnly(String aliasName, boolean isWriteIndex, ImmutableOpenMap<String, IndexMetaData> indices) {
        return validAliasWriteOnly(aliasName, isWriteIndex, ImmutableOpenMap.builder(indices));
    }
}
