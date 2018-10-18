/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.BucketAggregationSelectorScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.StringSortScript;

import static java.util.Collections.singletonList;

public class SqlPainlessExtension implements PainlessExtension {

    private static final Whitelist WHITELIST = WhitelistLoader.loadFromResourceFiles(SqlPainlessExtension.class, "sql_whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> whitelist = new HashMap<>();
        List<Whitelist> list = singletonList(WHITELIST);
        whitelist.put(FilterScript.CONTEXT, list);
        whitelist.put(AggregationScript.CONTEXT, list);
        whitelist.put(SearchScript.CONTEXT, list);
        whitelist.put(NumberSortScript.CONTEXT, list);
        whitelist.put(StringSortScript.CONTEXT, list);
        whitelist.put(BucketAggregationSelectorScript.CONTEXT, list);
        return whitelist;
    }
}
