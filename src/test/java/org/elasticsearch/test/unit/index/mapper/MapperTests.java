/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.mapper;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.source.SourceProviderModule;
import org.elasticsearch.index.source.SourceProviderService;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

/**
 *
 */
public class MapperTests {

    public static DocumentMapperParser newParser() {
        return new DocumentMapperParser(new Index("test"), newAnalysisService(), newSourceProviderService());
    }

    public static DocumentMapperParser newParser(Settings indexSettings) {
        return new DocumentMapperParser(new Index("test"), indexSettings, newAnalysisService(indexSettings), newSourceProviderService(indexSettings));
    }

    public static MapperService newMapperService() {
        return new MapperService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS, new Environment(),
                newAnalysisService(), newSourceProviderService());
    }


    public static AnalysisService newAnalysisService() {
        return newAnalysisService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static AnalysisService newAnalysisService(Settings indexSettings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(ImmutableSettings.Builder.EMPTY_SETTINGS), new EnvironmentModule(new Environment(ImmutableSettings.Builder.EMPTY_SETTINGS)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(new Index("test"), indexSettings),
                new IndexNameModule(new Index("test")),
                new AnalysisModule(indexSettings, parentInjector.getInstance(IndicesAnalysisService.class))).createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }

    public static SourceProviderService newSourceProviderService() {
        return newSourceProviderService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static SourceProviderService newSourceProviderService(Settings indexSettings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(ImmutableSettings.Builder.EMPTY_SETTINGS), new EnvironmentModule(new Environment(ImmutableSettings.Builder.EMPTY_SETTINGS))).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(new Index("test"), indexSettings),
                new IndexNameModule(new Index("test")),
                new SourceProviderModule(indexSettings)).createChildInjector(parentInjector);

        return injector.getInstance(SourceProviderService.class);
    }
}
