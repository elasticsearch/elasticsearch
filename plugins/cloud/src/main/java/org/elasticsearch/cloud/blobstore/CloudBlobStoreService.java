/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cloud.blobstore;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cloud.jclouds.JCloudsUtils;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.settings.SettingsFilter;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class CloudBlobStoreService extends AbstractLifecycleComponent<CloudBlobStoreService> {

    private final String type;

    private final BlobStoreContext blobStoreContext;

    @Inject public CloudBlobStoreService(Settings settings, SettingsFilter settingsFilter) throws IOException {
        super(settings);

        String type = componentSettings.get("type");
        if (type == null) {
            // see if we can get a global type
            type = settings.get("cloud.type");
        }
        // consolidate names
        if ("aws".equalsIgnoreCase(type) || "amazon".equalsIgnoreCase(type)) {
            type = "s3";
        } else if ("rackspace".equalsIgnoreCase(type)) {
            type = "cloudfiles";
        }
        this.type = type;

        String account = componentSettings.get("account", settings.get("cloud.account"));
        String key = componentSettings.get("key", settings.get("cloud.key"));

        if (type != null) {
            blobStoreContext = new BlobStoreContextFactory().createContext(type, account, key, JCloudsUtils.buildModules(settings));
            logger.info("Connected to {}/{} blob store service", type, account);
        } else {
            blobStoreContext = null;
        }

        settingsFilter.addFilter(new BlobStorSettingsFilter());
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
        if (blobStoreContext != null) {
            blobStoreContext.close();
        }
    }

    public BlobStoreContext context() {
        if (blobStoreContext == null) {
            throw new ElasticSearchIllegalStateException("No cloud blobstore service started, have you configured the 'cloud.type' setting?");
        }
        return blobStoreContext;
    }


    private static class BlobStorSettingsFilter implements SettingsFilter.Filter {
        @Override public void filter(ImmutableSettings.Builder settings) {
            settings.remove("cloud.key");
            settings.remove("cloud.account");
            settings.remove("cloud.blobstore.key");
            settings.remove("cloud.blobstore.account");
        }
    }
}
