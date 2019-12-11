/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.aggregatemetric;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class AggregateMetricInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public AggregateMetricInfoTransportAction(TransportService transportService, ActionFilters actionFilters,
                                              Settings settings, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.AGGREGATE_METRIC.name(), transportService, actionFilters);
    }

    @Override
    public String name() {
        return XPackField.AGGREGATE_METRIC;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

}
