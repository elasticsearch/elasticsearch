/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.component;

public abstract class LifecycleListener {

    public void beforeStart() {

    }

    public void afterStart() {

    }

    public void beforeStop() {

    }

    public void afterStop() {

    }

    public void beforeClose() {

    }

    public void afterClose() {

    }
}
