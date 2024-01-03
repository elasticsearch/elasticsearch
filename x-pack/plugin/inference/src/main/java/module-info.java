/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.inference {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    requires org.apache.httpcomponents.httpclient;
    requires org.apache.logging.log4j;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.lucene.core;

    exports org.elasticsearch.xpack.inference.common;
    exports org.elasticsearch.xpack.inference.action;
    exports org.elasticsearch.xpack.inference.registry;
    exports org.elasticsearch.xpack.inference.rest;
    exports org.elasticsearch.xpack.inference.services;
    exports org.elasticsearch.xpack.inference.external.http;
    exports org.elasticsearch.xpack.inference.external.http.sender;
    exports org.elasticsearch.xpack.inference.external.http.retry;
    exports org.elasticsearch.xpack.inference.external.http.batching;
    exports org.elasticsearch.xpack.inference.external.huggingface;
    exports org.elasticsearch.xpack.inference.external.openai;
    exports org.elasticsearch.xpack.inference.services.elser;
    exports org.elasticsearch.xpack.inference.services.huggingface.elser;
    exports org.elasticsearch.xpack.inference.services.openai;
    exports org.elasticsearch.xpack.inference;
}
