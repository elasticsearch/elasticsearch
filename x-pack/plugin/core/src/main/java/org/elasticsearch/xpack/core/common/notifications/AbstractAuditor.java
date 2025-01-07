/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractAuditor<T extends AbstractAuditMessage> {

    // The special ID that means the message applies to all jobs/resources.
    public static final String All_RESOURCES_ID = "";

    private static final Logger logger = LogManager.getLogger(AbstractAuditor.class);
    static final int MAX_BUFFER_SIZE = 1000;
    protected static final TimeValue MASTER_TIMEOUT = TimeValue.timeValueMinutes(1);

    private final OriginSettingClient client;
    private final String nodeName;
    private final String auditIndexWriteAlias;
    private final AbstractAuditMessageFactory<T> messageFactory;
    private final AtomicBoolean indexAndAliasCreated;

    private Queue<ToXContent> backlog;
    private final Consumer<ActionListener<Boolean>> createIndexAndAliasAction;
    private final AtomicBoolean indexAndAliasCreationInProgress;

    protected AbstractAuditor(
        OriginSettingClient client,
        String auditIndexWriteAlias,
        String nodeName,
        AbstractAuditMessageFactory<T> messageFactory,
        Consumer<ActionListener<Boolean>> createIndexAndAliasAction
    ) {
        this.client = Objects.requireNonNull(client);
        this.auditIndexWriteAlias = Objects.requireNonNull(auditIndexWriteAlias);
        this.messageFactory = Objects.requireNonNull(messageFactory);
        this.nodeName = Objects.requireNonNull(nodeName);
        this.createIndexAndAliasAction = Objects.requireNonNull(createIndexAndAliasAction);
        this.backlog = new ConcurrentLinkedQueue<>();
        this.indexAndAliasCreated = new AtomicBoolean();
        this.indexAndAliasCreationInProgress = new AtomicBoolean();
    }

    public void audit(Level level, String resourceId, String message) {
        indexDoc(messageFactory.newMessage(resourceId, message, level, new Date(), nodeName));
    }

    public void info(String resourceId, String message) {
        audit(Level.INFO, resourceId, message);
    }

    public void warning(String resourceId, String message) {
        audit(Level.WARNING, resourceId, message);
    }

    public void error(String resourceId, String message) {
        audit(Level.ERROR, resourceId, message);
    }

    private static void onIndexResponse(DocWriteResponse response) {
        logger.trace("Successfully wrote audit message");
    }

    private static void onIndexFailure(Exception exception) {
        logger.error("Failed to write audit message", exception);
    }

    protected void indexDoc(ToXContent toXContent) {
        if (indexAndAliasCreated.get()) {
            logger.info("has template Indexing audit message");
            writeDoc(toXContent);
            return;
        }

        // install template & create index with alias
        var createListener = ActionListener.<Boolean>wrap(success -> {
            indexAndAliasCreationInProgress.set(false);
            synchronized (this) {
                // synchronized so nothing can be added to backlog while this value changes
                indexAndAliasCreated.set(true);
                writeBacklog();
            }

        }, e -> { indexAndAliasCreationInProgress.set(false); });

        synchronized (this) {
            if (indexAndAliasCreated.get() == false) {
                // synchronized so that hasLatestTemplate does not change value
                // between the read and adding to the backlog
                assert backlog != null;
                if (backlog != null) {
                    if (backlog.size() >= MAX_BUFFER_SIZE) {
                        backlog.remove();
                    }
                    backlog.add(toXContent);
                } else {
                    logger.error("Latest audit template missing and audit message cannot be added to the backlog");
                }

                // stop multiple invocations
                if (indexAndAliasCreationInProgress.compareAndSet(false, true)) {
                    this.createIndexAndAliasAction.accept(createListener);
                }
                return;
            }
        }
    }

    private void writeDoc(ToXContent toXContent) {
        client.index(indexRequest(toXContent), ActionListener.wrap(AbstractAuditor::onIndexResponse, AbstractAuditor::onIndexFailure));
    }

    private IndexRequest indexRequest(ToXContent toXContent) {
        IndexRequest indexRequest = new IndexRequest(auditIndexWriteAlias);
        indexRequest.source(toXContentBuilder(toXContent));
        indexRequest.timeout(TimeValue.timeValueSeconds(5));
        indexRequest.setRequireAlias(true);
        return indexRequest;
    }

    private static XContentBuilder toXContentBuilder(ToXContent toXContent) {
        try (XContentBuilder jsonBuilder = jsonBuilder()) {
            return toXContent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void clearBacklog() {
        backlog = null;
    }

    protected void writeBacklog() {
        assert backlog != null;
        if (backlog == null) {
            logger.error("Message back log has already been written");
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();
        ToXContent doc = backlog.poll();
        while (doc != null) {
            bulkRequest.add(indexRequest(doc));
            doc = backlog.poll();
        }

        client.bulk(bulkRequest, ActionListener.wrap(bulkItemResponses -> {
            if (bulkItemResponses.hasFailures()) {
                logger.warn("Failures bulk indexing the message back log: {}", bulkItemResponses.buildFailureMessage());
            } else {
                logger.trace("Successfully wrote audit message backlog after upgrading template");
            }
            backlog = null;
        }, AbstractAuditor::onIndexFailure));
    }

    // for testing
    int backLogSize() {
        return backlog.size();
    }
}
