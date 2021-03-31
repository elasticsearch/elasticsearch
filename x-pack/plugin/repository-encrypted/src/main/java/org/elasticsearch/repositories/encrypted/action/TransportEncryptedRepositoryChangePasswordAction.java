/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.encrypted.EncryptedRepository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.repositories.encrypted.EncryptedRepositoryChangePasswordRequest;
import org.elasticsearch.xpack.core.repositories.encrypted.EncryptedRepositoryChangePasswordResponse;
import org.elasticsearch.xpack.core.repositories.encrypted.action.ChangeEncryptedRepositoryPasswordAction;

public final class TransportEncryptedRepositoryChangePasswordAction extends TransportMasterNodeAction<
    EncryptedRepositoryChangePasswordRequest,
    EncryptedRepositoryChangePasswordResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportEncryptedRepositoryChangePasswordAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ChangeEncryptedRepositoryPasswordAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            EncryptedRepositoryChangePasswordRequest::new,
            indexNameExpressionResolver,
            EncryptedRepositoryChangePasswordResponse::new,
            ThreadPool.Names.GENERIC
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void masterOperation(
        Task task,
        EncryptedRepositoryChangePasswordRequest request,
        ClusterState state,
        ActionListener<EncryptedRepositoryChangePasswordResponse> listener
    ) throws Exception {
        Repository repository = repositoriesService.repository(request.repositoryName());
        if (false == (repository instanceof EncryptedRepository)) {
            listener.onFailure(new IllegalArgumentException("Repository [" + request.repositoryName() + "] is not encrypted"));
            return;
        }
        EncryptedRepository encryptedRepository = (EncryptedRepository) repository;
        encryptedRepository.startOrResumePasswordChange(request.fromPasswordName(), request.toPasswordName(), ActionListener.wrap(aVoid -> {
            // TODO
            // move all existing DEKs to use the new password and the new name
            // update cluster state to decommission the old password
            // remove the now old DEKs
            // update cluster state to conclude the password change
        }, listener::onFailure));
        listener.onResponse(new EncryptedRepositoryChangePasswordResponse(false));
    }

    @Override
    protected ClusterBlockException checkBlock(EncryptedRepositoryChangePasswordRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
