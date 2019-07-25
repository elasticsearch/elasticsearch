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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskListener;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class keeps track of which tasks came in from which {@link HttpChannel}, by allowing to associate
 * an {@link HttpChannel} with a {@link TaskId}, and also removing the link once the task is complete.
 * Additionally, it accepts a consumer that gets called whenever an http channel gets closed, which
 * can be used to cancel the associated task when the underlying connection gets closed.
 */
final class HttpChannelTaskHandler {
    final Map<HttpChannel, CloseListener> httpChannels = new ConcurrentHashMap<>();

    <Response extends ActionResponse> void execute(NodeClient client, HttpChannel httpChannel, ActionRequest request,
                                                   ActionType<Response> actionType, ActionListener<Response> listener) {

        AtomicBoolean linkEnabled = new AtomicBoolean(true);
        Task task = client.executeLocally(actionType, request,
            new TaskListener<>() {
                @Override
                public void onResponse(Task task, Response searchResponse) {
                    unlink(task);
                    listener.onResponse(searchResponse);
                }

                @Override
                public void onFailure(Task task, Throwable e) {
                    unlink(task);
                    if (e instanceof Exception) {
                        listener.onFailure((Exception)e);
                    } else {
                        //TODO should we rather throw in case of throwable instead of notifying the listener?
                        listener.onFailure(new RuntimeException(e));
                    }
                }

                private void unlink(Task task) {
                    //the synchronized blocks are to make sure that only link or unlink for a specific task can happen at a given time,
                    //they can't happen concurrently. The flag is needed because unlink can still be called before link, which would
                    //lead to piling up task ids that are never removed from the map.
                    //It may look like only either synchronized or the boolean flag are needed but they both are. In fact, the boolean flag
                    //is needed to ensure that we don't link a task if we have already unlinked it. But it's not enough as, once we start
                    //the linking, we do want its corresponding unlinking to happen, but only once the linking is completed. With only
                    //the boolean flag, we would just miss unlinking for some tasks that are being linked when onResponse is called.
                    synchronized(task) {
                        try {
                            //nothing to do if link was not called yet: it would not find the task anyways.
                            if (linkEnabled.compareAndSet(false, true)) {
                                CloseListener closeListener = httpChannels.get(httpChannel);
                                TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
                                closeListener.unregisterTask(taskId);
                            }
                        } catch(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }
            });

        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, channel -> new CloseListener(client));
        synchronized (task) {
            //make sure that the link is made only if the task is not already completed, otherwise unlink would have already been called
            if (linkEnabled.compareAndSet(true, false)) {
                TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
                closeListener.registerTask(httpChannel, taskId);
            }
        }

        //TODO test case where listener is registered, but no tasks have been added yet:
        // - connection gets closed, channel will be removed, no tasks will be cancelled
        // - unlink is called before, hence the task is removed (not found) before it gets added

        //TODO check that no tasks are left behind through assertions at node close
    }

    final class CloseListener implements ActionListener<Void> {
        private final Client client;
        final Set<TaskId> taskIds = new CopyOnWriteArraySet<>();
        private final AtomicReference<HttpChannel> channel = new AtomicReference<>();

        CloseListener(Client client) {
            this.client = client;
        }

        void registerTask(HttpChannel httpChannel, TaskId taskId) {
            if (channel.compareAndSet(null, httpChannel)) {
                //In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                //remove the channel from the map straight-away. That is why we do this in two stages. If we provided the channel at close
                //listener initialization we would have to deal with close listeners calls before the channel is in the map.
                httpChannel.addCloseListener(this);
            }
            this.taskIds.add(taskId);
        }

        private void unregisterTask(TaskId taskId) {
            this.taskIds.remove(taskId);
        }

        @Override
        public void onResponse(Void aVoid) {
            //When the channel gets closed it won't be reused: we can remove it from the map as there is no chance we will
            //register another close listener to it later.
            //The channel reference may be null, if the connection gets closed before we set it.
            //The channel must be found in the map though as this listener gets registered after the channel is added.
            //TODO test channel null here? it can happen!
            httpChannels.remove(channel.get());
            for (TaskId previousTaskId : taskIds) {
                CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                cancelTasksRequest.setTaskId(previousTaskId);
                //We don't wait for cancel tasks to come back. Task cancellation is just best effort.
                //Note that cancel tasks fails if the user sending the search request does not have the permissions to call it.
                client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(r -> {}, e -> {}));
            }
        }

        @Override
        public void onFailure(Exception e) {
            //nothing to do here
        }
    }
}
