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

package org.elasticsearch.cloud.azure.blobstore.util;

import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class SocketAccess {

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) throws StorageException, URISyntaxException {
        checkSpecialPermission();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (StorageException) e.getCause();
        }
    }

    public static void doPrivilegedVoidException(VoidOpException action) throws StorageException, URISyntaxException {
        checkSpecialPermission();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.execute();
                return null;
            });
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StorageException) {
                throw (StorageException) cause;
            } else {
                throw (URISyntaxException) cause;
            }
        }
    }

    private static void checkSpecialPermission() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
    }

    @FunctionalInterface
    public interface VoidOpException {
        void execute() throws StorageException, URISyntaxException;
    }

}
