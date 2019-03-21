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
package org.elasticsearch.gradle;

import java.io.File;
import java.io.IOException;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

public class EmptyDirTaskTests extends GradleUnitTestCase {

    public void testCreateEmptyDir() throws Exception {
        Project project = createProject();
        EmptyDirTask emptyDirTask = createTask(project);
        assertEquals(0755, emptyDirTask.getDirMode());

        // generate a new temporary folder and make sure it does not exists
        File newEmptyFolder = getNewNonExistingTempFolderFile(project);

        emptyDirTask.setDir(newEmptyFolder);
        emptyDirTask.create();

        assertTrue(newEmptyFolder.exists());
        assertTrue(newEmptyFolder.isDirectory());
        assertTrue(newEmptyFolder.canExecute());
        assertTrue(newEmptyFolder.canRead());
        assertTrue(newEmptyFolder.canWrite());

        // cleanup
        newEmptyFolder.delete();
    }

    public void testCreateEmptyDirNoPermissions() throws Exception {
        Project project = createProject();
        EmptyDirTask emptyDirTask = createTask(project);
        emptyDirTask.setDirMode(0000);

        // generate a new temporary folder and make sure it does not exists
        File newEmptyFolder = getNewNonExistingTempFolderFile(project);

        emptyDirTask.setDir(newEmptyFolder);
        emptyDirTask.create();

        assertTrue(newEmptyFolder.exists());
        assertTrue(newEmptyFolder.isDirectory());
        assertFalse(newEmptyFolder.canExecute());
        assertFalse(newEmptyFolder.canRead());
        assertFalse(newEmptyFolder.canWrite());

        newEmptyFolder.delete();
    }

    private File getNewNonExistingTempFolderFile(Project project) throws IOException {
        File newEmptyFolder = File.createTempFile("empty",".dir", project.getBuildDir());
        assertFalse(newEmptyFolder.exists());
        return newEmptyFolder;
    }

    private Project createProject() throws IOException {
        return ProjectBuilder.builder().build();
    }

    private EmptyDirTask createTask(Project project) {
        return project.getTasks().create("emptyDirTask", EmptyDirTask.class);
    }
}
