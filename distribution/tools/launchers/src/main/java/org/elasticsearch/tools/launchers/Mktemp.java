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

package org.elasticsearch.tools.launchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Creates a temporary directory as a sub-directory of the default temporary directory. Note that this causes the created temporary
 * directory to be a private temporary directory.
 */
final class Mktemp {

    /**
     * The main entry point. The exit code is 0 if we successfully created a temporary directory as a sub-directory of the default
     * temporary directory and printed the resulting path to the console.
     *
     * @param args the args to the program which should be empty
     * @throws IOException if an I/O exception occurred while creating the temporary directory
     */
    public static void main(final String[] args) throws IOException {
        if (args.length != 0) {
            throw new IllegalArgumentException("expected zero arguments but was " + Arrays.toString(args));
        }
        final Path path = Files.createTempDirectory("elasticsearch-");
        // wrap output in quotes in case the default temporary directory contains spaces
        Launchers.outPrintln("\"" + path.toString() + "\"");
    }

}