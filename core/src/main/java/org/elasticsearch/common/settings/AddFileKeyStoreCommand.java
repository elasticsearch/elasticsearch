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

package org.elasticsearch.common.settings;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli which adds a file setting.
 */
class AddFileKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> forceOption;
    private final OptionSpec<String> arguments;

    AddFileKeyStoreCommand() {
        super("Add a file setting to the keystore");
        this.forceOption = parser.acceptsAll(Arrays.asList("f", "force"), "Overwrite existing setting without prompting");
        // jopt simple has issue with multiple non options, so we just get one set of them here
        // and convert to File when necessary
        // see https://github.com/jopt-simple/jopt-simple/issues/103
        this.arguments = parser.nonOptions("setting [filepath]");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        // check arguments
        final List<String> argumentValues = arguments.values(options);
        if (argumentValues.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing setting name");
        }
        final String settingName = argumentValues.get(0);
        if (argumentValues.size() == 1) {
            throw new UserException(ExitCodes.USAGE, "Missing file name");
        }
        final Path file = getPath(argumentValues.get(1));
        if (Files.exists(file) == false) {
            throw new UserException(ExitCodes.IO_ERROR, "File [" + file.toString() + "] does not exist");
        }
        if (argumentValues.size() > 2) {
            throw new UserException(ExitCodes.USAGE, "Unrecognized extra arguments ["
                    + String.join(", ", argumentValues.subList(2, argumentValues.size())) + "] after filepath");
        }
        // load or create keystore
        Optional<KeyStoreWrapper> keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore.isPresent() == false) {
            if (options.has(forceOption) == false &&
                terminal.promptYesNo("The elasticsearch keystore does not exist. Do you want to create it?", false) == false) {
                terminal.println("Exiting without creating keystore.");
                return;
            }
            // create and save empty keystore
            try (KeyStoreWrapper.Builder builder = KeyStoreWrapper.builder(new char[0])) {
                builder.save(env.configFile());
            }
            terminal.println("Created elasticsearch keystore in " + env.configFile());
            keystore = KeyStoreWrapper.load(env.configFile());
        }
        assert keystore.isPresent();
        /* TODO: prompt for password when they are supported */
        try (AutoCloseable ignored = keystore.get().unlock(new char[0])) {
            if (keystore.get().getSettingNames().contains(settingName) && options.has(forceOption) == false) {
                if (terminal.promptYesNo("Setting " + settingName + " already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without modifying keystore.");
                    return;
                }
            }
            try (KeyStoreWrapper.Builder builder = KeyStoreWrapper.builder(keystore.get())) {
                builder.setFile(settingName, Files.readAllBytes(file)).save(env.configFile());
            } catch (IllegalArgumentException e) {
                throw new UserException(ExitCodes.DATA_ERROR, "Keystore exception.", e);
            }
        }
    }

    @SuppressForbidden(reason="file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }
}
