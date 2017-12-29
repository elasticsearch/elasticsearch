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

package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginInfo implements Writeable, ToXContentObject {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String ES_PLUGIN_POLICY = "plugin-security.policy";

    private final String uberPlugin;
    private final String name;
    private final String description;
    private final String version;
    private final String classname;
    private final boolean hasNativeController;
    private final boolean requiresKeystore;

    /**
     * Construct plugin info.
     *
     * @param uberPlugin          the name of the uber plugin or null if this plugin is a standalone plugin
     * @param name                the name of the plugin
     * @param description         a description of the plugin
     * @param version             the version of Elasticsearch the plugin is built for
     * @param classname           the entry point to the plugin
     * @param hasNativeController whether or not the plugin has a native controller
     * @param requiresKeystore    whether or not the plugin requires the elasticsearch keystore to be created
     */
    public PluginInfo(@Nullable String uberPlugin, String name, String description, String version, String classname,
                      boolean hasNativeController, boolean requiresKeystore) {
        this.uberPlugin = uberPlugin;
        this.name = name;
        this.description = description;
        this.version = version;
        this.classname = classname;
        this.hasNativeController = hasNativeController;
        this.requiresKeystore = requiresKeystore;
    }

    /**
     * Construct plugin info from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    public PluginInfo(final StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            this.uberPlugin = in.readOptionalString();
        } else {
            this.uberPlugin = null;
        }
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        this.classname = in.readString();
        if (in.getVersion().onOrAfter(Version.V_5_4_0)) {
            hasNativeController = in.readBoolean();
        } else {
            hasNativeController = false;
        }
        if (in.getVersion().onOrAfter(Version.V_6_0_0_beta2)) {
            requiresKeystore = in.readBoolean();
        } else {
            requiresKeystore = false;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeOptionalString(uberPlugin);
        }
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        out.writeString(classname);
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            out.writeBoolean(hasNativeController);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta2)) {
            out.writeBoolean(requiresKeystore);
        }
    }

    /**
     * Extracts all {@link PluginInfo} from the provided {@code rootPath} expanding uber-plugins if needed.
     * @param rootPath the path where the plugins are installed
     * @return A list of all plugins installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the plugin descriptors
     */
    public static List<PluginInfo> extractAllPlugins(final Path rootPath) throws IOException {
        return extractAllPlugins(rootPath, Collections.emptySet());
    }

    /**
     * Extracts all {@link PluginInfo} from the provided {@code rootPath} expanding uber-plugins if needed.
     * @param rootPath the path where the plugins are installed
     * @param excludePlugins the set of plugins names to exclude
     * @return A list of all plugins installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the plugin descriptors
     */
    public static List<PluginInfo> extractAllPlugins(final Path rootPath, final Set<String> excludePlugins) throws IOException {
        final List<PluginInfo> plugins = new LinkedList<>(); // order is already lost, but some filesystems have it
        final Set<String> seen = new HashSet<>();
        if (Files.exists(rootPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
                for (Path plugin : stream) {
                    if (FileSystemUtils.isDesktopServicesStore(plugin)) {
                        continue;
                    }
                    if (excludePlugins.contains(plugin.getFileName().toString())) {
                        continue;
                    }
                    if (UberPluginInfo.isUberPlugin(plugin)) {
                        final UberPluginInfo uberInfo;
                        try {
                            uberInfo = UberPluginInfo.readFromProperties(plugin);
                        } catch (IOException e) {
                            throw new IllegalStateException("Could not load uber plugin descriptor for existing uber plugin ["
                                + plugin.getFileName() + "].", e);
                        }
                        Set<String> subPlugins = Arrays.stream(uberInfo.getPlugins()).collect(Collectors.toSet());
                        try (DirectoryStream<Path> subStream = Files.newDirectoryStream(plugin)) {
                            for (Path subPlugin : subStream) {
                                String filename = subPlugin.getFileName().toString();
                                if (UberPluginInfo.ES_UBER_PLUGIN_PROPERTIES.equals(filename) ||
                                        FileSystemUtils.isDesktopServicesStore(subPlugin)) {
                                    continue;
                                }
                                if (subPlugins.contains(filename) == false) {
                                    throw new IllegalStateException(
                                        "invalid plugin: " + subPlugin + " for uber-plugin: " + uberInfo.getName());
                                }
                                final PluginInfo info = PluginInfo.readFromProperties(uberInfo.getName(), subPlugin);
                                if (seen.add(info.getName()) == false) {
                                    throw new IllegalStateException("duplicate plugin: " + info.getName());
                                }
                                plugins.add(info);
                            }
                        }
                    } else {
                        final PluginInfo info;
                        try {
                            info = PluginInfo.readFromProperties(plugin);
                        } catch (IOException e) {
                            throw new IllegalStateException("Could not load plugin descriptor for existing plugin ["
                                + plugin.getFileName() + "]. Was the plugin built before 2.0?", e);
                        }
                        if (seen.add(info.getName()) == false) {
                            throw new IllegalStateException("duplicate plugin: " + plugin);
                        }
                        plugins.add(info);
                    }
                }
            }
        }
        return plugins;
    }

    /**
     * Reads and validates the plugin descriptor file.
     *
     * @param path the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
        return readFromProperties(null, path);
    }

    /**
     * Reads and validates the plugin descriptor file.
     *
     * @param uberPlugin the name of the uber plugin or null if this plugin is a standalone plugin
     * @param path the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(@Nullable final String uberPlugin, final Path path) throws IOException {
        final Path descriptor = path.resolve(ES_PLUGIN_PROPERTIES);

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        final String name = propsMap.remove("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }
        final String description = propsMap.remove("description");
        if (description == null) {
            throw new IllegalArgumentException(
                    "property [description] is missing for plugin [" + name + "]");
        }
        final String version = propsMap.remove("version");
        if (version == null) {
            throw new IllegalArgumentException(
                    "property [version] is missing for plugin [" + name + "]");
        }

        final String esVersionString = propsMap.remove("elasticsearch.version");
        if (esVersionString == null) {
            throw new IllegalArgumentException(
                    "property [elasticsearch.version] is missing for plugin [" + name + "]");
        }
        final Version esVersion = Version.fromString(esVersionString);
        if (esVersion.equals(Version.CURRENT) == false) {
            final String message = String.format(
                    Locale.ROOT,
                    "plugin [%s] is incompatible with version [%s]; was designed for version [%s]",
                    name,
                    Version.CURRENT.toString(),
                    esVersionString);
            throw new IllegalArgumentException(message);
        }
        final String javaVersionString = propsMap.remove("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException(
                    "property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        JarHell.checkJavaVersion(name, javaVersionString);
        final String classname = propsMap.remove("classname");
        if (classname == null) {
            throw new IllegalArgumentException(
                    "property [classname] is missing for plugin [" + name + "]");
        }

        final String hasNativeControllerValue = propsMap.remove("has.native.controller");
        final boolean hasNativeController;
        if (hasNativeControllerValue == null) {
            hasNativeController = false;
        } else {
            switch (hasNativeControllerValue) {
                case "true":
                    hasNativeController = true;
                    break;
                case "false":
                    hasNativeController = false;
                    break;
                default:
                    final String message = String.format(
                        Locale.ROOT,
                        "property [%s] must be [%s], [%s], or unspecified but was [%s]",
                        "has_native_controller",
                        "true",
                        "false",
                        hasNativeControllerValue);
                    throw new IllegalArgumentException(message);
            }
        }

        String requiresKeystoreValue = propsMap.remove("requires.keystore");
        if (requiresKeystoreValue == null) {
            requiresKeystoreValue = "false";
        }
        final boolean requiresKeystore;
        try {
            requiresKeystore = Booleans.parseBoolean(requiresKeystoreValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("property [requires.keystore] must be [true] or [false]," +
                                               " but was [" + requiresKeystoreValue + "]", e);
        }

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties in plugin descriptor: " + propsMap.keySet());
        }

        return new PluginInfo(uberPlugin, name, description, version, classname, hasNativeController, requiresKeystore);
    }

    /**
     * Resolves {@code rootPath} to the path where the plugin should be installed
     * @param rootPath The root path for all plugins
     * @return The path of this plugin
     */
    public Path getPath(Path rootPath) {
        return uberPlugin != null ? rootPath.resolve(uberPlugin).resolve(name) : rootPath.resolve(name);
    }

    /**
     * The name of the uber-plugin that installed this plugin or null if this plugin is a standalone plugin.
     *
     * @return The name of the uber-plugin
     */
    public String getUberPlugin() {
        return uberPlugin;
    }

    /**
     * The name of the plugin.
     *
     * @return the plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * The name of the plugin prefixed with the {@code uberPlugin} name.
     * @return the full name of the plugin
     */
    public String getFullName() {
        return (uberPlugin != null ? uberPlugin + ":" : "")  + name;
    }

    /**
     * The description of the plugin.
     *
     * @return the plugin description
     */
    public String getDescription() {
        return description;
    }

    /**
     * The entry point to the plugin.
     *
     * @return the entry point to the plugin
     */
    public String getClassname() {
        return classname;
    }

    /**
     * The version of Elasticsearch the plugin was built for.
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Whether or not the plugin has a native controller.
     *
     * @return {@code true} if the plugin has a native controller
     */
    public boolean hasNativeController() {
        return hasNativeController;
    }

    /**
     * Whether or not the plugin requires the elasticsearch keystore to exist.
     *
     * @return {@code true} if the plugin requires a keystore, {@code false} otherwise
     */
    public boolean requiresKeystore() {
        return requiresKeystore;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (uberPlugin != null) {
                builder.field("uber_plugin", uberPlugin);
            }
            builder.field("name", name);
            builder.field("version", version);
            builder.field("description", description);
            builder.field("classname", classname);
            builder.field("has_native_controller", hasNativeController);
            builder.field("requires_keystore", requiresKeystore);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        if (!name.equals(that.name)) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder information = new StringBuilder().append("- Plugin information:\n");
        if (uberPlugin != null) {
            information.append("Uber Plugin: ").append(uberPlugin).append("\n");
        }
        information
            .append("Name: ").append(name).append("\n")
            .append("Description: ").append(description).append("\n")
            .append("Version: ").append(version).append("\n")
            .append("Native Controller: ").append(hasNativeController).append("\n")
            .append("Requires Keystore: ").append(requiresKeystore).append("\n")
            .append(" * Classname: ").append(classname);
        return information.toString();
    }
}
