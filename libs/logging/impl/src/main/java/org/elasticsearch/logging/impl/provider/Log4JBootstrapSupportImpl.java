/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.ConfigurationException;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.composite.CompositeConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAliases;
import org.apache.logging.log4j.core.config.plugins.processor.PluginEntry;
import org.apache.logging.log4j.core.config.plugins.util.PluginRegistry;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.impl.ClusterIdConverter;
import org.elasticsearch.logging.impl.CustomMapFieldsConverter;
import org.elasticsearch.logging.impl.ECSJsonLayout;
import org.elasticsearch.logging.impl.ESJsonLayout;
import org.elasticsearch.logging.impl.HeaderWarningAppenderImpl;
import org.elasticsearch.logging.impl.JsonThrowablePatternConverter;
import org.elasticsearch.logging.impl.Log4jRateLimitingFilter;
import org.elasticsearch.logging.impl.Loggers;
import org.elasticsearch.logging.impl.LoggingOutputStream;
import org.elasticsearch.logging.impl.NodeAndClusterIdConverter;
import org.elasticsearch.logging.impl.NodeIdConverter;
import org.elasticsearch.logging.impl.NodeNamePatternConverter;
import org.elasticsearch.logging.impl.TraceIdConverter;
import org.elasticsearch.logging.impl.Util;
import org.elasticsearch.logging.spi.LoggingBootstrapSupport;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class Log4JBootstrapSupportImpl implements LoggingBootstrapSupport {
    public Log4JBootstrapSupportImpl() {}


    /*
     * We want to detect situations where we touch logging before the configuration is loaded. If we do this, Log4j will status log an error
     * message at the error level. With this error listener, we can capture if this happens. More broadly, we can detect any error-level
     * status log message which likely indicates that something is broken. The listener is installed immediately on startup, and then when
     * we get around to configuring logging we check that no error-level log messages have been logged by the status logger. If they have we
     * fail startup and any such messages can be seen on the console.
     */
    private static final AtomicBoolean error = new AtomicBoolean();
    private static final StatusListener ERROR_LISTENER = new StatusConsoleListener(Level.ERROR) {
        @Override
        public void log(StatusData data) {
            error.set(true);
            super.log(data);
        }
    };

    /**
     * Registers a listener for status logger errors. This listener should be registered as early as possible to ensure that no errors are
     * logged by the status logger before logging is configured.
     */
    public  void registerErrorListener() {
        error.set(false);
        StatusLogger.getLogger().registerListener(ERROR_LISTENER);
    }

    /**
     * Configure logging without reading a log4j2.properties file, effectively configuring the
     * status logger and all loggers to the console.
     *
     //* @param settings for configuring logger.level and individual loggers
     */
    public  void configureWithoutConfig(
        Optional<org.elasticsearch.logging.Level> defaultLogLevel,
        Map<String, org.elasticsearch.logging.Level> logLevelSettingsMap
    ) {
        Objects.requireNonNull(defaultLogLevel);
        Objects.requireNonNull(logLevelSettingsMap);
        // we initialize the status logger immediately otherwise Log4j will complain when we try to get the context
        configureStatusLogger();
        configureLoggerLevels(defaultLogLevel, logLevelSettingsMap);
    }

    /**
     * Configure logging reading from any log4j2.properties found in the config directory and its
     * subdirectories from the specified environment. Will also configure logging to point the logs
     * directory from the specified environment.
     *
     //* @param environment the environment for reading configs and the logs path
     * @throws IOException   if there is an issue readings any log4j2.properties in the config
     *                       directory
     * @throws RuntimeException if there are no log4j2.properties in the specified configs path
     */
    public  void configure(
        String clusterName,
        String nodeName,
        Optional<org.elasticsearch.logging.Level> defaultLogLevel,
        Map<String, org.elasticsearch.logging.Level> logLevelSettingsMap,
        Path configFile,
        Path logsFile
    ) throws IOException, RuntimeException {
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(nodeName);
        Objects.requireNonNull(logLevelSettingsMap);
        try {
            // we are about to configure logging, check that the status logger did not log any error-level messages
            checkErrorListener();
        } finally {
            // whether or not the error listener check failed we can remove the listener now
            StatusLogger.getLogger().removeListener(ERROR_LISTENER);
        }
        configureImpl(clusterName, nodeName, defaultLogLevel, logLevelSettingsMap, configFile, logsFile);
    }

    /**
     * Load logging plugins so we can have {@code node_name} in the pattern.
     */
    public  void loadLog4jPlugins() { //TODO PG when startup problems look here..


        Set<Class<?>> classes = Set.of(
        ClusterIdConverter.class,
        NodeNamePatternConverter.class,
        CustomMapFieldsConverter.class,
        ECSJsonLayout.class,
        ESJsonLayout.class,
        JsonThrowablePatternConverter.class,
        Log4jRateLimitingFilter.class,
        NodeAndClusterIdConverter.class,
        NodeIdConverter.class,
        TraceIdConverter.class,
        HeaderWarningAppenderImpl.class
        );
        final Map<String, List<PluginType<?>>> newPluginsByCategory = new HashMap<>();
        for (final Class<?> clazz : classes) {
            final Plugin plugin = clazz.getAnnotation(Plugin.class);
            final String categoryLowerCase = plugin.category().toLowerCase();
            List<PluginType<?>> list = newPluginsByCategory.get(categoryLowerCase);
            if (list == null) {
                newPluginsByCategory.put(categoryLowerCase, list = new ArrayList<>());
            }
            final PluginEntry mainEntry = new PluginEntry();
            final String mainElementName = plugin.elementType().equals(
                Plugin.EMPTY) ? plugin.name() : plugin.elementType();
            mainEntry.setKey(plugin.name().toLowerCase());
            mainEntry.setName(plugin.name());
            mainEntry.setCategory(plugin.category());
            mainEntry.setClassName(clazz.getName());
            mainEntry.setPrintable(plugin.printObject());
            mainEntry.setDefer(plugin.deferChildren());
            final PluginType<?> mainType = new PluginType<>(mainEntry, clazz, mainElementName);
            list.add(mainType);
            final PluginAliases pluginAliases = clazz.getAnnotation(PluginAliases.class);
            if (pluginAliases != null) {
                for (final String alias : pluginAliases.value()) {
                    final PluginEntry aliasEntry = new PluginEntry();
                    final String aliasElementName = plugin.elementType().equals(
                        Plugin.EMPTY) ? alias.trim() : plugin.elementType();
                    aliasEntry.setKey(alias.trim().toLowerCase());
                    aliasEntry.setName(plugin.name());
                    aliasEntry.setCategory(plugin.category());
                    aliasEntry.setClassName(clazz.getName());
                    aliasEntry.setPrintable(plugin.printObject());
                    aliasEntry.setDefer(plugin.deferChildren());
                    final PluginType<?> aliasType = new PluginType<>(aliasEntry, clazz, aliasElementName);
                    list.add(aliasType);
                }
            }
        }
        PluginRegistry.getInstance().getPluginsByCategoryByBundleId()
            .put(1L, newPluginsByCategory);
    }

    public static  void initPlugins(String category, Class<?> pluginClass, String elementName, PluginEntry pluginEntry) {
        if(pluginEntry.getKey() == null){
            pluginEntry.setKey(elementName.toLowerCase(Locale.ROOT));
        }
        if(pluginEntry.getName() == null){
            pluginEntry.setName(elementName);
        }
        pluginEntry.setClassName(pluginClass.getCanonicalName());
        pluginEntry.setCategory(category.toLowerCase(Locale.ROOT));
        PluginRegistry.getInstance().getPluginsByCategoryByBundleId()
            .computeIfAbsent(1L,a-> new ConcurrentHashMap<>())
            .computeIfAbsent(category.toLowerCase(Locale.ROOT), c-> new ArrayList<>())
            .add(new PluginType<>(
                pluginEntry,
                pluginClass,
                elementName
            ));
    }

    /**
     * Sets the node name. This is called before logging is configured if the
     * node name is set in elasticsearch.yml. Otherwise it is called as soon
     * as the node id is available.
     */
    public  void setNodeName(String nodeName) {
        NodeNamePatternConverter.setNodeName(nodeName);
    }

    public  void init() {
        // LogConfigurator
        // Tuple<String,String> nodeAndClusterId();
    }

    public  void shutdown() {
        Configurator.shutdown((LoggerContext) LogManager.getContext(false));
    }

    public  final Consumer<LoggingBootstrapSupport.ConsoleAppenderMode> consoleAppender() {
        return mode -> {
            final Logger rootLogger = LogManager.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (maybeConsoleAppender == null) {
                return;
            }
            if (mode == LoggingBootstrapSupport.ConsoleAppenderMode.ENABLE) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            } else {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
        };
    }

    // public static void removeConsoleAppender() {
    // final Logger rootLogger = LogManager.getRootLogger();
    // final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
    // if (maybeConsoleAppender != null) {
    // Loggers.removeAppender(rootLogger, maybeConsoleAppender);
    // }
    // }
    //
    // public static void addConsoleAppender() {
    // final Logger rootLogger = LogManager.getRootLogger();
    // final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
    // if (maybeConsoleAppender != null) {
    // Loggers.addAppender(rootLogger, maybeConsoleAppender);
    // }
    // }

    /* TODO PG private */ public  void checkErrorListener() {
        assert errorListenerIsRegistered() : "expected error listener to be registered";
        if (error.get()) {
            throw new IllegalStateException("status logger logged an error before logging was configured");
        }
    }

    private static boolean errorListenerIsRegistered() {
        return StreamSupport.stream(StatusLogger.getLogger().getListeners().spliterator(), false).anyMatch(l -> l == ERROR_LISTENER);
    }

    private  void configureImpl(
        String clusterName,
        String nodeName,
        Optional<org.elasticsearch.logging.Level> defaultLogLevel,
        Map<String, org.elasticsearch.logging.Level> logLevelSettingsMap,
        Path configsPath,
        Path logsPath
    ) throws IOException, RuntimeException { // TODO PG userException is from cli. maybe we should have an exception in api too..
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(nodeName);
        Objects.requireNonNull(logsPath);

        loadLog4jPlugins();

        setLogConfigurationSystemProperty(logsPath, clusterName, nodeName);
        // we initialize the status logger immediately otherwise Log4j will complain when we try to get the context
        configureStatusLogger();

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);

        final Set<String> locationsWithDeprecatedPatterns = Collections.synchronizedSet(new HashSet<>());
        final List<AbstractConfiguration> configurations = new ArrayList<>();
        /*
         * Subclass the properties configurator to hack the new pattern in
         * place so users don't have to change log4j2.properties in
         * a minor release. In 7.0 we'll remove this and force users to
         * change log4j2.properties. If they don't customize log4j2.properties
         * then they won't have to do anything anyway.
         *
         * Everything in this subclass that isn't marked as a hack is copied
         * from log4j2's source.
         */
        final PropertiesConfigurationFactory factory = new PropertiesConfigurationFactory() {
            @Override
            public PropertiesConfiguration getConfiguration(final LoggerContext loggerContext, final ConfigurationSource source) {
                final Properties properties = new Properties();
                try (InputStream configStream = source.getInputStream()) {
                    properties.load(configStream);
                } catch (final IOException ioe) {
                    throw new ConfigurationException("Unable to load " + source.toString(), ioe);
                }
                // Hack the new pattern into place
                for (String name : properties.stringPropertyNames()) {
                    if (false == name.endsWith(".pattern")) continue;
                    // Null is weird here but we can't do anything with it so ignore it
                    String value = properties.getProperty(name);
                    if (value == null) continue;
                    // Tests don't need to be changed
                    if (value.contains("%test_thread_info")) continue;
                    /*
                     * Patterns without a marker are sufficiently customized
                     * that we don't have an opinion about them.
                     */
                    if (false == value.contains("%marker")) continue;
                    if (false == value.contains("%node_name")) {
                        locationsWithDeprecatedPatterns.add(source.getLocation());
                        properties.setProperty(name, value.replace("%marker", "[%node_name]%marker "));
                    }
                }
                // end hack
                return new PropertiesConfigurationBuilder().setConfigurationSource(source)
                    .setRootProperties(properties)
                    .setLoggerContext(loggerContext)
                    .build();
            }
        };
        final Set<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(configsPath, options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().equals("log4j2.properties")) {
                    configurations.add((PropertiesConfiguration) factory.getConfiguration(context, file.toString(), file.toUri()));
                }
                return FileVisitResult.CONTINUE;
            }
        });

        if (configurations.isEmpty()) {
            throw new RuntimeException(/*ExitCodes.CONFIG, */"no log4j2.properties found; tried [" + configsPath + "] and its subdirectories");
        }

        context.start(new CompositeConfiguration(configurations));

        configureLoggerLevels(defaultLogLevel, logLevelSettingsMap);

        final String deprecatedLocationsString = String.join("\n  ", locationsWithDeprecatedPatterns);
        if (deprecatedLocationsString.length() > 0) {
            LogManager.getLogger(Log4JBootstrapSupportImpl.class)
                .warn(
                    "Some logging configurations have %marker but don't have %node_name. "
                        + "We will automatically add %node_name to the pattern to ease the migration for users who customize "
                        + "log4j2.properties but will stop this behavior in 7.0. You should manually replace `%node_name` with "
                        + "`[%node_name]%marker ` in these locations:\n  {}",
                    deprecatedLocationsString
                );
        }

        // Redirect stdout/stderr to log4j. While we ensure Elasticsearch code does not write to those streams,
        // third party libraries may do that. Note that we do NOT close the streams because other code may have
        // grabbed a handle to the streams and intend to write to it, eg log4j for writing to the console
        System.setOut(new PrintStream(new LoggingOutputStream(LogManager.getLogger("stdout"), Level.INFO), false, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new LoggingOutputStream(LogManager.getLogger("stderr"), Level.WARN), false, StandardCharsets.UTF_8));
    }

    private static void configureStatusLogger() {
        final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();// TODO PG plugin loading
        builder.setStatusLevel(Level.ERROR);
        Configurator.initialize(builder.build());
    }

    /**
     * Configures the logging levels for loggers configured in the specified settings.
     *
     //* @param settings the settings from which logger levels will be extracted
     */
    private static void configureLoggerLevels(
        Optional<org.elasticsearch.logging.Level> defaultLogLevel,
        Map<String, org.elasticsearch.logging.Level> logLevelSettingsMap
    ) {
        defaultLogLevel.ifPresent(level -> Loggers.setLevelImpl(LogManager.getRootLogger(), Util.log4jLevel(level)));
        logLevelSettingsMap.forEach((k, v) -> Loggers.setLevelImpl(LogManager.getLogger(k), Util.log4jLevel(v)));
    }

    /**
     * Set system properties that can be used in configuration files to specify paths and file patterns for log files. We expose three
     * properties here:
     * <ul>
     * <li>
     * {@code es.logs.base_path} the base path containing the log files
     * </li>
     * <li>
     * {@code es.logs.cluster_name} the cluster name, used as the prefix of log filenames in the default configuration
     * </li>
     * <li>
     * {@code es.logs.node_name} the node name, can be used as part of log filenames
     * </li>
     * </ul>
     *
     * @param logsPath the path to the log files
     * @param clusterName the cluster name
     * @param nodeName the node name
     */
    @SuppressForbidden(reason = "sets system property for logging configuration")
    private static void setLogConfigurationSystemProperty(final Path logsPath, final String clusterName, final String nodeName) {
        System.setProperty("es.logs.base_path", logsPath.toString());
        System.setProperty("es.logs.cluster_name", clusterName); // ClusterName.CLUSTER_NAME_SETTING.get(settings).value());
        System.setProperty("es.logs.node_name", nodeName); // Node.NODE_NAME_SETTING.get(settings));
    }


}
