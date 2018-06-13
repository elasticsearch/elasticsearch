/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.NetworkUtil;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;

/**
 * Utility wrapper around Apache {@link SimpleKdcServer} backed by Unboundid
 * {@link InMemoryDirectoryServer}.<br>
 * Starts in memory Ldap server and then uses it as backend for Kdc Server.
 */
public class SimpleKdcLdapServer {
    private static final Logger logger = Loggers.getLogger(SimpleKdcLdapServer.class);

    private Path workDir = null;
    private SimpleKdcServer simpleKdc;
    private InMemoryDirectoryServer ldapServer;

    // KDC properties
    private String transport = ESTestCase.randomFrom("TCP", "UDP");
    private int kdcPort = 0;
    private String host;
    private String realm;
    private boolean krb5DebugBackupConfigValue;

    // LDAP properties
    private String baseDn;
    private Path ldiff;
    private int ldapPort;

    /**
     * Constructor for SimpleKdcLdapServer, creates instance of Kdc server and ldap
     * backend server. Also initializes them with provided configuration.
     *
     * @param workDir Base directory for server, used to locate kdc.conf,
     *            backend.conf and kdc.ldiff
     * @param orgName Org name for base dn
     * @param domainName domain name for base dn
     * @param ldiff for ldap directory.
     * @throws Exception
     */
    public SimpleKdcLdapServer(final Path workDir, final String orgName, final String domainName, final Path ldiff) throws Exception {
        this.workDir = workDir;
        this.realm = domainName.toUpperCase(Locale.ROOT) + "." + orgName.toUpperCase(Locale.ROOT);
        this.baseDn = "dc=" + domainName + ",dc=" + orgName;
        this.ldiff = ldiff;
        this.krb5DebugBackupConfigValue = AccessController.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
            @Override
            @SuppressForbidden(reason = "set or clear system property krb5 debug in tests")
            public Boolean run() throws Exception {
                boolean oldDebugSetting = Boolean.parseBoolean(System.getProperty("sun.security.krb5.debug"));
                System.setProperty("sun.security.krb5.debug", Boolean.TRUE.toString());
                return oldDebugSetting;
            }
        });

        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                init();
                return null;
            }
        });
        logger.info("SimpleKdcLdapServer started.");
    }

    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File")
    private void init() throws Exception {
        // start ldap server
        createLdapServiceAndStart();
        // create ldap backend conf
        createLdapBackendConf();
        // Kdc Server
        simpleKdc = new SimpleKdcServer(this.workDir.toFile(), new KrbConfig());
        prepareKdcServerAndStart();
    }

    private void createLdapServiceAndStart() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(baseDn);
        config.setSchema(null);
        ldapServer = new InMemoryDirectoryServer(config);
        ldapServer.importFromLDIF(true, this.ldiff.toString());
        ldapServer.startListening();
        ldapPort = ldapServer.getListenPort();
    }

    private void createLdapBackendConf() throws IOException {
        String backendConf = KdcConfigKey.KDC_IDENTITY_BACKEND.getPropertyKey()
                + " = org.apache.kerby.kerberos.kdc.identitybackend.LdapIdentityBackend\n"
                + "host=127.0.0.1\n" + "port=" + ldapPort + "\n" + "admin_dn=uid=admin,ou=system," + baseDn
                + "\n" + "admin_pw=secret\n" + "base_dn=" + baseDn;
        Files.write(this.workDir.resolve("backend.conf"), backendConf.getBytes(StandardCharsets.UTF_8));
        assert Files.exists(this.workDir.resolve("backend.conf"));
    }

    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File")
    private void prepareKdcServerAndStart() throws Exception {
        // transport
        simpleKdc.setWorkDir(workDir.toFile());
        simpleKdc.setKdcHost(host);
        simpleKdc.setKdcRealm(realm);
        if (kdcPort == 0) {
            kdcPort = NetworkUtil.getServerPort();
        }
        if (transport != null) {
            if (transport.trim().equals("TCP")) {
                simpleKdc.setKdcTcpPort(kdcPort);
                simpleKdc.setAllowUdp(false);
            } else if (transport.trim().equals("UDP")) {
                simpleKdc.setKdcUdpPort(kdcPort);
                simpleKdc.setAllowTcp(false);
            } else {
                throw new IllegalArgumentException("Invalid transport: " + transport);
            }
        } else {
            throw new IllegalArgumentException("Need to set transport!");
        }
        long minimumTicketLifeTime = simpleKdc.getKdcConfig().getMinimumTicketLifetime();
        long maxRenewableLifeTime = simpleKdc.getKdcConfig().getMaximumRenewableLifetime();
        simpleKdc.getKdcConfig().setLong(KdcConfigKey.MINIMUM_TICKET_LIFETIME, 86400000L);
        simpleKdc.getKdcConfig().setLong(KdcConfigKey.MAXIMUM_RENEWABLE_LIFETIME, 604800000L);
        logger.info("MINIMUM_TICKET_LIFETIME changed from {}  to {}", minimumTicketLifeTime,
                simpleKdc.getKdcConfig().getMinimumTicketLifetime());
        logger.info("MAXIMUM_RENEWABLE_LIFETIME changed from {}  to {}", maxRenewableLifeTime,
                simpleKdc.getKdcConfig().getMaximumRenewableLifetime());
        simpleKdc.init();
        simpleKdc.start();
    }

    public String getRealm() {
        return realm;
    }

    public int getLdapListenPort() {
        return ldapPort;
    }

    public int getKdcPort() {
        return kdcPort;
    }

    /**
     * Creates a principal in the KDC with the specified user and password.
     *
     * @param principal principal name, do not include the domain.
     * @param password password.
     * @throws Exception thrown if the principal could not be created.
     */
    public synchronized void createPrincipal(final String principal, final String password) throws Exception {
        simpleKdc.createPrincipal(principal, password);
    }

    /**
     * Creates multiple principals in the KDC and adds them to a keytab file.
     *
     * @param keytabFile keytab file to add the created principals. If keytab file
     *            exists and then always appends to it.
     * @param principals principals to add to the KDC, do not include the domain.
     * @throws Exception thrown if the principals or the keytab file could not be
     *             created.
     */
    @SuppressForbidden(reason = "Uses Apache Kdc which requires usage of java.io.File")
    public synchronized void createPrincipal(final Path keytabFile, final String... principals) throws Exception {
        simpleKdc.createPrincipals(principals);
        for (String principal : principals) {
            simpleKdc.getKadmin().exportKeytab(keytabFile.toFile(), principal);
        }
    }

    /**
     * Stop Simple Kdc Server
     * 
     * @throws PrivilegedActionException
     */
    public synchronized void stop() throws PrivilegedActionException {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {

            @Override
            @SuppressForbidden(reason = "set or clear system property krb5 debug in tests")
            public Void run() throws Exception {
                if (simpleKdc != null) {
                    try {
                        simpleKdc.stop();
                    } catch (KrbException e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    } finally {
                        System.setProperty("sun.security.krb5.debug", Boolean.toString(krb5DebugBackupConfigValue));
                    }
                }

                try {
                    // Will be fixed in next Kerby version.
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }

                if (ldapServer != null) {
                    ldapServer.shutDown(true);
                }
                return null;
            }
        });
        logger.info("SimpleKdcServer stoppped.");
    }

}