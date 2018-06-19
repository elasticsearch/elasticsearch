/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Utility class that validates kerberos ticket for peer authentication.
 * <p>
 * This class takes care of login by ES service credentials using keytab,
 * GSSContext establishment, and then validating the incoming token.
 * <p>
 * It may respond with token which needs to be communicated with the peer.
 */
public class KerberosTicketValidator {
    static final Oid SPNEGO_OID = getSpnegoOid();

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            throw ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    private static final Logger LOGGER = ESLoggerFactory.getLogger(KerberosTicketValidator.class);

    private static final String KEY_TAB_CONF_NAME = "KeytabConf";
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    /**
     * Validates client kerberos ticket received from the peer.
     * <p>
     * First performs service login using keytab, supports multiple principals in
     * keytab and the principal is selected based on the request.
     * <p>
     * The GSS security context establishment is handled and if it is established then
     * returns the Tuple of username and out token for peer reply. It may return
     * Tuple with null username but with a outToken to be sent to peer for further
     * negotiation.
     *
     * @param decodedToken base64 decoded kerberos ticket bytes
     * @param keyTabPath Path to Service key tab file containing credentials for ES
     *            service.
     * @param krbDebug if {@code true} enables jaas krb5 login module debug logs.
     * @return {@link Tuple} of user name {@link GSSContext#getSrcName()} and out
     *         token base64 encoded if any. When context is not yet established user
     *         name is {@code null}.
     * @throws LoginException thrown when service authentication fails
     *             {@link LoginContext#login()}
     * @throws GSSException thrown when GSS Context negotiation fails
     *             {@link GSSException}
     */
    public Tuple<String, String> validateTicket(final byte[] decodedToken, final Path keyTabPath, final boolean krbDebug)
            throws LoginException, GSSException {
        final GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        LoginContext loginContext = null;
        try {
            loginContext = serviceLogin(keyTabPath.toString(), krbDebug);
            GSSCredential serviceCreds = createCredentials(gssManager, loginContext.getSubject());
            gssContext = gssManager.createContext(serviceCreds);
            final String base64OutToken = base64Encode(acceptSecContext(decodedToken, gssContext, loginContext.getSubject()));
            LOGGER.trace("validateTicket isGSSContextEstablished = {}, username = {}, outToken = {}", gssContext.isEstablished(),
                    gssContext.getSrcName().toString(), base64OutToken);
            return new Tuple<>(gssContext.isEstablished() ? gssContext.getSrcName().toString() : null, base64OutToken);
        } catch (PrivilegedActionException pve) {
            if (pve.getCause() instanceof LoginException) {
                throw (LoginException) pve.getCause();
            }
            if (pve.getCause() instanceof GSSException) {
                throw (GSSException) pve.getCause();
            }
            throw ExceptionsHelper.convertToRuntime((Exception) ExceptionsHelper.unwrapCause(pve));
        } finally {
            privilegedLogoutNoThrow(loginContext);
            privilegedDisposeNoThrow(gssContext);
        }
    }

    private String base64Encode(final byte[] outToken) {
        if (outToken != null && outToken.length > 0) {
            return Base64.getEncoder().encodeToString(outToken);
        }
        return null;
    }

    /**
     * Handles GSS context establishment. Received token is passed to the GSSContext
     * on acceptor side and returns with out token that needs to be sent to peer for
     * further GSS context establishment.
     * <p>
     *
     * @param base64decodedTicket in token generated by peer
     * @param gssContext instance of acceptor {@link GSSContext}
     * @param subject authenticated subject
     * @return a byte[] containing the token to be sent to the peer. null indicates
     *         that no token is generated.
     * @throws PrivilegedActionException
     * @see GSSContext#acceptSecContext(byte[], int, int)
     */
    private static byte[] acceptSecContext(final byte[] base64decodedTicket, final GSSContext gssContext, Subject subject)
            throws PrivilegedActionException {
        // process token with gss context
        return doAsWrapper(subject,
                (PrivilegedExceptionAction<byte[]>) () -> gssContext.acceptSecContext(base64decodedTicket, 0, base64decodedTicket.length));
    }

    /**
     * For acquiring SPNEGO mechanism credentials for service based on the subject
     *
     * @param gssManager {@link GSSManager}
     * @param subject logged in {@link Subject}
     * @return {@link GSSCredential} for particular mechanism
     * @throws GSSException
     * @throws PrivilegedActionException
     */
    private static GSSCredential createCredentials(final GSSManager gssManager,
            final Subject subject) throws GSSException, PrivilegedActionException {
        return doAsWrapper(subject, (PrivilegedExceptionAction<GSSCredential>) () -> gssManager
                .createCredential(null, GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.ACCEPT_ONLY));
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs to perform work as
     * given subject.
     *
     * @param subject {@link Subject} to be used for this work
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @return the value returned by the PrivilegedExceptionAction's run method
     * @throws PrivilegedActionException
     */
    private static <T> T doAsWrapper(final Subject subject, final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAs(subject, action));
        } catch (PrivilegedActionException pae) {
            if (pae.getCause() instanceof PrivilegedActionException) {
                throw (PrivilegedActionException) pae.getCause();
            }
            throw pae;
        }
    }

    /**
     * Privileged wrapper for closing GSSContext, does not throw exceptions but logs
     * them as debug message.
     *
     * @param gssContext GSSContext to be disposed.
     */
    private static void privilegedDisposeNoThrow(final GSSContext gssContext) {
        if (gssContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    gssContext.dispose();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                LOGGER.debug("Could not dispose GSS Context", (Exception) ExceptionsHelper.unwrapCause(e));
            }
        }
    }

    /**
     * Privileged wrapper for closing LoginContext, does not throw exceptions but
     * logs them as debug message.
     *
     * @param loginContext LoginContext to be closed
     */
    private static void privilegedLogoutNoThrow(final LoginContext loginContext) {
        if (loginContext != null) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    loginContext.logout();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                LOGGER.debug("Could not close LoginContext", (Exception) ExceptionsHelper.unwrapCause(e));
            }
        }
    }

    /**
     * Performs authentication using provided keytab
     *
     * @param keytabFilePath Keytab file path
     * @param krbDebug if {@code true} enables jaas krb5 login module debug logs.
     * @return authenticated {@link LoginContext} instance. Note: This needs to be
     *         closed using {@link LoginContext#logout()} after usage.
     * @throws PrivilegedActionException when privileged action threw exception
     */
    private static LoginContext serviceLogin(final String keytabFilePath, final boolean krbDebug)
            throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<LoginContext>) () -> {
            final Subject subject = new Subject(false, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
            final Configuration conf = new KeytabJaasConf(keytabFilePath, krbDebug);
            final LoginContext loginContext = new LoginContext(KEY_TAB_CONF_NAME, subject, null, conf);
            loginContext.login();
            return loginContext;
        });
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration. As
     * we have static configuration except debug flag, we are constructing in memory. This
     * avoid additional configuration required from the user.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and requires appropriate security permissions to do so.
     */
    static class KeytabJaasConf extends Configuration {
        private final String keytabFilePath;
        private final boolean krbDebug;

        KeytabJaasConf(final String keytabFilePath, final boolean krbDebug) {
            this.keytabFilePath = keytabFilePath;
            this.krbDebug = krbDebug;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("keyTab", keytabFilePath);
            /*
             * As acceptor, we can have multiple SPNs, we do not want to use particular
             * principal so it uses "*"
             */
            options.put("principal", "*");
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("doNotPrompt", Boolean.TRUE.toString());
            options.put("renewTGT", Boolean.FALSE.toString());
            options.put("refreshKrb5Config", Boolean.TRUE.toString());
            options.put("isInitiator", Boolean.FALSE.toString());
            options.put("debug", Boolean.toString(krbDebug));

            return new AppConfigurationEntry[] { new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.unmodifiableMap(options)) };
        }

    }
}
