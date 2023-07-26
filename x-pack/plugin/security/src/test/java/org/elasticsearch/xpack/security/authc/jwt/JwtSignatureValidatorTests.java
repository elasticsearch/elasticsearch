/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class JwtSignatureValidatorTests extends ESTestCase {

    private AtomicInteger validateSignatureAttemptCounter;
    private AtomicInteger finalSuccessCounter;
    private AtomicInteger finalFailureCounter;
    private AtomicInteger reloadAttemptCounter;
    private JwkSetLoader jwkSetLoader;
    private JwtSignatureValidator.PkcJwtSignatureValidator signatureValidator;
    private ActionListener<Void> primaryListener;
    private final SignedJWT signedJWT = mock(SignedJWT.class);

    @Before
    public void setup() throws Exception {
        final Path tempDir = createTempDir();
        final Path path = tempDir.resolve("jwkset.json");
        Files.write(path, List.of("{\"keys\":[]}"), StandardCharsets.UTF_8);
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH)).thenReturn("jwkset.json");
        final Environment env = mock(Environment.class);
        when(env.configFile()).thenReturn(tempDir);
        when(realmConfig.env()).thenReturn(env);

        validateSignatureAttemptCounter = new AtomicInteger();
        reloadAttemptCounter = new AtomicInteger();
        finalSuccessCounter = new AtomicInteger();
        finalFailureCounter = new AtomicInteger();

        jwkSetLoader = spy(new JwkSetLoader(realmConfig, List.of(), null));

        final JwtSignatureValidator.PkcJwkSetReloadNotifier reloadNotifier = () -> {};
        signatureValidator = spy(new JwtSignatureValidator.PkcJwtSignatureValidator(jwkSetLoader, reloadNotifier));

        // emulates the primary listener that is passed to the authenticate method, success here is a successful authentication
        primaryListener = new ActionListener<>() {
            @Override
            public void onResponse(Void o) {
                finalSuccessCounter.getAndIncrement();
            }

            @Override
            public void onFailure(Exception e) {
                finalFailureCounter.getAndIncrement();
            }
        };
    }

    @SuppressWarnings("unchecked")
    public void testWorkflowWithSuccess() throws Exception {
        Mockito.doAnswer(invocation -> {
            // count any reload attempts
            reloadAttemptCounter.getAndIncrement();
            // don't bother calling the real method, we just want to assert 0 attempts to reload
            return null;
        }).when(jwkSetLoader).reload(any(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            validateSignatureAttemptCounter.getAndIncrement();
            // don't actually attempt the validation, just don't throw an exception to represent success
            return null;
        }).when(signatureValidator).validateSignature(any(SignedJWT.class), anyList());
        signatureValidator.validate(randomIdentifier(), signedJWT, primaryListener);
        assertThat(validateSignatureAttemptCounter.get(), is(1));
        assertThat(finalSuccessCounter.get(), is(1));
        assertThat(finalFailureCounter.get(), is(0));
        assertThat(reloadAttemptCounter.get(), is(0));
    }

    @SuppressWarnings("unchecked")
    public void testWorkflowWithFailure() throws Exception {
        // failures will trigger a reload of the keyset
        Mockito.doAnswer(invocation -> {
            // count the reload events
            reloadAttemptCounter.getAndIncrement();
            // grab the listener that is passed to reload method and call the onResponse method to simulate a successful reload
            ActionListener<Tuple<Boolean, JwkSetLoader.JwksAlgs>> listener = invocation.getArgument(0);
            listener.onResponse(new Tuple<>(Boolean.FALSE, new JwkSetLoader.JwksAlgs(Collections.emptyList(), Collections.emptyList())));
            return null;
        }).when(jwkSetLoader).reload(any(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            validateSignatureAttemptCounter.getAndIncrement();
            // don't actually attempt signature, but do throw an exception to represent a failure which will cause a reload
            throw new RuntimeException("boom");
        }).when(signatureValidator).validateSignature(any(SignedJWT.class), anyList());
        signatureValidator.validate(randomIdentifier(), signedJWT, primaryListener);
        assertThat(validateSignatureAttemptCounter.get(), is(1));
        assertThat(finalSuccessCounter.get(), is(0));
        assertThat(finalFailureCounter.get(), is(1));
        assertThat(reloadAttemptCounter.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testWorkflowWithFailureThenSuccess() throws Exception {
        // failures will trigger a reload of the keyset
        Mockito.doAnswer(invocation -> {
            // count the reload events
            reloadAttemptCounter.getAndIncrement();
            // grab the listener that is passed to reload method and call the onResponse method to simulate a successful reload
            ActionListener<Tuple<Boolean, JwkSetLoader.JwksAlgs>> listener = invocation.getArgument(0);
            listener.onResponse(new Tuple<>(Boolean.TRUE, new JwkSetLoader.JwksAlgs(Collections.emptyList(), Collections.emptyList())));
            return null;
        }).when(jwkSetLoader).reload(any(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            // don't actually attempt to do the validation the signature
            // throw an exception to represent a failure or return null to represent a success
            if (validateSignatureAttemptCounter.getAndIncrement() == 0) {
                // signature validation fails on first attempt
                throw new RuntimeException("boom");
            } else {
                // subsequent signature validation works
                return null;
            }
        }).when(signatureValidator).validateSignature(any(SignedJWT.class), anyList());
        signatureValidator.validate(randomIdentifier(), signedJWT, primaryListener);
        assertThat(validateSignatureAttemptCounter.get(), is(2));
        assertThat(finalSuccessCounter.get(), is(1));
        assertThat(finalFailureCounter.get(), is(0));
        assertThat(reloadAttemptCounter.get(), is(1));
    }

    // There is a concurrency bug around retry of the signature verification.
    // This test reproduces the scenario.
    // critical code is [reload -> work -> update state]
    // if 2 threads enter the critical code concurrently, but one thread finishes first the sequence can be
    // thread 1: fail signature -> [reload (1) -> work (3) -> update volatile state (4) ] -> retry signature
    // thread 2: fail signature -> [reload (2) -> work (5) -> already updated (6) ] -> don't retry signature
    // thread 2 does the work (same work as thread 1) but does results in no update because thread 1 already updated the volatile state
    // because the state is not updated from the work done in the thread we don't attempt to retry the signature validation which
    // results in a failure to verify the signature when it should work
    @SuppressWarnings("unchecked")
    public void testConcurrentWorkflowWithFailureThenSuccess() throws Exception {

        final CyclicBarrier reloadBarrier = new CyclicBarrier(2);

        // failure - with reload then success
        Mockito.doAnswer(invocation -> {
            safeAwait(reloadBarrier); // block here to ensure both threads have failed once

            // grab the listener that is passed to reload method and call the onResponse method to simulate a successful reload
            ActionListener<Tuple<Boolean, JwkSetLoader.JwksAlgs>> listener = invocation.getArgument(0);
            if (reloadAttemptCounter.getAndIncrement() == 0) {
                // first reload returns it was indeed reloaded
                listener.onResponse(new Tuple<>(Boolean.TRUE, new JwkSetLoader.JwksAlgs(Collections.emptyList(), Collections.emptyList())));
            } else {
                // subsequent reloads returns it was not reloaded
                listener.onResponse(
                    new Tuple<>(Boolean.FALSE, new JwkSetLoader.JwksAlgs(Collections.emptyList(), Collections.emptyList()))
                );
            }
            return null;
        }).when(jwkSetLoader).reload(any(ActionListener.class));

        Mockito.doAnswer(invocation -> {
            // don't actually attempt to do the signature
            // throw an exception to represent a failure or return null to represent a success
            if (validateSignatureAttemptCounter.getAndIncrement() <= 1) {
                // signature validation fails on first and second attempt, since we block when reloading we can be assured that first and
                // second attempts are attempt1 for thread1 and thread2
                throw new RuntimeException("boom");
            } else {
                // subsequent signature validation works
                return null;
            }
        }).when(signatureValidator).validateSignature(any(SignedJWT.class), anyList());

        final CyclicBarrier barrier = new CyclicBarrier(3);

        Thread t1 = new Thread(() -> {
            safeAwait(barrier);
            signatureValidator.validate(randomIdentifier(), signedJWT, primaryListener);
        });

        Thread t2 = new Thread(() -> {
            safeAwait(barrier);
            signatureValidator.validate(randomIdentifier(), signedJWT, primaryListener);
        });

        t1.start();
        t2.start();
        safeAwait(barrier); // kick off the work
        t1.join();
        t2.join();

        /**
         * THESE VALUES REPRESENT HOW IT ACTUALLY WORKS, NOT HOW IT SHOULD WORK
         * DUE THE BUG THE SECOND VALIDATION INCORRECTLY FAILS
         */
        assertThat(validateSignatureAttemptCounter.get(), is(3)); // should be 4, but the bug results in 1
        assertThat(finalSuccessCounter.get(), is(1)); // should be 2, but the bug results in 1
        assertThat(finalFailureCounter.get(), is(1)); // should be 0, but the bug results in 1
        assertThat(reloadAttemptCounter.get(), is(2)); // 2 is correct with or without the bug
    }

    public void testJwtSignVerifyPassedForAllSupportedAlgorithms() {
        // Pass: "ES256", "ES384", "ES512", RS256", "RS384", "RS512", "PS256", "PS384", "PS512, "HS256", "HS384", "HS512"
        for (final String signatureAlgorithm : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS) {
            try {
                helpTestSignatureAlgorithm(signatureAlgorithm, false);
            } catch (Exception e) {
                fail("signature validation with algorithm [" + signatureAlgorithm + "] should have succeeded");
            }
        }
        // Fail: "ES256K"
        final Exception exp1 = expectThrows(JOSEException.class, () -> helpTestSignatureAlgorithm(JWSAlgorithm.ES256K.getName(), false));
        final String msg1 = "Unsupported signature algorithm ["
            + JWSAlgorithm.ES256K
            + "]. Supported signature algorithms are "
            + JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS
            + ".";
        assertThat(exp1.getMessage(), equalTo(msg1));
    }

    private void helpTestSignatureAlgorithm(final String signatureAlgorithm, final boolean requireOidcSafe) throws Exception {
        logger.trace("Testing signature algorithm " + signatureAlgorithm);
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm, requireOidcSafe);
        final SecureString serializedJWTOriginal = JwtTestCase.randomBespokeJwt(jwk, signatureAlgorithm);
        final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal.toString());
        // OK ... so this empty jwtSignatureValidator is weird.
        // It's because there once was a static method that held the validateSignature method. It could be invoked independent of which
        // subclass was actually invoked. To help with test a bug with the signature validation the static validateSignature was moved
        // from a static method to default method in the interface. This is not ideal, but neither was un-testable code.
        JwtSignatureValidator jwtSignatureValidator = (tokenPrincipal, jwt, listener) -> {};
        jwtSignatureValidator.validateSignature(parsedSignedJWT, List.of(jwk));
    }

}
