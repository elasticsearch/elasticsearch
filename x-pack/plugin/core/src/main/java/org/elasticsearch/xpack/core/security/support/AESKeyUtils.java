/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public final class AESKeyUtils {
    public static final int KEY_LENGTH_IN_BYTES = 32; // 256-bit AES key
    public static final int WRAPPED_KEY_LENGTH_IN_BYTES = KEY_LENGTH_IN_BYTES + 8; // https://www.ietf.org/rfc/rfc3394.txt section 2.2
    // parameter for the KDF function, it's a funny and unusual iter count larger than 60k
    private static final int KDF_ITER = 61616;
    // the KDF algorithm that generate the symmetric key given the password
    private static final String KDF_ALGO = "PBKDF2WithHmacSHA512";
    // The Id of any AES SecretKey is the AES-Wrap-ciphertext of this fixed 32 byte wide array.
    // Key wrapping encryption is deterministic (same plaintext generates the same ciphertext)
    // and the probability that two different keys map the same plaintext to the same ciphertext is very small
    // (2^-256, much lower than the UUID collision of 2^-128), assuming AES is indistinguishable from a pseudorandom permutation.
    private static final byte[] KEY_ID_PLAINTEXT = "wrapping known text forms key id".getBytes(StandardCharsets.UTF_8);

    public static byte[] wrap(SecretKey wrappingKey, SecretKey keyToWrap) throws GeneralSecurityException {
        assert "AES".equals(wrappingKey.getAlgorithm());
        assert "AES".equals(keyToWrap.getAlgorithm());
        Cipher c = Cipher.getInstance("AESWrap");
        c.init(Cipher.WRAP_MODE, wrappingKey);
        return c.wrap(keyToWrap);
    }

    public static SecretKey unwrap(SecretKey wrappingKey, byte[] keyToUnwrap) throws GeneralSecurityException {
        assert "AES".equals(wrappingKey.getAlgorithm());
        assert keyToUnwrap.length == WRAPPED_KEY_LENGTH_IN_BYTES;
        Cipher c = Cipher.getInstance("AESWrap");
        c.init(Cipher.UNWRAP_MODE, wrappingKey);
        Key unwrappedKey = c.unwrap(keyToUnwrap, "AES", Cipher.SECRET_KEY);
        return new SecretKeySpec(unwrappedKey.getEncoded(), "AES"); // make sure unwrapped key is "AES"
    }

    /**
     * Computes the ID of the given AES {@code SecretKey}.
     * The ID can be published as it does not leak any information about the key.
     * Different {@code SecretKey}s have different IDs with a very high probability.
     * <p>
     * The ID is the ciphertext of a known plaintext, using the AES Wrap cipher algorithm.
     * AES Wrap algorithm is deterministic, i.e. encryption using the same key, of the same plaintext, generates the same ciphertext.
     * Moreover, the ciphertext reveals no information on the key, and the probability of collision of ciphertexts given different
     * keys is statistically negligible.
     */
    public static String computeId(SecretKey secretAESKey) throws GeneralSecurityException {
        byte[] ciphertextOfKnownPlaintext = wrap(secretAESKey, new SecretKeySpec(KEY_ID_PLAINTEXT, "AES"));
        return new String(Base64.getUrlEncoder().withoutPadding().encode(ciphertextOfKnownPlaintext), StandardCharsets.UTF_8);
    }

    public static SecretKey generatePasswordBasedKey(SecureString password, String salt) throws ExecutionException, InterruptedException {
        return generatePasswordBasedKey(password, salt, EsExecutors.newDirectExecutorService()).get();
    }

    private static ListenableFuture<SecretKey> generatePasswordBasedKey(SecureString password, String salt,
                                                                        ExecutorService executorService) {
        final ListenableFuture<SecretKey> listenableFuture = new ListenableFuture<>();
        executorService.execute(ActionRunnable.supply(listenableFuture,
                () -> {
                    PBEKeySpec keySpec = new PBEKeySpec(password.getChars(), salt.getBytes(StandardCharsets.UTF_8), KDF_ITER,
                            KEY_LENGTH_IN_BYTES * Byte.SIZE);
                    SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KDF_ALGO);
                    SecretKey secretKey = keyFactory.generateSecret(keySpec);
                    SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");
                    return secret;
                }));
        return listenableFuture;
    }

    public static ListenableFuture<String> computeSaltedPasswordHash(SecureString password, ExecutorService executorService) {
        return computeSaltedPasswordHash(password, UUIDs.randomBase64UUID(), executorService);
    }

    public static boolean verifySaltedPasswordHash(SecureString password, String saltedHash)
            throws ExecutionException, InterruptedException {
        return verifySaltedPasswordHash(password, saltedHash, EsExecutors.newDirectExecutorService()).get();
    }

    public static ListenableFuture<Boolean> verifySaltedPasswordHash(SecureString password, String saltedHash,
                                                                     ExecutorService executorService) {
        final ListenableFuture<Boolean> listenableFuture = new ListenableFuture<>();
        if (password == null) {
            listenableFuture.onFailure(new IllegalArgumentException("Unexpected null password used for hash verify"));
        } else if (saltedHash == null) {
            listenableFuture.onFailure(new IllegalArgumentException("Unexpected null hash to verify"));
        } else {
            String[] saltAndHash = saltedHash.split(":");
            if (saltAndHash == null || saltAndHash.length != 2) {
                listenableFuture.onFailure(new IllegalArgumentException("Invalid salted hash format"));
            } else {
                // hash computation is always performed by the passed-in executor argument
                computeSaltedPasswordHash(password, saltAndHash[0], executorService)
                        .addListener(listenableFuture.map(computedSaltedHash ->
                                computedSaltedHash.equals(saltedHash)), EsExecutors.newDirectExecutorService());
            }
        }
        return listenableFuture;
    }

    private static ListenableFuture<String> computeSaltedPasswordHash(SecureString password, String salt,
                                                                      ExecutorService executorService) {
        final ListenableFuture<String> listenableFuture = new ListenableFuture<>();
        generatePasswordBasedKey(password, salt, executorService).addListener(listenableFuture.map(secretKey -> {
            String keyId = AESKeyUtils.computeId(secretKey);
            return salt + ":" + keyId;
        }), EsExecutors.newDirectExecutorService());
        return listenableFuture;
    }
}
