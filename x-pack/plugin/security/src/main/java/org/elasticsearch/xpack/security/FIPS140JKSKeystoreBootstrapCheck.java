package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;


public class FIPS140JKSKeystoreBootstrapCheck implements BootstrapCheck {

    private final boolean fipsModeEnabled;

    FIPS140JKSKeystoreBootstrapCheck(Settings settings) {
        this.fipsModeEnabled = Security.FIPS_MODE_ENABLED.get(settings);
    }

    /**
     * Test if the node fails the check.
     *
     * @param context the bootstrap context
     * @return the result of the bootstrap check
     */
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {

        if (fipsModeEnabled) {
            final Settings settings = context.settings;
            Settings keystoreTypeSettings = settings.filter(k -> k.endsWith("keystore.type"))
                .filter(k -> settings.get(k).equalsIgnoreCase("jks"));
            if (keystoreTypeSettings.isEmpty() == false) {
                return BootstrapCheckResult.failure("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                    "revisit [" + keystoreTypeSettings.toDelimitedString(',') + "] settings");
            }
            // Default Keystore type is JKS if not explicitly set
            Settings keystorePathSettings = settings.filter(k -> k.endsWith("keystore.path"))
                .filter(k -> settings.hasValue(k.replace(".path", ".type")) == false);
            if (keystorePathSettings.isEmpty() == false) {
                return BootstrapCheckResult.failure("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                    "revisit [" + keystorePathSettings.toDelimitedString(',') + "] settings");
            }

        }
        return BootstrapCheckResult.success();
    }

    @Override
    public boolean alwaysEnforce() {
        return fipsModeEnabled;
    }
}
