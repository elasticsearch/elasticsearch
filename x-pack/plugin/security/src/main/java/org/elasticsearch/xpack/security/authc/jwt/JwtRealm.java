/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.support.CacheIteratorHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.join;
import static org.elasticsearch.core.Strings.format;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "ES-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";
    public static final String HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME = "SharedSecret";

    private final Cache<BytesArray, ExpiringUser> jwtCache;
    private final CacheIteratorHelper<BytesArray, ExpiringUser> jwtCacheHelper;
    private final JwtRealmsService jwtRealmsService;
    final UserRoleMapper userRoleMapper;
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    final ClaimParser claimParserDn;
    final ClaimParser claimParserMail;
    final ClaimParser claimParserName;
    final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    private final JwtAuthenticator jwtAuthenticator;
    private final TimeValue allowedClockSkew;
    DelegatedAuthorizationSupport delegatedAuthorizationSupport = null;

    JwtRealm(
        final RealmConfig realmConfig,
        final JwtRealmsService jwtRealmsService,
        final SSLService sslService,
        final UserRoleMapper userRoleMapper
    ) throws SettingsException {
        super(realmConfig);
        this.jwtRealmsService = jwtRealmsService; // common configuration settings shared by all JwtRealm instances
        this.userRoleMapper = userRoleMapper;
        this.userRoleMapper.refreshRealmOnChange(this);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.claimParserPrincipal = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_PRINCIPAL, realmConfig, true);
        this.claimParserGroups = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_GROUPS, realmConfig, false);
        this.claimParserDn = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_DN, realmConfig, false);
        this.claimParserMail = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_MAIL, realmConfig, false);
        this.claimParserName = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_NAME, realmConfig, false);
        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
        this.clientAuthenticationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        final SecureString sharedSecret = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET);
        this.clientAuthenticationSharedSecret = Strings.hasText(sharedSecret) ? sharedSecret : null; // convert "" to null

        // Validate Client Authentication settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            this.clientAuthenticationSharedSecret
        );

        final TimeValue jwtCacheTtl = realmConfig.getSetting(JwtRealmSettings.JWT_CACHE_TTL);
        final int jwtCacheSize = realmConfig.getSetting(JwtRealmSettings.JWT_CACHE_SIZE);
        if (jwtCacheTtl.getNanos() > 0 && jwtCacheSize > 0) {
            this.jwtCache = CacheBuilder.<BytesArray, ExpiringUser>builder()
                .setExpireAfterWrite(jwtCacheTtl)
                .setMaximumWeight(jwtCacheSize)
                .build();
            this.jwtCacheHelper = new CacheIteratorHelper<>(this.jwtCache);
        } else {
            // TODO: log invalid cache settings
            this.jwtCache = null;
            this.jwtCacheHelper = null;
        }
        jwtAuthenticator = new JwtAuthenticator(realmConfig, sslService, this::expireAll);
    }

    /**
     * If X-pack licensing allows it, initialize delegated authorization support.
     * JWT realm will use the list of all realms to link to its named authorization realms.
     * @param allRealms List of all realms containing authorization realms for this JWT realm.
     * @param xpackLicenseState X-pack license state.
     */
    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        if (delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm " + name() + " has already been initialized");
        }
        // extract list of realms referenced by config.settings() value for DelegatedAuthorizationSettings.ROLES_REALMS
        delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, config, xpackLicenseState);
    }

    /**
     * Clean up JWT cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        jwtAuthenticator.close();
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        ensureInitialized();
        listener.onResponse(null); // Run-As and Delegated Authorization lookups are not supported by JWT realms
    }

    @Override
    public void expire(final String username) {
        ensureInitialized();
        if (isCacheEnabled()) {
            logger.trace("Expiring JWT cache entries for realm [{}] principal=[{}]", name(), username);
            jwtCacheHelper.removeValuesIf(expiringUser -> expiringUser.user.principal().equals(username));
            logger.trace("Expired JWT cache entries for realm [{}] principal=[{}]", name(), username);
        }
    }

    @Override
    public void expireAll() {
        ensureInitialized();
        invalidateJwtCache();
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        ensureInitialized();
        // Token parsing is common code for all realms
        // First JWT realm will parse in a way that is compatible with all JWT realms,
        // taking into consideration each JWT realm might have a different principal claim name
        return jwtRealmsService.token(threadContext);
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        ensureInitialized();
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            final String tokenPrincipal = jwtAuthenticationToken.principal();

            // Authenticate client: If client authc off, fall through. Otherwise, only fall through if secret matched.
            final SecureString clientSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
            try {
                JwtUtil.validateClientAuthentication(clientAuthenticationType, clientAuthenticationSharedSecret, clientSecret);
                logger.trace("Realm [{}] client authentication succeeded for token=[{}].", name(), tokenPrincipal);
            } catch (Exception e) {
                final String msg = "Realm [" + name() + "] client authentication failed for token=[" + tokenPrincipal + "].";
                logger.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return; // FAILED (secret is missing or mismatched)
            }

            final SecureString serializedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final BytesArray jwtCacheKey = isCacheEnabled() ? new BytesArray(JwtUtil.sha256(serializedJwt)) : null;
            if (jwtCacheKey != null) {
                final User cachedUser = tryAuthenticateWithCache(tokenPrincipal, jwtCacheKey);
                // TODO: why not cache the final user after delegation?
                if (cachedUser != null) {
                    if (delegatedAuthorizationSupport.hasDelegation()) {
                        delegatedAuthorizationSupport.resolve(cachedUser.principal(), listener);
                    } else {
                        listener.onResponse(AuthenticationResult.success(cachedUser));
                    }
                    return;
                }
            }

            // Validate JWT: Extract JWT and claims set, and validate JWT.
            jwtAuthenticator.authenticate(
                jwtAuthenticationToken,
                ActionListener.wrap(claimsSet -> processValidatedJwt(tokenPrincipal, jwtCacheKey, claimsSet, listener), ex -> {
                    final String msg = "Realm [" + name() + "] JWT validation failed for token=[" + tokenPrincipal + "].";
                    logger.debug(msg, ex);
                    // TODO: No point to continue to another realm if failure is ParseException
                    listener.onResponse(AuthenticationResult.unsuccessful(msg, ex));
                })
            );

        } else {
            assert false : "should not happen";
            final String className = (authenticationToken == null) ? "null" : authenticationToken.getClass().getCanonicalName();
            final String msg = "Realm [" + name() + "] does not support AuthenticationToken [" + className + "].";
            logger.trace(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    void ensureInitialized() {
        if (delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
    }

    private User tryAuthenticateWithCache(final String tokenPrincipal, final BytesArray jwtCacheKey) {
        final ExpiringUser expiringUser = jwtCache.get(jwtCacheKey);
        if (expiringUser == null) {
            logger.trace("Realm [" + name() + "] JWT cache miss token=[" + tokenPrincipal + "] key=[" + jwtCacheKey + "].");
        } else {
            final User user = expiringUser.user;
            final Date exp = expiringUser.exp; // claimsSet.getExpirationTime().getTime() + allowedClockSkew.getMillis()
            final String principal = user.principal();
            final Date now = new Date();
            if (now.getTime() < exp.getTime()) {
                logger.trace(
                    "Realm ["
                        + name()
                        + "] JWT cache hit token=["
                        + tokenPrincipal
                        + "] key=["
                        + jwtCacheKey
                        + "] principal=["
                        + principal
                        + "] exp=["
                        + exp
                        + "] now=["
                        + now
                        + "]."
                );
                return user;
            }
            // TODO: evict the entry
            logger.trace(
                "Realm ["
                    + name()
                    + "] JWT cache exp token=["
                    + tokenPrincipal
                    + "] key=["
                    + jwtCacheKey
                    + "] principal=["
                    + principal
                    + "] exp=["
                    + exp
                    + "] now=["
                    + now
                    + "]."
            );
        }
        return null;
    }

    private void processValidatedJwt(
        String tokenPrincipal,
        BytesArray jwtCacheKey,
        JWTClaimsSet claimsSet,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        // At this point, JWT is validated. Parse the JWT claims using realm settings.
        final String principal = claimParserPrincipal.getClaimValue(claimsSet);
        if (Strings.hasText(principal) == false) {
            final String msg = "Realm ["
                + name()
                + "] no principal for token=["
                + tokenPrincipal
                + "] parser=["
                + claimParserPrincipal
                + "] claims=["
                + claimsSet
                + "].";
            logger.debug(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
            return;
        }

        // Roles listener: Log roles from delegated authz lookup or role mapping, and cache User if JWT cache is enabled.
        final ActionListener<AuthenticationResult<User>> logAndCacheListener = ActionListener.wrap(result -> {
            if (result.isAuthenticated()) {
                final User user = result.getValue();
                logger.debug(() -> format("Realm [%s] roles [%s] for principal=[%s].", name(), join(",", user.roles()), principal));
                if (isCacheEnabled()) {
                    try (ReleasableLock ignored = jwtCacheHelper.acquireUpdateLock()) {
                        final long expWallClockMillis = claimsSet.getExpirationTime().getTime() + allowedClockSkew.getMillis();
                        jwtCache.put(jwtCacheKey, new ExpiringUser(result.getValue(), new Date(expWallClockMillis)));
                    }
                }
            }
            listener.onResponse(result);
        }, listener::onFailure);

        // Delegated role lookup or Role mapping: Use the above listener to log roles and cache User.
        if (delegatedAuthorizationSupport.hasDelegation()) {
            delegatedAuthorizationSupport.resolve(principal, logAndCacheListener);
            return;
        }

        // User metadata: If enabled, extract metadata from JWT claims set. Use it in UserRoleMapper.UserData and User constructors.
        final Map<String, Object> userMetadata;
        try {
            userMetadata = populateUserMetadata ? JwtUtil.toUserMetadata(claimsSet) : Map.of();
        } catch (Exception e) {
            final String msg = "Realm [" + name() + "] parse metadata failed for principal=[" + principal + "].";
            logger.debug(msg, e);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
            return;
        }

        // Role resolution: Handle role mapping in JWT Realm.
        final List<String> groups = claimParserGroups.getClaimValues(claimsSet);
        final String dn = claimParserDn.getClaimValue(claimsSet);
        final String mail = claimParserMail.getClaimValue(claimsSet);
        final String name = claimParserName.getClaimValue(claimsSet);
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, config);
        userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
            final User user = new User(principal, rolesSet.toArray(Strings.EMPTY_ARRAY), name, mail, userData.getMetadata(), true);
            logAndCacheListener.onResponse(AuthenticationResult.success(user));
        }, logAndCacheListener::onFailure));
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        ensureInitialized();
        usageStats(ActionListener.wrap(stats -> {
            stats.put("jwt.cache", Collections.singletonMap("size", jwtCache == null ? -1 : jwtCache.count()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    /**
     * Clean up JWT cache (if enabled).
     */
    private void invalidateJwtCache() {
        if (isCacheEnabled()) {
            try {
                logger.trace("Invalidating JWT cache for realm [{}]", name());
                try (ReleasableLock ignored = jwtCacheHelper.acquireUpdateLock()) {
                    jwtCache.invalidateAll();
                }
                logger.debug("Invalidated JWT cache for realm [{}]", name());
            } catch (Exception e) {
                logger.warn("Exception invalidating JWT cache for realm [{}]", name(), e);
            }
        }
    }

    private boolean isCacheEnabled() {
        return jwtCache != null && jwtCacheHelper != null;
    }

    // Cached authenticated users, and adjusted JWT expiration date (=exp+skew) for checking if the JWT expired before the cache entry
    record ExpiringUser(User user, Date exp) {
        ExpiringUser {
            Objects.requireNonNull(user, "User must not be null");
            Objects.requireNonNull(exp, "Expiration date must not be null");
        }
    }
}
