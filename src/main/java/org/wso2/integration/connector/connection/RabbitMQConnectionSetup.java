/*
 * Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.integration.connector.connection;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.wso2.integration.connector.exception.InvalidConfigurationException;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.pojo.ConnectionConfiguration;
import org.wso2.integration.connector.pojo.RabbitMQConnectionConfig;
import org.wso2.integration.connector.pojo.RabbitMQOAuth2Config;
import org.wso2.integration.connector.pojo.RabbitMQSecureConnectionConfig;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.wso2.integration.connector.utils.RabbitMQConstants.CLIENT_ID;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CLIENT_SECRET;
import static org.wso2.integration.connector.utils.RabbitMQConstants.ConnectionRecoveryPolicy;
import static org.wso2.integration.connector.utils.RabbitMQConstants.OAUTH2_PASSWORD;
import static org.wso2.integration.connector.utils.RabbitMQConstants.OAUTH2_PASSWORD_GRANT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.OAUTH2_USERNAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SASL_MECHANISM;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SERVER_TLS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SERVER_TRUSTSTORE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SERVER_TRUSTSTORE_PASSWORD;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SERVER_TRUSTSTORE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SERVER_URLS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.TOKEN_ENDPOINT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.VIRTUAL_HOST;

/**
 * This class is responsible for setting up and managing RabbitMQ connections.
 * It provides methods to configure plain and secure connections, apply OAuth2 authentication,
 * and handle connection recovery policies. The class also includes utility methods for
 * initializing SSL contexts, validating configurations, and retrying connection attempts.
 * <p>
 * It ensures that the application can establish and maintain reliable communication
 * with RabbitMQ brokers.
 */
public class RabbitMQConnectionSetup {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionSetup.class);

    /**
     * Constructs a `ConnectionBuilder` using the provided `ConnectionConfiguration` and `AmqpEnvironmentBuilder`.
     *
     * @param configuration The connection configuration containing details for the RabbitMQ connection.
     * @param builder       The AMQP environment builder used to configure the connection settings.
     * @return A `ConnectionBuilder` instance configured with the provided settings.
     * @throws InvalidConfigurationException If the connection configuration is invalid or null.
     */
    public ConnectionBuilder constructConnectionBuilder(ConnectionConfiguration configuration,
                                                        AmqpEnvironmentBuilder builder)
            throws InvalidConfigurationException, RabbitMQConnectorException {

        // Retrieve the connection settings from the builder
        AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings = builder.connectionSettings();

        // Update connection settings for plain or secure connections
        RabbitMQConnectionConfig connectionConfig = configuration.getConnectionConfig();
        if (connectionConfig != null) {
            connectionSettings = updateConnectionSettingsForPlainConnection(connectionConfig, connectionSettings);
            if (connectionConfig instanceof RabbitMQSecureConnectionConfig) {
                RabbitMQSecureConnectionConfig secureConfig = (RabbitMQSecureConnectionConfig) connectionConfig;
                connectionSettings = updateConnectionSettingsForSecureConnection(secureConfig, connectionSettings);
            }
        } else {
            throw new InvalidConfigurationException("The connection configuration is null.");
        }

        // Apply OAuth2 configuration if available
        if (configuration.getOAuth2Config() != null) {
            connectionSettings = setOAuth2Configuration(configuration.getOAuth2Config(), connectionSettings);
        }

        // Build the environment and return the connection builder
        builder = connectionSettings.environmentBuilder();
        return getConnectionBuilder(configuration, builder.build().connectionBuilder());
    }

    /**
     * Updates the connection settings for a secure RabbitMQ connection.
     *
     * @param secureConfig       The secure connection configuration.
     * @param connectionSettings The current connection settings to be updated.
     * @return The updated connection settings.
     * @throws InvalidConfigurationException If an error occurs while configuring the secure connection.
     */
    public AmqpEnvironmentBuilder.EnvironmentConnectionSettings updateConnectionSettingsForSecureConnection(
            RabbitMQSecureConnectionConfig secureConfig,
            AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings)
            throws InvalidConfigurationException {

        // Configure TLS settings based on the secure configuration
        if (secureConfig.isTrustEverything()) {
            connectionSettings.tls().trustEverything();
        } else {
            connectionSettings.tls().sslContext(getSSLContext(secureConfig));
            if (secureConfig.isHostnameVerificationEnabled()) {
                connectionSettings.tls().hostnameVerification(true);
            }
        }
        return connectionSettings;
    }

    /**
     * Updates the connection settings for a plain RabbitMQ connection.
     *
     * @param connectionConfig   The plain connection configuration.
     * @param connectionSettings The current connection settings to be updated.
     * @return The updated connection settings.
     * @throws InvalidConfigurationException If mandatory parameters are missing or invalid.
     */
    public AmqpEnvironmentBuilder.EnvironmentConnectionSettings updateConnectionSettingsForPlainConnection(
            RabbitMQConnectionConfig connectionConfig,
            AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings)
            throws InvalidConfigurationException {

        // Validate and set server URLs, virtual host, and SASL mechanism
        validateAndSet(
                connectionConfig.getServerUrls(),
                SERVER_URLS,
                connectionSettings::uris);
        validateAndSet(
                connectionConfig.getVirtualHost(),
                VIRTUAL_HOST,
                connectionSettings::virtualHost);
        validateAndSet(
                connectionConfig.getSaslMechanism(),
                SASL_MECHANISM,
                connectionSettings::saslMechanism);

        // Set username and password for plain SASL mechanism
        if (!connectionConfig.isOAuth2Enabled() &&
                ConnectionSettings.SASL_MECHANISM_PLAIN.equalsIgnoreCase(connectionConfig.getSaslMechanism())) {
            connectionSettings.username(
                    validateNotNull(connectionConfig.getUsername(), "Mandatory parameter 'username' is not set."));
            connectionSettings.password(
                    validateNotNull(connectionConfig.getPassword(), "Mandatory parameter 'password' is not set."));
        }

        // Set connection idle timeout if specified
        if (connectionConfig.getConnectionIdleTimeout() != null) {
            connectionSettings.idleTimeout(connectionConfig.getConnectionIdleTimeout());
        }
        return connectionSettings;
    }

    /**
     * Configures the connection settings for OAuth2 authentication.
     *
     * @param oAuth2Config       The OAuth2 configuration containing token endpoint, client credentials, and grant type.
     * @param connectionSettings The current connection settings to be updated.
     * @return The updated connection settings with OAuth2 configuration applied.
     * @throws InvalidConfigurationException If mandatory OAuth2 parameters are missing or invalid.
     */
    public AmqpEnvironmentBuilder.EnvironmentConnectionSettings setOAuth2Configuration(
            RabbitMQOAuth2Config oAuth2Config,
            AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings)
            throws InvalidConfigurationException {

        connectionSettings.oauth2().tls().sslContext(getOAuth2SSLContext());
        connectionSettings.oauth2().shared(true);

        // Validate and set OAuth2 parameters
        validateAndSet(
                oAuth2Config.getTokenEndpoint(),
                TOKEN_ENDPOINT,
                connectionSettings.oauth2()::tokenEndpointUri
        );
        validateAndSet(
                oAuth2Config.getClientId(),
                CLIENT_ID,
                connectionSettings.oauth2()::clientId
        );
        validateAndSet(
                oAuth2Config.getClientSecret(),
                CLIENT_SECRET,
                connectionSettings.oauth2()::clientSecret
        );

        connectionSettings.oauth2().grantType(oAuth2Config.getGrantType());

        // Set username and password for password grant type
        if (OAUTH2_PASSWORD_GRANT_TYPE.equalsIgnoreCase(oAuth2Config.getGrantType())) {
            validateAndSet(
                    oAuth2Config.getUserName(),
                    OAUTH2_USERNAME,
                    username -> connectionSettings.oauth2().parameter("username", username));
            validateAndSet(
                    oAuth2Config.getPassword(),
                    OAUTH2_PASSWORD,
                    password -> connectionSettings.oauth2().parameter("password", password));
        }

        return connectionSettings;
    }

    /**
     * Creates and initializes an SSLContext for a secure RabbitMQ connection.
     *
     * @param secureConfig The secure connection configuration containing keystore and truststore details.
     * @return The initialized SSLContext.
     * @throws InvalidConfigurationException If an error occurs while creating the SSLContext.
     */
    private SSLContext getSSLContext(RabbitMQSecureConnectionConfig secureConfig)
            throws InvalidConfigurationException {
        SSLContext sslContext;
        try {
            // Validate and load keystore and truststore details
            String keyStoreLocation = validateNotNull(
                    secureConfig.getKeyStoreLocation(),
                    "Key Store Location is not provided for the secure connection.");
            String keyStorePassword = validateNotNull(
                    secureConfig.getKeyStorePassword(),
                    "Key Store password is not provided for the secure connection.");
            String trustStoreLocation = validateNotNull(
                    secureConfig.getTrustStoreLocation(),
                    "Trust Store Location is not provided for the secure connection.");
            String trustStorePassword = validateNotNull(
                    secureConfig.getTrustStorePassword(),
                    "Trust Store Password is not provided for the secure connection.");
            String keyStoreType = secureConfig.getKeyStoreType();
            String trustStoreType = secureConfig.getTrustStoreType();
            String sslVersion = secureConfig.getSslVersion();

            char[] keyPassphrase = keyStorePassword.toCharArray();
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            ks.load(Files.newInputStream(Paths.get(keyStoreLocation)), keyPassphrase);

            // Initialize KeyManagerFactory and TrustManagerFactory
            KeyManagerFactory kmf = initKeyManagerFactory(
                    keyStoreLocation, keyStoreType, keyStorePassword);
            TrustManagerFactory tmf = initTrustManagerFactory(
                    trustStoreLocation, trustStoreType, trustStorePassword);

            // Create and initialize the SSLContext
            try {
                sslContext = SSLContext.getInstance(sslVersion);
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            } catch (NoSuchAlgorithmException e) {
                throw new InvalidConfigurationException("Invalid SSL version: " + sslVersion +
                        ". Available SSL versions are: " + String.join(
                        ", ", SSLContext.getDefault().getSupportedSSLParameters().getProtocols()), e);
            }

        } catch (Exception ex) {
            throw new InvalidConfigurationException("Error while initiating SSL Context ", ex);
        }

        return sslContext;
    }

    /**
     * Creates and initializes an SSLContext for OAuth2 authentication.
     *
     * @return The initialized SSLContext for OAuth2.
     * @throws InvalidConfigurationException If an error occurs while creating the SSLContext.
     */
    private SSLContext getOAuth2SSLContext() throws InvalidConfigurationException {
        SSLContext sslContext = null;

        // Retrieve truststore details from system properties
        String trustStoreLocation = System.getProperty(SERVER_TRUSTSTORE);
        String trustStorePassword = System.getProperty(SERVER_TRUSTSTORE_PASSWORD);
        String trustStoreType = StringUtils.defaultIfEmpty(
                System.getProperty(SERVER_TRUSTSTORE_TYPE),
                KeyStore.getDefaultType());

        try {
            // Initialize TrustManagerFactory
            TrustManagerFactory tmf = initTrustManagerFactory(
                    trustStoreLocation, trustStoreType, trustStorePassword);

            // Create and initialize the SSLContext
            sslContext = SSLContext.getInstance(SERVER_TLS);
            sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
            return sslContext;
        } catch (Exception e) {
            log.error("Error in initializing SSL context from Server configuration.", e);
            throw new InvalidConfigurationException("Error in initializing SSL context from " +
                    "Server configuration", e);
        }
    }

    /**
     * Retrieves the recovery policy for RabbitMQ connections based on the provided configuration.
     *
     * @param connectionConfig The RabbitMQ connection configuration containing recovery policy details.
     * @return The `BackOffDelayPolicy` instance configured with the recovery policy.
     */
    private BackOffDelayPolicy getRecoveryPolicy(RabbitMQConnectionConfig connectionConfig) {

        String policyType = connectionConfig.getConnectionRecoveryPolicyType();
        Duration initialDelay = connectionConfig.getConnectionRecoveryInitialDelay();
        Duration interval = connectionConfig.getConnectionRecoveryInterval();
        Duration timeout = connectionConfig.getConnectionRecoveryTimeout();

        switch (ConnectionRecoveryPolicy.valueOf(policyType)) {
            case FIXED:
                return BackOffDelayPolicy.fixed(interval);
            case FIXED_WITH_INITIAL_DELAY:
                return BackOffDelayPolicy.fixedWithInitialDelay(initialDelay, interval);
            case FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT:
            default:
                return BackOffDelayPolicy.fixedWithInitialDelay(initialDelay, interval, timeout);
        }
    }

    /**
     * Creates a `ConnectionBuilder` for RabbitMQ with retry logic for connection establishment.
     *
     * @param configuration     The connection configuration containing connection details.
     * @param connectionBuilder The initial `ConnectionBuilder` instance.
     * @return The configured `ConnectionBuilder` instance.
     * @throws RabbitMQConnectorException If all retry attempts are exhausted.
     */
    public ConnectionBuilder getConnectionBuilder(ConnectionConfiguration configuration,
                                                  ConnectionBuilder connectionBuilder)
            throws RabbitMQConnectorException {
        String connectionName = configuration.getConnectionName();
        int attempts = 0;
        int retryCount = configuration.getConnectionConfig().getConnectionEstablishRetryCount();
        long retryInterval = configuration.getConnectionConfig().getConnectionEstablishRetryInterval();

        while (retryCount == -1 || attempts < retryCount) {
            try {
                if (attempts > 0) {
                    log.info("[" + connectionName + "] Attempting to create connection to RabbitMQ Broker." +
                            " Retry attempt: " + attempts);
                }
                return connectionBuilder
                        .recovery()
                        .backOffDelayPolicy(getRecoveryPolicy(configuration.getConnectionConfig()))
                        .connectionBuilder()
                        .listeners(new RabbitMQConnectionStateListener(connectionName));
            } catch (AmqpException.AmqpConnectionException e) {
                log.error("[" + connectionName + "] Connection attempt failed. Retrying in "
                        + retryInterval + "ms.", e);
                attempts++;
                sleep(retryInterval);
            }
        }
        throw new RabbitMQConnectorException("[" + connectionName + "] Could not connect to RabbitMQ Broker." +
                " All retry attempts exhausted.");
    }

    /**
     * Initializes a `KeyManagerFactory` using the provided keystore details.
     *
     * @param location The location of the keystore file.
     * @param type     The type of the keystore (e.g., JKS, PKCS12).
     * @param password The password for the keystore.
     * @return The initialized `KeyManagerFactory` instance.
     * @throws Exception If an error occurs during initialization.
     */
    private KeyManagerFactory initKeyManagerFactory(String location,
                                                    String type,
                                                    String password) throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(Files.newInputStream(Paths.get(location)), password.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password.toCharArray());
        return kmf;
    }

    /**
     * Initializes a `TrustManagerFactory` using the provided truststore details.
     *
     * @param location The location of the truststore file.
     * @param type     The type of the truststore (e.g., JKS, PKCS12).
     * @param password The password for the truststore.
     * @return The initialized `TrustManagerFactory` instance.
     * @throws Exception If an error occurs during initialization.
     */
    private TrustManagerFactory initTrustManagerFactory(String location,
                                                        String type,
                                                        String password) throws Exception {
        KeyStore tks = KeyStore.getInstance(type);
        tks.load(Files.newInputStream(Paths.get(location)), password.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);
        return tmf;
    }

    /**
     * Pauses the current thread for the specified duration.
     *
     * @param millis The duration in milliseconds to pause the thread.
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Validates that the provided value is not null.
     *
     * @param value        The value to validate.
     * @param errorMessage The error message to include in the exception if the value is null.
     * @return The validated value.
     * @throws InvalidConfigurationException If the value is null.
     */
    private String validateNotNull(String value, String errorMessage) throws InvalidConfigurationException {
        if (value == null) {
            throw new InvalidConfigurationException(errorMessage);
        }
        return value;
    }

    /**
     * Validates and sets a single value using the provided setter function.
     *
     * @param value     The value to validate and set.
     * @param fieldName The name of the field being validated.
     * @param setter    The setter function to apply the value.
     * @throws InvalidConfigurationException If the value is null.
     */
    private void validateAndSet(String value,
                                String fieldName,
                                java.util.function.Consumer<String> setter)
            throws InvalidConfigurationException {
        if (value != null) {
            setter.accept(value);
        } else {
            throw new InvalidConfigurationException("Mandatory parameter '" + fieldName + "' is not set.");
        }
    }

    /**
     * Validates and sets an array of values using the provided setter function.
     *
     * @param values    The array of values to validate and set.
     * @param fieldName The name of the field being validated.
     * @param setter    The setter function to apply the values.
     * @throws InvalidConfigurationException If the array is null or empty.
     */
    private void validateAndSet(String[] values,
                                String fieldName,
                                java.util.function.Consumer<String[]> setter)
            throws InvalidConfigurationException {
        if (values != null && values.length > 0) {
            setter.accept(values);
        } else {
            throw new InvalidConfigurationException("Mandatory parameter '" + fieldName + "' is not set.");
        }
    }
}
