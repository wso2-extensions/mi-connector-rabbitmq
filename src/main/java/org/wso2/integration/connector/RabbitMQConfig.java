/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.integration.connector;

import com.rabbitmq.client.amqp.AmqpException;

import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;

import org.wso2.integration.connector.connection.RabbitMQConnection;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.exception.InvalidConfigurationException;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.pojo.ConnectionConfiguration;
import org.wso2.integration.connector.pojo.RabbitMQConnectionConfig;
import org.wso2.integration.connector.pojo.RabbitMQOAuth2Config;
import org.wso2.integration.connector.pojo.RabbitMQSecureConnectionConfig;
import org.wso2.integration.connector.utils.Error;
import org.wso2.integration.connector.utils.RabbitMQConstants;
import org.wso2.integration.connector.utils.RabbitMQDeclarationLockManager;
import org.wso2.integration.connector.utils.RabbitMQUtils;
import org.wso2.integration.connector.utils.RabbitMQXMLInputFactory;

/**
 * Configuration class for setting up RabbitMQ messaging.
 * This class defines the necessary beans and configurations
 * required to connect to a RabbitMQ broker, such as queues,
 * exchanges, and bindings.
 * It ensures that the application can send messages
 * using RabbitMQ.
 */
public class RabbitMQConfig extends AbstractConnector implements ManagedLifecycle {

    /**
     * Establishes a connection to the RabbitMQ broker using the provided configuration.
     * If the connection does not already exist, it creates a new one.
     *
     * @param messageContext The message context containing connection parameters.
     * @throws ConnectException If an error occurs while establishing the connection.
     */
    @Override
    public void connect(MessageContext messageContext) throws ConnectException {

        String connectionName = (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_NAME);

        try {
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            if (!handler.checkIfConnectionExists(RabbitMQConstants.CONNECTOR_NAME, connectionName)) {
                ConnectionConfiguration configuration = getConnectionConfigFromContext(messageContext);
                RabbitMQConnection rabbitMQConnection = new RabbitMQConnection(configuration);
                try {
                    handler.createConnection(RabbitMQConstants.CONNECTOR_NAME, connectionName, rabbitMQConnection,
                            messageContext);
                } catch (NoSuchMethodError e) {
                    handler.createConnection(RabbitMQConstants.CONNECTOR_NAME, connectionName, rabbitMQConnection);
                }
            }
        } catch (RabbitMQConnectorException | InvalidConfigurationException e) {
            String errorDetail = "[" + connectionName + "] Failed to initiate RabbitMQ connector configuration.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail);
        } catch (AmqpException.AmqpConnectionException e) {
            String errorDetail = "[" + connectionName + "] Failed to create the RabbitMQ connection.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail);
        }
    }

    /**
     * Retrieves the connection configuration from the message context.
     * This method determines whether SSL or OAuth2 is enabled and sets the appropriate configuration.
     *
     * @param messageContext The message context containing connection parameters.
     * @return The constructed `ConnectionConfiguration` object.
     * @throws RabbitMQConnectorException If an error occurs while retrieving the configuration.
     */
    private ConnectionConfiguration getConnectionConfigFromContext(MessageContext messageContext)
            throws RabbitMQConnectorException {

        ConnectionConfiguration configuration = new ConnectionConfiguration();
        configuration.setConnectionName((String) getParameter(messageContext, RabbitMQConstants.CONNECTION_NAME));
        configuration.setSslEnabled((String) getParameter(messageContext, RabbitMQConstants.SSL_ENABLED));
        configuration.setOAuth2Enabled((String) getParameter(messageContext, RabbitMQConstants.OAUTH2_ENABLED));

        if (configuration.getSslEnabled()) {
            RabbitMQSecureConnectionConfig secureConfig = getRabbitMQSecureConnectionConfigFromContext(messageContext);
            configuration.setConnectionConfig(secureConfig);
        } else {
            RabbitMQConnectionConfig connectionConfig = getRabbitMQConnectionConfigFromContext(messageContext, null);
            configuration.setConnectionConfig(connectionConfig);
        }

        if (configuration.getOAuth2Enabled()) {
            setOAuth2ConfigToConnectionConfig(messageContext, configuration);
        }
        return configuration;
    }

    /**
     * Retrieves the RabbitMQ connection configuration from the message context.
     * If a configuration object is provided, it updates it with the parameters from the message context.
     * Otherwise, it creates a new configuration object and populates it.
     *
     * @param messageContext The message context containing connection parameters.
     * @param config         The existing RabbitMQ connection configuration (can be null).
     * @return The populated RabbitMQ connection configuration.
     * @throws RabbitMQConnectorException If an error occurs while retrieving the configuration.
     */
    private RabbitMQConnectionConfig getRabbitMQConnectionConfigFromContext(MessageContext messageContext,
                                                                            RabbitMQConnectionConfig config)
            throws RabbitMQConnectorException {

        if (config == null) {
            config = new RabbitMQConnectionConfig();
        }
        config.setServerUrls(
                (String) getParameter(messageContext, RabbitMQConstants.SERVER_URLS));
        config.setVirtualHost(
                (String) getParameter(messageContext, RabbitMQConstants.VIRTUAL_HOST));
        config.setSaslMechanism(
                (String) getParameter(messageContext, RabbitMQConstants.SASL_MECHANISM));
        config.setOAuth2Enabled(
                (String) getParameter(messageContext, RabbitMQConstants.OAUTH2_ENABLED));
        config.setUsername(
                (String) getParameter(messageContext, RabbitMQConstants.SERVER_USERNAME));
        config.setPassword(
                (String) getParameter(messageContext, RabbitMQConstants.SERVER_PASSWORD));
        config.setConnectionIdleTimeout(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_IDLE_TIMEOUT));
        config.setConnectionEstablishRetryCount(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_COUNT));
        config.setConnectionEstablishRetryInterval(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_INTERVAL));
        config.setConnectionRecoveryPolicyType(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_RECOVERY_POLICY_TYPE));
        config.setConnectionRecoveryInitialDelay(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_RECOVERY_INITIAL_DELAY));
        config.setConnectionRecoveryInterval(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_RECOVERY_INTERVAL));
        config.setConnectionRecoveryTimeout(
                (String) getParameter(messageContext, RabbitMQConstants.CONNECTION_RECOVERY_TIMEOUT));

        return config;

    }

    /**
     * Retrieves the RabbitMQ secure connection configuration from the message context.
     * This method populates the secure connection configuration with SSL-related parameters
     * such as keystore, truststore, and SSL version.
     *
     * @param messageContext The message context containing secure connection parameters.
     * @return The populated `RabbitMQSecureConnectionConfig` object.
     * @throws RabbitMQConnectorException If an error occurs while retrieving the configuration.
     */

    private RabbitMQSecureConnectionConfig getRabbitMQSecureConnectionConfigFromContext(MessageContext messageContext)
            throws RabbitMQConnectorException {

        RabbitMQSecureConnectionConfig config = new RabbitMQSecureConnectionConfig();
        getRabbitMQConnectionConfigFromContext(messageContext, config);
        config.setTrustEverything(
                (String) getParameter(messageContext, RabbitMQConstants.TRUST_EVERYTHING));
        config.setHostnameVerificationEnabled(
                (String) getParameter(messageContext, RabbitMQConstants.HOSTNAME_VERIFICATION_ENABLED));
        if (config.isTrustEverything()) {
            // When 'trustEverything' is enabled, skip further SSL configuration
            return config;
        }
        config.setKeyStoreLocation(
                (String) getParameter(messageContext, RabbitMQConstants.KEYSTORE_LOCATION));
        config.setKeyStoreType(
                (String) getParameter(messageContext, RabbitMQConstants.KEYSTORE_TYPE));
        config.setKeyStorePassword(
                (String) getParameter(messageContext, RabbitMQConstants.KEYSTORE_PASSWORD));
        config.setTrustStoreLocation(
                (String) getParameter(messageContext, RabbitMQConstants.TRUSTSTORE_LOCATION));
        config.setTrustStoreType(
                (String) getParameter(messageContext, RabbitMQConstants.TRUSTSTORE_TYPE));
        config.setTrustStorePassword(
                (String) getParameter(messageContext, RabbitMQConstants.TRUSTSTORE_PASSWORD));
        config.setSslVersion(
                (String) getParameter(messageContext, RabbitMQConstants.SSL_VERSION));

        return config;

    }

    /**
     * Sets the OAuth2 configuration to the connection configuration.
     * This method populates the OAuth2 configuration with parameters such as token endpoint,
     * grant type, client ID, client secret, and optionally username and password for the 'password' grant type.
     *
     * @param messageContext The message context containing OAuth2 parameters.
     * @param configuration  The connection configuration to which the OAuth2 configuration will be set.
     * @throws RabbitMQConnectorException If an error occurs while setting the OAuth2 configuration.
     */
    private void setOAuth2ConfigToConnectionConfig(MessageContext messageContext,
                                                   ConnectionConfiguration configuration)
            throws RabbitMQConnectorException {

        RabbitMQOAuth2Config oAuth2Config = new RabbitMQOAuth2Config();
        oAuth2Config.setTokenEndpoint(
                (String) getParameter(messageContext, RabbitMQConstants.TOKEN_ENDPOINT));
        oAuth2Config.setGrantType(
                (String) getParameter(messageContext, RabbitMQConstants.GRANT_TYPE));
        oAuth2Config.setClientId(
                (String) getParameter(messageContext, RabbitMQConstants.CLIENT_ID));
        oAuth2Config.setClientSecret(
                (String) getParameter(messageContext, RabbitMQConstants.CLIENT_SECRET));
        if (oAuth2Config.getGrantType().equals(RabbitMQConstants.OAUTH2_PASSWORD_GRANT_TYPE)) {
            oAuth2Config.setUserName(
                    (String) getParameter(messageContext, RabbitMQConstants.OAUTH2_USERNAME));
            oAuth2Config.setPassword(
                    (String) getParameter(messageContext, RabbitMQConstants.OAUTH2_PASSWORD));
        }
        configuration.setOAuth2Config(oAuth2Config);
    }

    /**
     * Sets error to context and handle.
     *
     * @param msgCtx      Message Context to set info
     * @param e           Exception associated
     * @param error       Error code
     * @param errorDetail Error detail
     */
    private void handleError(MessageContext msgCtx, Exception e, Error error, String errorDetail) {
        RabbitMQUtils.setErrorPropertiesToMessage(msgCtx, e, error);
        handleException(errorDetail, e, msgCtx);
    }

    /**
     * Shuts down all RabbitMQ connections associated with this connector.
     * This method is called during the destruction of the connector.
     */
    @Override
    public void destroy() {
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        handler.shutdownConnections(RabbitMQConstants.CONNECTOR_NAME);
        RabbitMQDeclarationLockManager.destroyMap();
        RabbitMQXMLInputFactory.destroyXMLInputFactoryHolder();
    }

    /**
     * Cleans up and shuts down all RabbitMQ connections associated with this connector.
     * This method is invoked during the destruction phase of the connector lifecycle.
     */
    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        //nothing to do here
    }
}
