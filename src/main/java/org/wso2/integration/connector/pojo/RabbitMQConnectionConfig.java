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
package org.wso2.integration.connector.pojo;

import com.rabbitmq.client.amqp.ConnectionSettings;
import org.apache.commons.lang3.StringUtils;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;

import java.time.Duration;

import static org.wso2.integration.connector.utils.RabbitMQConstants.ConnectionRecoveryPolicy;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_IDLE_TIMEOUT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_RETRY_COUNT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_RETRY_INTERVAL;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_SASL_MECHANISM;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_VIRTUAL_HOST;

/**
 * This class encapsulates the configuration details for a RabbitMQ connection.
 * It includes properties such as server URLs, authentication mechanisms, virtual host,
 * connection timeouts, and recovery policies. The class provides methods to set and retrieve
 * these configurations, ensuring proper validation and default values where applicable.
 * <p>
 * This class is designed to be used within the WSO2 Integration framework to manage
 * RabbitMQ connection settings effectively.
 */
public class RabbitMQConnectionConfig {
    private String[] serverUrls;
    private String saslMechanism;
    private Boolean oAuth2Enabled;
    private String username;
    private String password;
    private String virtualHost;
    private Duration connectionIdleTimeout;
    private Integer connectionEstablishRetryCount;
    private Long connectionEstablishRetryInterval;
    private String connectionRecoveryPolicyType;
    private Duration connectionRecoveryInitialDelay;
    private Duration connectionRecoveryInterval;
    private Duration connectionRecoveryTimeout;

    /**
     * Retrieves the RabbitMQ server URLs.
     *
     * @return An array of server URLs.
     */
    public String[] getServerUrls() {

        return serverUrls;
    }

    /**
     * Sets the RabbitMQ server URLs after validating the input.
     * Ensures the URLs are in the correct format and prepends the "amqp://" protocol.
     *
     * @param serverUrls A comma-separated string of server URLs.
     * @throws RabbitMQConnectorException If the input is empty or invalid.
     */
    public void setServerUrls(String serverUrls) throws RabbitMQConnectorException {
        // Check if the input string is empty
        if (StringUtils.isEmpty(serverUrls)) {
            throw new RabbitMQConnectorException("Mandatory parameter 'serverUrls' is not set.");
        }

        // Split the input string into an array of URLs
        String[] urls = serverUrls.split(",");
        for (int i = 0; i < urls.length; i++) {
            // Validate the format of each URL
            String[] parts = urls[i].split(":");
            if (parts.length != 2) {
                throw new RabbitMQConnectorException("Invalid server url: "
                        + urls[i] + ". Expected format 'host:port'.");
            }
            try {
                // Ensure the port is a valid number
                Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new RabbitMQConnectorException("Invalid server url: "
                        + urls[i] + ". Port is not a number.");
            }
            // Prepend the "amqp://" protocol to the URL
            urls[i] = "amqp://" + urls[i].trim();
        }
        // Assign the validated URLs to the class property
        this.serverUrls = urls;
    }


    /**
     * Retrieves the SASL mechanism used for authentication.
     *
     * @return The SASL mechanism as a string.
     */
    public String getSaslMechanism() {
        return saslMechanism;
    }

    /**
     * Sets the SASL mechanism for authentication. Defaults to the
     * RabbitMQ constant if the input is empty.
     *
     * @param saslMechanism The SASL mechanism to be set.
     */
    public void setSaslMechanism(String saslMechanism) {
        if (StringUtils.isNotEmpty(saslMechanism)) {
            this.saslMechanism = saslMechanism;
        } else {
            this.saslMechanism = DEFAULT_SASL_MECHANISM;
        }

    }

    /**
     * Checks if OAuth2 is enabled for the connection.
     *
     * @return `true` if OAuth2 is enabled, `false` otherwise.
     */
    public boolean isOAuth2Enabled() {
        return oAuth2Enabled;
    }

    /**
     * Sets the OAuth2 enablement status by parsing the input string.
     * Defaults to `false` if the input is empty.
     *
     * @param oAuth2Enabled The string representation of OAuth2 enablement.
     */
    public void setOAuth2Enabled(String oAuth2Enabled) {
        if (StringUtils.isNotEmpty(oAuth2Enabled)) {
            this.oAuth2Enabled = Boolean.parseBoolean(oAuth2Enabled);
        } else {
            this.oAuth2Enabled = false;
        }

    }

    /**
     * Retrieves the username for authentication.
     *
     * @return The username as a string.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username for authentication. Throws an exception if the
     * username is mandatory but not provided.
     *
     * @param username The username to be set.
     * @throws RabbitMQConnectorException If the username is mandatory but not set.
     */
    public void setUsername(String username) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(username)) {
            this.username = username;
        } else if (!isOAuth2Enabled() && getSaslMechanism().equalsIgnoreCase(ConnectionSettings.SASL_MECHANISM_PLAIN)) {
            throw new RabbitMQConnectorException("Mandatory parameter 'username' is not set.");
        }
    }


    /**
     * Retrieves the password used for authentication.
     *
     * @return The password as a string.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password for authentication. Throws an exception if the password
     * is mandatory but not provided. The password is required when OAuth2 is not
     * enabled and the SASL mechanism is set to "PLAIN".
     *
     * @param password The password to be set.
     * @throws RabbitMQConnectorException If the password is mandatory but not set.
     */
    public void setPassword(String password) throws RabbitMQConnectorException {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(password)) {
            // Set the password
            this.password = password;
        } else if (!isOAuth2Enabled() && getSaslMechanism().equalsIgnoreCase(ConnectionSettings.SASL_MECHANISM_PLAIN)) {
            // Throw an exception if the password is mandatory but not provided
            throw new RabbitMQConnectorException("Mandatory parameter 'password' is not set.");
        }
    }

    /**
     * Retrieves the virtual host for the RabbitMQ connection.
     *
     * @return The virtual host as a string.
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * Sets the virtual host for the RabbitMQ connection. If the input is empty,
     * it defaults to the predefined virtual host.
     *
     * @param virtualHost The virtual host to be set.
     */
    public void setVirtualHost(String virtualHost) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(virtualHost)) {
            // Set the virtual host
            this.virtualHost = virtualHost;
        } else {
            // Default to the predefined virtual host
            this.virtualHost = DEFAULT_VIRTUAL_HOST;
        }
    }

    /**
     * Retrieves the connection idle timeout duration.
     *
     * @return The idle timeout as a `Duration` object.
     */
    public Duration getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    /**
     * Sets the connection idle timeout. Parses the input string to a `Duration`
     * and defaults to a predefined value if the input is empty.
     *
     * @param connectionIdleTimeout The idle timeout as a string.
     */
    public void setConnectionIdleTimeout(String connectionIdleTimeout) {
        if (StringUtils.isNotEmpty(connectionIdleTimeout)) {
            this.connectionIdleTimeout = Duration.ofMillis(Long.parseLong(connectionIdleTimeout));
        } else {
            this.connectionIdleTimeout = Duration.ofMillis(DEFAULT_IDLE_TIMEOUT);
        }
    }

    /**
     * Retrieves the number of retry attempts for establishing a connection.
     *
     * @return The retry count as an integer.
     */
    public int getConnectionEstablishRetryCount() {
        return connectionEstablishRetryCount;
    }

    /**
     * Sets the number of retry attempts for establishing a connection.
     * If the input is empty, it defaults to the predefined retry count.
     *
     * @param connectionEstablishRetryCount The retry count as a string.
     */
    public void setConnectionEstablishRetryCount(String connectionEstablishRetryCount) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionEstablishRetryCount)) {
            // Parse the string to an integer and set the retry count
            this.connectionEstablishRetryCount = Integer.parseInt(connectionEstablishRetryCount);
        } else {
            // Default to the predefined retry count
            this.connectionEstablishRetryCount = DEFAULT_RETRY_COUNT;
        }
    }

    /**
     * Retrieves the interval between retry attempts for establishing a connection.
     *
     * @return The retry interval as a Long value.
     */
    public Long getConnectionEstablishRetryInterval() {
        return connectionEstablishRetryInterval;
    }

    /**
     * Sets the interval between retry attempts for establishing a connection.
     * If the input is empty, it defaults to the predefined retry interval.
     *
     * @param connectionEstablishRetryInterval The retry interval as a string.
     */
    public void setConnectionEstablishRetryInterval(String connectionEstablishRetryInterval) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionEstablishRetryInterval)) {
            // Parse the string to a long value and set the retry interval
            this.connectionEstablishRetryInterval = Long.parseLong(connectionEstablishRetryInterval);
        } else {
            // Default to the predefined retry interval
            this.connectionEstablishRetryInterval = DEFAULT_RETRY_INTERVAL;
        }
    }

    /**
     * Retrieves the connection recovery policy type.
     *
     * @return The recovery policy type as a string.
     */
    public String getConnectionRecoveryPolicyType() {
        return connectionRecoveryPolicyType;
    }

    /**
     * Sets the connection recovery policy type.
     * If the input is empty, it defaults to a fixed policy with initial delay and timeout.
     *
     * @param connectionRecoveryPolicyType The recovery policy type as a string.
     */
    public void setConnectionRecoveryPolicyType(String connectionRecoveryPolicyType) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionRecoveryPolicyType)) {
            // Set the recovery policy type
            this.connectionRecoveryPolicyType = connectionRecoveryPolicyType;
        } else {
            // Default to a fixed policy with initial delay and timeout
            this.connectionRecoveryPolicyType = String.valueOf(
                    ConnectionRecoveryPolicy.FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT);
        }
    }

    /**
     * Retrieves the initial delay for connection recovery.
     *
     * @return The initial delay as a `Duration` object.
     */
    public Duration getConnectionRecoveryInitialDelay() {
        return connectionRecoveryInitialDelay;
    }

    /**
     * Sets the initial delay for connection recovery.
     * If the input is empty, it defaults to the predefined initial delay.
     *
     * @param connectionRecoveryInitialDelay The initial delay as a string.
     */
    public void setConnectionRecoveryInitialDelay(String connectionRecoveryInitialDelay) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionRecoveryInitialDelay)) {
            // Parse the string to a `Duration` object and set the initial delay
            this.connectionRecoveryInitialDelay = Duration.ofMillis(
                    Long.parseLong(connectionRecoveryInitialDelay));
        } else {
            // Default to the predefined initial delay
            this.connectionRecoveryInitialDelay = Duration.ofMillis(
                    DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY);
        }
    }

    /**
     * Retrieves the interval between recovery attempts.
     *
     * @return The recovery interval as a `Duration` object.
     */
    public Duration getConnectionRecoveryInterval() {
        return connectionRecoveryInterval;
    }

    /**
     * Sets the interval between recovery attempts.
     * If the input is empty, it defaults to the predefined recovery interval.
     *
     * @param connectionRecoveryInterval The recovery interval as a string.
     */
    public void setConnectionRecoveryInterval(String connectionRecoveryInterval) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionRecoveryInterval)) {
            // Parse the string to a `Duration` object and set the recovery interval
            this.connectionRecoveryInterval = Duration.ofMillis(
                    Long.parseLong(connectionRecoveryInterval));
        } else {
            // Default to the predefined recovery interval
            this.connectionRecoveryInterval = Duration.ofMillis(
                    DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL);
        }
    }

    /**
     * Retrieves the timeout duration for connection recovery.
     *
     * @return The recovery timeout as a `Duration` object.
     */
    public Duration getConnectionRecoveryTimeout() {
        return connectionRecoveryTimeout;
    }

    /**
     * Sets the timeout duration for connection recovery.
     * If the input is empty, it defaults to the predefined recovery timeout.
     *
     * @param connectionRecoveryTimeout The recovery timeout as a string.
     */
    public void setConnectionRecoveryTimeout(String connectionRecoveryTimeout) {
        // Check if the input string is not empty
        if (StringUtils.isNotEmpty(connectionRecoveryTimeout)) {
            // Parse the string to a `Duration` object and set the recovery timeout
            this.connectionRecoveryTimeout = Duration.ofMillis(
                    Long.parseLong(connectionRecoveryTimeout));
        } else {
            // Default to the predefined recovery timeout
            this.connectionRecoveryTimeout = Duration.ofMillis(
                    DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT);
        }
    }
}
