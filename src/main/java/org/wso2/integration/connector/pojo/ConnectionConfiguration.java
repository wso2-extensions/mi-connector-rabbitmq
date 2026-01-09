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
package org.wso2.integration.connector.pojo;

import org.apache.commons.lang3.StringUtils;

/**
 * This class represents the configuration for a RabbitMQ connection.
 * It encapsulates details such as the connection name, RabbitMQ connection
 * configuration, OAuth2 configuration, and SSL settings. The class provides
 * getter and setter methods to manage these properties and ensures proper
 * parsing of boolean values for SSL and OAuth2 enablement.
 * <p>
 * This class is designed to be used within the WSO2 Integration framework
 * to configure and manage RabbitMQ connections.
 */
public class ConnectionConfiguration {
    private String connectionName;
    private RabbitMQConnectionConfig connectionConfig;
    private RabbitMQOAuth2Config oAuth2Config;
    private Boolean sslEnabled;
    private Boolean oAuth2Enabled;

    /**
     * Retrieves the name of the RabbitMQ connection.
     *
     * @return The connection name as a string.
     */
    public String getConnectionName() {
        return connectionName;
    }


    /**
     * Sets the name of the RabbitMQ connection.
     *
     * @param connectionName The connection name to be set.
     */
    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    /**
     * Retrieves the RabbitMQ connection configuration.
     *
     * @return The `RabbitMQConnectionConfig` object containing the connection details.
     */
    public RabbitMQConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * Sets the RabbitMQ connection configuration.
     *
     * @param connectionConfig The `RabbitMQConnectionConfig` object to be set.
     */
    public void setConnectionConfig(RabbitMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    /**
     * Retrieves the OAuth2 configuration for the RabbitMQ connection.
     *
     * @return The `RabbitMQOAuth2Config` object containing the OAuth2 details.
     */
    public RabbitMQOAuth2Config getOAuth2Config() {
        return oAuth2Config;
    }

    /**
     * Sets the OAuth2 configuration for the RabbitMQ connection.
     *
     * @param oAuth2Config The `RabbitMQOAuth2Config` object to be set.
     */
    public void setOAuth2Config(RabbitMQOAuth2Config oAuth2Config) {
        this.oAuth2Config = oAuth2Config;
    }

    /**
     * Checks if SSL is enabled for the RabbitMQ connection.
     *
     * @return `true` if SSL is enabled, `false` otherwise.
     */
    public Boolean getSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets the SSL enablement for the RabbitMQ connection.
     * Parses the input string to a boolean value.
     *
     * @param sslEnabled The string representation of the SSL enablement.
     */
    public void setSslEnabled(String sslEnabled) {
        if (StringUtils.isNotEmpty(sslEnabled)) {
            this.sslEnabled = Boolean.parseBoolean(sslEnabled);
        } else {
            this.sslEnabled = false;
        }
    }

    /**
     * Checks if OAuth2 is enabled for the RabbitMQ connection.
     *
     * @return `true` if OAuth2 is enabled, `false` otherwise.
     */
    public Boolean getOAuth2Enabled() {
        return oAuth2Enabled;
    }

    /**
     * Sets the OAuth2 enablement for the RabbitMQ connection.
     * Parses the input string to a boolean value.
     *
     * @param oAuth2Enabled The string representation of the OAuth2 enablement.
     */
    public void setOAuth2Enabled(String oAuth2Enabled) {
        if (StringUtils.isNotEmpty(oAuth2Enabled)) {
            this.oAuth2Enabled = Boolean.parseBoolean(oAuth2Enabled);
        } else {
            this.oAuth2Enabled = false;
        }
    }
}
