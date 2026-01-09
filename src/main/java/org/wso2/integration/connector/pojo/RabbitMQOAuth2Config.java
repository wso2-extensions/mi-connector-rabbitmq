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

import org.apache.commons.lang3.StringUtils;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;

import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_OAUTH2_GRANT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.OAUTH2_PASSWORD_GRANT_TYPE;

/**
 * This class represents the configuration for OAuth2 authentication in RabbitMQ.
 * It includes properties such as token endpoint, grant type, client ID, client secret,
 * username, and password, along with their respective validation logic.
 */
public class RabbitMQOAuth2Config {

    private String tokenEndpoint;
    private String grantType;
    private String clientId;
    private String clientSecret;
    private String userName;
    private String password;

    /**
     * Retrieves the token endpoint for OAuth2 authentication.
     *
     * @return The token endpoint URL.
     */
    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    /**
     * Sets the token endpoint for OAuth2 authentication.
     *
     * @param tokenEndpoint The token endpoint URL.
     * @throws RabbitMQConnectorException If the token endpoint is empty or null.
     */
    public void setTokenEndpoint(String tokenEndpoint) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(tokenEndpoint)) {
            this.tokenEndpoint = tokenEndpoint;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'tokenEndpoint' is not set.");
        }
    }

    /**
     * Retrieves the grant type for OAuth2 authentication.
     *
     * @return The grant type.
     */
    public String getGrantType() {
        return grantType;
    }

    /**
     * Sets the grant type for OAuth2 authentication.
     * If the grant type is empty, it defaults to the predefined constant.
     *
     * @param grantType The grant type.
     */
    public void setGrantType(String grantType) {
        if (StringUtils.isNotEmpty(grantType)) {
            this.grantType = grantType;
        } else {
            this.grantType = DEFAULT_OAUTH2_GRANT_TYPE;
        }
    }

    /**
     * Retrieves the client ID for OAuth2 authentication.
     *
     * @return The client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the client ID for OAuth2 authentication.
     *
     * @param clientId The client ID.
     * @throws RabbitMQConnectorException If the client ID is empty or null.
     */
    public void setClientId(String clientId) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(clientId)) {
            this.clientId = clientId;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'clientId' is not set.");
        }
    }

    /**
     * Retrieves the client secret for OAuth2 authentication.
     *
     * @return The client secret.
     */
    public String getClientSecret() {
        return clientSecret;
    }

    /**
     * Sets the client secret for OAuth2 authentication.
     *
     * @param clientSecret The client secret.
     * @throws RabbitMQConnectorException If the client secret is empty or null.
     */
    public void setClientSecret(String clientSecret) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(clientSecret)) {
            this.clientSecret = clientSecret;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'clientSecret' is not set.");
        }
    }

    /**
     * Retrieves the username for OAuth2 authentication.
     *
     * @return The username.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the username for OAuth2 authentication.
     * Throws an exception if the username is not set for the 'password' grant type.
     *
     * @param userName The username.
     * @throws RabbitMQConnectorException If the username is empty or null for the 'password' grant type.
     */
    public void setUserName(String userName) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(userName)) {
            this.userName = userName;
        } else if (getGrantType().equalsIgnoreCase(OAUTH2_PASSWORD_GRANT_TYPE)) {
            throw new RabbitMQConnectorException("Mandatory parameter 'userName' is not " +
                    "set for the 'password' grant type.");
        }
    }

    /**
     * Retrieves the password for OAuth2 authentication.
     *
     * @return The password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password for OAuth2 authentication.
     * Throws an exception if the password is not set for the 'password' grant type.
     *
     * @param password The password.
     * @throws RabbitMQConnectorException If the password is empty or null for the 'password' grant type.
     */
    public void setPassword(String password) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(password)) {
            this.password = password; // Set the password if it is not empty
        } else if (getGrantType().equalsIgnoreCase(OAUTH2_PASSWORD_GRANT_TYPE)) {
            throw new RabbitMQConnectorException("Mandatory parameter 'password' is not " +
                    "set for the 'password' grant type.");
        }
    }
}
