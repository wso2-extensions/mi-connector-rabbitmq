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

import org.apache.commons.lang.StringUtils;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;

import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_KEYSTORE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_SSL_VERSION;

/**
 * This class represents the configuration for establishing a secure connection
 * to a RabbitMQ server. It includes SSL-related settings such as keystore and
 * truststore configurations, as well as options for hostname verification and
 * SSL version.
 */
public class RabbitMQSecureConnectionConfig extends RabbitMQConnectionConfig {

    private boolean trustEverything;
    private boolean hostnameVerificationEnabled;
    private String keyStoreLocation;
    private String keyStoreType;
    private String keyStorePassword;
    private String trustStoreLocation;
    private String trustStoreType;
    private String trustStorePassword;
    private String sslVersion;


    /**
     * Checks if the "trust everything" option is enabled for the RabbitMQ connection.
     *
     * @return `true` if "trust everything" is enabled, `false` otherwise.
     */
    public boolean isTrustEverything() {
        return trustEverything;
    }

    /**
     * Enables or disables the "trust everything" option for the RabbitMQ connection.
     * Converts the provided string value to a boolean.
     *
     * @param trustEverything A string representing whether "trust everything" is enabled ("true" or "false").
     */
    public void setTrustEverything(String trustEverything) {
        if (StringUtils.isNotEmpty(trustEverything)) {
            this.trustEverything = Boolean.parseBoolean(trustEverything);
        }
    }

    /**
     * Checks if hostname verification is enabled for the RabbitMQ connection.
     *
     * @return `true` if hostname verification is enabled, `false` otherwise.
     */
    public boolean isHostnameVerificationEnabled() {
        return hostnameVerificationEnabled;
    }

    /**
     * Enables or disables hostname verification for the RabbitMQ connection.
     * Converts the provided string value to a boolean.
     *
     * @param hostnameVerificationEnabled A string representing whether
     * hostname verification is enabled ("true" or "false").
     */
    public void setHostnameVerificationEnabled(String hostnameVerificationEnabled) {
        if (StringUtils.isNotEmpty(hostnameVerificationEnabled)) {
            this.hostnameVerificationEnabled = Boolean.parseBoolean(hostnameVerificationEnabled);
        }
    }

    /**
     * Retrieves the location of the keystore used for SSL configuration.
     *
     * @return The keystore location.
     */
    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    /**
     * Sets the location of the keystore used for SSL configuration.
     *
     * @param keyStoreLocation The keystore location.
     * @throws RabbitMQConnectorException If the keystore location is empty or null.
     */
    public void setKeyStoreLocation(String keyStoreLocation) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(keyStoreLocation)) {
            this.keyStoreLocation = keyStoreLocation;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'keyStoreLocation' is not set.");
        }
    }

    /**
     * Retrieves the type of the keystore used for SSL configuration.
     *
     * @return The keystore type.
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Sets the type of the keystore used for SSL configuration.
     * Defaults to the predefined constant if the provided value is empty.
     *
     * @param keyStoreType The keystore type.
     */
    public void setKeyStoreType(String keyStoreType) {
        if (StringUtils.isNotEmpty(keyStoreType)) {
            this.keyStoreType = keyStoreType;
        } else {
            this.keyStoreType = DEFAULT_KEYSTORE_TYPE;
        }
    }


    /**
     * Retrieves the password for the keystore used for SSL configuration.
     *
     * @return The keystore password.
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * Sets the password for the keystore used for SSL configuration.
     *
     * @param keyStorePassword The keystore password.
     * @throws RabbitMQConnectorException If the keystore password is empty or null.
     */
    public void setKeyStorePassword(String keyStorePassword) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(keyStorePassword)) {
            this.keyStorePassword = keyStorePassword;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'keyStorePassword' is not set.");
        }
    }

    /**
     * Retrieves the location of the truststore used for SSL configuration.
     *
     * @return The truststore location.
     */
    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    /**
     * Sets the location of the truststore used for SSL configuration.
     *
     * @param trustStoreLocation The truststore location.
     * @throws RabbitMQConnectorException If the truststore location is empty or null.
     */
    public void setTrustStoreLocation(String trustStoreLocation) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(trustStoreLocation)) {
            this.trustStoreLocation = trustStoreLocation;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'trustStoreLocation' is not set.");
        }
    }

    /**
     * Retrieves the type of the truststore used for SSL configuration.
     *
     * @return The truststore type.
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * Sets the type of the truststore used for SSL configuration.
     * Defaults to the predefined constant if the provided value is empty.
     *
     * @param trustStoreType The truststore type.
     */
    public void setTrustStoreType(String trustStoreType) {
        if (StringUtils.isNotEmpty(trustStoreType)) {
            this.trustStoreType = trustStoreType;
        } else {
            this.trustStoreType = DEFAULT_KEYSTORE_TYPE;
        }
    }

    /**
     * Retrieves the password for the truststore used for SSL configuration.
     *
     * @return The truststore password.
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * Sets the password for the truststore used for SSL configuration.
     *
     * @param trustStorePassword The truststore password.
     * @throws RabbitMQConnectorException If the truststore password is empty or null.
     */
    public void setTrustStorePassword(String trustStorePassword) throws RabbitMQConnectorException {
        if (StringUtils.isNotEmpty(trustStorePassword)) {
            this.trustStorePassword = trustStorePassword;
        } else {
            throw new RabbitMQConnectorException("Mandatory parameter 'trustStorePassword' is not set.");
        }
    }

    /**
     * Retrieves the SSL version used for the RabbitMQ connection.
     *
     * @return The SSL version.
     */
    public String getSslVersion() {
        return sslVersion;
    }

    /**
     * Sets the SSL version used for the RabbitMQ connection.
     * Defaults to the predefined constant if the provided value is empty.
     *
     * @param sslVersion The SSL version.
     */
    public void setSslVersion(String sslVersion) {
        if (StringUtils.isNotEmpty(sslVersion)) {
            this.sslVersion = sslVersion;
        } else {
            this.sslVersion = DEFAULT_SSL_VERSION;
        }
    }
}
