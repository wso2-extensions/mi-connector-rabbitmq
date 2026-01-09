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
package org.wso2.integration.connector.exception;

/**
 * Exception thrown when an invalid configuration is detected.
 * This is used to indicate issues with the configuration parameters
 * provided for setting up the RabbitMQ connection or other components.
 */
public class InvalidConfigurationException extends Exception {

    /**
     * Constructs a new InvalidConfigurationException with the specified detail message.
     *
     * @param message the detail message
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructs a new InvalidConfigurationException with the specified detail message
     * and cause.
     *
     * @param message the detail message
     * @param cause   the cause of the exception
     */
    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
