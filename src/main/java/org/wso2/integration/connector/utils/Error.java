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
package org.wso2.integration.connector.utils;

/**
 * Enum representing error codes and messages for RabbitMQ integration.
 * Each error has a unique code and a corresponding message.
 */
public enum Error {
    CONNECTION_ERROR("701201", "RABBITMQ:CONNECTION_ERROR"),
    INVALID_CONFIGURATION("701202", "RABBITMQ:INVALID_CONFIGURATION"),
    OPERATION_ERROR("701203", "RABBITMQ:OPERATION_ERROR"),
    PUBLISH_TIMEOUT("701204", "RABBITMQ:PUBLISH_TIMEOUT"),
    AXIS_FAULT_ERROR("701205", "RABBITMQ:AXIS_FAULT_ERROR"),
    AMQP_ERROR("701206", "RABBITMQ:AMQP_ERROR"),
    RABBITMQ_GENERAL_ERROR("701207", "RABBITMQ:RABBITMQ_GENERAL_ERROR");

    private final String code;
    private final String message;

    /**
     * Create an error code.
     *
     * @param code    error code represented by number
     * @param message error message
     */
    Error(String code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * Retrieves the error code.
     *
     * @return The error code as a string.
     */
    public String getErrorCode() {
        return this.code;
    }

    /**
     * Retrieves the error message.
     *
     * @return The error message as a string.
     */
    public String getErrorMessage() {
        return this.message;
    }
}
