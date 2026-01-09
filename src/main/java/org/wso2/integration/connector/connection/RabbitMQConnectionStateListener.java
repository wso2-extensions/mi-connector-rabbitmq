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
package org.wso2.integration.connector.connection;

import com.rabbitmq.client.amqp.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class listens to the state changes of a RabbitMQ connection and provides
 * appropriate handling mechanisms for connection events such as connection
 * establishment, closure, and errors. It ensures that the application can
 * respond to connection state changes effectively, maintaining reliable
 * communication with the RabbitMQ broker.
 * <p>
 * The class can be extended or customized to implement specific behaviors
 * based on the application's requirements.
 */
public class RabbitMQConnectionStateListener implements Resource.StateListener {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionStateListener.class);
    private final String connectionName;
    public RabbitMQConnectionStateListener(String connectionName) {
        this.connectionName = connectionName;
    }

    @Override
    public void handle(Resource.Context context) {

        if (context.currentState() == Resource.State.RECOVERING && context.previousState() == Resource.State.OPEN) {
            log.info("[" + connectionName + "] connection to the RabbitMQ broker started to recover.");
        }
        if (context.currentState() == Resource.State.OPEN && context.previousState() == Resource.State.RECOVERING) {
            log.info("[" + connectionName + "] connection to the RabbitMQ broker was recovered.");
        }

        if (context.currentState() == Resource.State.OPENING && context.previousState() == null) {
            log.info("[" + connectionName + "] connection to the RabbitMQ broker is opening.");
        }

        if (context.currentState() == Resource.State.OPEN && context.previousState() == Resource.State.OPENING) {
            log.info("[" + connectionName + "] connection to the RabbitMQ broker is established.");
        }

        if (context.currentState() == Resource.State.CLOSING && context.previousState() == Resource.State.OPEN) {
            if (context.failureCause() != null) {
                log.warn("[" + connectionName + "] connection to the " +
                        "RabbitMQ broker is closing due to: " + context.failureCause());
            } else {
                log.info("[" + connectionName + "] connection to the RabbitMQ broker is closing.");
            }
        }
        if (context.currentState() == Resource.State.CLOSED && context.previousState() == Resource.State.CLOSING) {
            if (context.failureCause() == null) {
                log.info("[" + connectionName + "] connection to the RabbitMQ broker is closed.");
            } else {
                log.warn("[" + connectionName + "] connection to the " +
                        "RabbitMQ broker is closed due to: " + context.failureCause());
            }

        }
    }
}
