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

import com.rabbitmq.client.amqp.Publisher;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;

import java.util.concurrent.CompletableFuture;

/**
 * This class implements the RabbitMQ `Publisher.Callback` interface to handle the status of published messages.
 * It uses a `CompletableFuture` to track the result of the message publishing operation.
 */
public class RabbitMQPublisherCallBack implements Publisher.Callback {

    private final String messageID;

    /**
     * Constructor for RabbitMQPublisherCallBack.
     *
     * @param messageID The RabbitMQ message context associated with this callback.
     */
    public RabbitMQPublisherCallBack(String messageID) {
        this.messageID = messageID;
    }

    // CompletableFuture to track the result of the message publishing operation
    public CompletableFuture<Publisher.Status> result = new CompletableFuture<>();

    /**
     * Handles the status of the published message.
     *
     * @param context The context of the message publishing operation.
     */
    @Override
    public void handle(Publisher.Context context) {
        // Handle all possible statuses of the message
        switch (context.status()) {
            case ACCEPTED:
                result.complete(Publisher.Status.ACCEPTED);
                break;
            case REJECTED:
                result.completeExceptionally(
                        new RabbitMQConnectorException("Message with message id: " + messageID +
                                " was rejected.", context.failureCause()));
                break;
            case RELEASED:
                result.completeExceptionally(
                        new RabbitMQConnectorException("Message with message id: " + messageID +
                                " was released.", context.failureCause()));
                break;
            default:
                result.completeExceptionally(
                        new RabbitMQConnectorException("Message with message id: " + messageID +
                                " encountered an unknown status: " + context.status(), context.failureCause()));
                break;
        }
    }
}
