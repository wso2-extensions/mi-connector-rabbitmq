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

import java.util.Objects;

/**
 * Represents a unique key for identifying a RabbitMQ client configuration.
 * This class is used to encapsulate the details of a RabbitMQ client, such as
 * queue name, exchange name, routing key, reply-to queue name, and publish timeout.
 */
public class ClientKey {
    String queueName;
    String exchangeName;
    String routingKey;
    String replyToQueueName;

    /**
     * Constructs a `ClientKey` object with the specified parameters.
     *
     * @param queue            The name of the queue.
     * @param exchange         The name of the exchange.
     * @param routingKey       The routing key.
     * @param replyToQueueName The name of the reply-to queue.
     */
    public ClientKey(String queue, String exchange, String routingKey, String replyToQueueName) {
        this.queueName = queue;
        this.exchangeName = exchange;
        this.routingKey = routingKey;
        this.replyToQueueName = replyToQueueName;
    }

    /**
     * Compares this `ClientKey` object to another object for equality.
     *
     * @param o The object to compare with.
     * @return `true` if the objects are equal, `false` otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientKey)) {
            return false;
        }
        ClientKey that = (ClientKey) o;
        return Objects.equals(queueName, that.queueName) &&
                Objects.equals(exchangeName, that.exchangeName) &&
                Objects.equals(routingKey, that.routingKey) &&
                Objects.equals(replyToQueueName, that.replyToQueueName);
    }

    /**
     * Computes the hash code for this `ClientKey` object.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return Objects.hash(queueName, exchangeName, routingKey, replyToQueueName);
    }

    /**
     * Returns a string representation of this `ClientKey` object.
     *
     * @return A string representation of the object.
     */
    @Override
    public String toString() {
        return (queueName != null ? queueName : "null") + "|" +
                (exchangeName != null ? exchangeName : "null") + "|" +
                (routingKey != null ? routingKey : "null") + "|" +
                (replyToQueueName != null ? replyToQueueName : "null");

    }
}
