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
package org.wso2.integration.connector.operations;

import com.google.gson.JsonObject;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.PublisherBuilder;

import org.apache.axis2.AxisFault;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.wso2.integration.connector.connection.RabbitMQConnection;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.utils.ClientKey;
import org.wso2.integration.connector.utils.Error;
import org.wso2.integration.connector.utils.RabbitMQDeclarationLockManager;
import org.wso2.integration.connector.utils.RabbitMQPublisherCallBack;
import org.wso2.integration.connector.utils.RabbitMQUtils;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.wso2.integration.connector.utils.RabbitMQConstants.CONNECTION_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CONNECTOR_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_EXCHANGE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_PUBLISH_TIMEOUT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DIRECT_EXCHANGE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.PUBLISHER_CONFIRMS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.PUBLISH_TIMEOUT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.ROUTING_KEY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.TOPIC_EXCHANGE_TYPE;

/**
 * This class is responsible for publishing messages to a RabbitMQ broker.
 * It handles the creation of queues, exchanges, and bindings if they are not already declared.
 * The class also manages the publishing of messages with a specified timeout and handles
 * any errors that occur during the process.
 * This class is designed to work within the WSO2 Integration framework and uses
 * RabbitMQ as the messaging system.
 */
public class PublishMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(PublishMessage.class);
    private volatile boolean isDeclared = false;
    /**
     * Publishes a message to a RabbitMQ queue or exchange. This method retrieves the connection,
     * prepares the publisher, sends the message, and handles the response or errors.
     *
     * @param messageContext   The Synapse message context containing the message details.
     * @param responseVariable The variable to store the response (not used in this method).
     * @param overwriteBody    Whether to overwrite the message body with the response (not used in this method).
     */
    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {

        try {
            // Retrieve the connection handler and RabbitMQ connection
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            String connectionName = getConnectionName(messageContext);
            RabbitMQConnection rabbitMQConnection =
                    (RabbitMQConnection) handler.getConnection(CONNECTOR_NAME, connectionName);
            Connection clientConnection = rabbitMQConnection.getClientConnection();

            // Extract parameters from the message context
            String queue = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, QUEUE_NAME);
            String exchange = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, EXCHANGE_NAME);
            String exchangeType = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, EXCHANGE_TYPE);
            String routingKey = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, ROUTING_KEY);

            if (!isDeclared) {
                // Retrieve or create the ReentrantLock associated with this
                // connectionName (fine-grained locking).
                Lock connectionSpecificLock = RabbitMQDeclarationLockManager
                        .getDeclarationLockMap().computeIfAbsent(connectionName, k -> new ReentrantLock());
                // Acquire the specific lock for this connectionName.
                connectionSpecificLock.lock();
                try {
                    // Double-check: Re-check 'isDeclared' after acquiring the lock
                    // to prevent duplicate declaration if another thread just finished.
                    if (!isDeclared) {
                        try (Management management = clientConnection.management()) {
                            Properties rabbitmqProperties = new Properties();
                            RabbitMQUtils.populateRabbitMQProperties(messageContext, rabbitmqProperties);
                            RabbitMQUtils.declareQueue(management, queue, rabbitmqProperties);
                            RabbitMQUtils.declareExchange(management, exchange, rabbitmqProperties);
                            RabbitMQUtils.bindQueueToExchange(management, queue, exchange, rabbitmqProperties);
                            isDeclared = true;

                        } catch (AmqpException e) {
                            String errorDetail = "An AMQP exception occurred while declaring the " +
                                    "RabbitMQ resources (queue/exchange/binding).";
                            handleError(
                                    messageContext, e, RabbitMQUtils.getErrorCode(e),
                                    errorDetail, responseVariable);
                        } catch (Exception e) {
                            String errorDetail = "Error while declaring the RabbitMQ resources.";
                            handleError(
                                    messageContext, e, RabbitMQUtils.getErrorCode(e),
                                    errorDetail, responseVariable);
                        }
                    }
                } finally {
                    connectionSpecificLock.unlock();
                }
            }
            // Parse the publishing timeout from the message context
            long publishTimeout;
            String publishTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, PUBLISH_TIMEOUT);
            if (publishTimeoutStr != null) {
                try {
                    publishTimeout = Long.parseLong(publishTimeoutStr);
                } catch (NumberFormatException e) {
                    // Log a warning and use the default timeout if the provided value is invalid
                    log.warn("[" + connectionName + "] Invalid value for " + PUBLISH_TIMEOUT
                            + " : '" + publishTimeoutStr + "'. Using default: "
                            + DEFAULT_PUBLISH_TIMEOUT, e);
                    publishTimeout = DEFAULT_PUBLISH_TIMEOUT;
                }
            } else {
                // Use the default timeout if no value is provided
                publishTimeout = DEFAULT_PUBLISH_TIMEOUT;
            }
            // Create the publisher
            Publisher publisher =
                    getPublisher(queue, exchange, exchangeType, routingKey, clientConnection, rabbitMQConnection);

            // Prepare the message to be sent
            String messageID = messageContext.getMessageID();
            byte[] messageBody = RabbitMQUtils.getRequestMessageBody(messageContext);
            Message message = publisher.message(messageBody).messageId(messageID);
            RabbitMQUtils.populateRequestMessageProperties(messageContext, message);

            // Publish the message and handle the callback
            RabbitMQPublisherCallBack publisherCallBack = new RabbitMQPublisherCallBack(messageID);
            try {

                publisher.publish(message, publisherCallBack);
                // Determine if publisher confirms are enabled
                boolean publisherConfirms;
                try {
                    publisherConfirms = Boolean.parseBoolean((String) ConnectorUtils.lookupTemplateParamater(
                            messageContext, PUBLISHER_CONFIRMS));
                } catch (Exception e) {
                    // Log a warning and use the default value if the provided value is invalid
                    log.warn("[" + connectionName + "] Invalid value for " + PUBLISHER_CONFIRMS
                            + ". Using default: true", e);
                    publisherConfirms = true;
                }

                if (!publisherConfirms) {
                    // Handle asynchronous publishing without publisher confirms
                    handleConnectorResponse(messageContext, responseVariable, false,
                            RabbitMQUtils.buildSuccessResponse(messageID), null, null);
                    publisherCallBack.result.thenRunAsync(() -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[" + connectionName + "] Message with message id: "
                                    + messageID + " successfully published");
                        }
                    });
                } else {
                    // Handle synchronous publishing with publisher confirms
                    publisherCallBack.result.thenAccept((status) -> {
                        handleConnectorResponse(
                                messageContext, responseVariable, false,
                                RabbitMQUtils.buildSuccessResponse(messageID), null, null);
                        if (log.isDebugEnabled()) {
                            log.debug("[" + connectionName + "] Message with message id: "
                                    + messageID + " successfully published");
                        }
                    }).exceptionally(throwable -> {
                        // Handle errors during synchronous publishing
                        String errorDetail;
                        if (throwable.getCause() instanceof RabbitMQConnectorException) {
                            handleConnectorResponse(
                                    messageContext, responseVariable, false,
                                    RabbitMQUtils.buildErrorResponse(
                                            messageContext, throwable.getCause(), Error.OPERATION_ERROR),
                                    null, null);
                        } else {
                            errorDetail = "Error occurred while performing rabbitmq:publishMessage "
                                    + "message with message id: " + messageID;
                            handleError(
                                    messageContext, new Exception(throwable.getCause()), Error.OPERATION_ERROR,
                                    errorDetail, responseVariable);
                        }
                        return null;
                    }).get(publishTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (TimeoutException e) {
                // Handle timeout errors during publishing
                String errorDetail = "Message with message id: " + messageID + " timed out after "
                        + publishTimeout + " ms.";
                handleError(
                        messageContext, e, RabbitMQUtils.getErrorCode(e),
                        errorDetail, responseVariable);
            } catch (ExecutionException | InterruptedException e) {
                // Handle execution or interruption errors during publishing
                String errorDetail = "Error occurred while performing rabbitmq:publishMessage operation with "
                        + "message id: " + messageID;
                handleError(
                        messageContext, e, RabbitMQUtils.getErrorCode(e),
                        errorDetail, responseVariable);
            } finally {
                // Return the connection to the pool
                handler.returnConnection(CONNECTOR_NAME, connectionName, rabbitMQConnection);
            }
        } catch (AxisFault e) {
            // Handle AxisFault errors during the operation
            String errorDetail = "AxisFault error occurred while performing rabbitmq:publishMessage operation.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail, responseVariable);
        } catch (Exception e) {
            // Handle generic errors during the operation
            String errorDetail = "Error occurred while performing rabbitmq:publishMessage operation.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail, responseVariable);
        }
    }


    /**
     * Sets error to context and handle.
     *
     * @param msgCtx           Message Context to set info
     * @param e                Exception associated
     * @param error            Error code
     * @param errorDetail      Error detail
     * @param responseVariable Response variable name
     */
    private void handleError(MessageContext msgCtx, Exception e, Error error, String errorDetail,
                             String responseVariable) {

        JsonObject resultJSON = RabbitMQUtils.buildErrorResponse(msgCtx, e, error);
        handleConnectorResponse(msgCtx, responseVariable, false, resultJSON, null, null);
        handleException(errorDetail, e, msgCtx);
    }

    /**
     * Retrieves or creates a publisher for the given parameters. The publisher is cached
     * for reuse to improve performance.
     *
     * @param queue            The name of the RabbitMQ queue.
     * @param exchange         The name of the RabbitMQ exchange.
     * @param exchangeType     The type of the RabbitMQ exchange.
     * @param routingKey       The routing key for the message.
     * @param clientConnection The RabbitMQ connection.
     * @param connection       The RabbitMQ connection wrapper.
     * @return The Publisher instance.
     */
    private Publisher getPublisher(String queue, String exchange,
                                   String exchangeType, String routingKey,
                                   Connection clientConnection, RabbitMQConnection connection) {
        ClientKey key = new ClientKey(queue, exchange, routingKey, null);
        if (queue == null && exchange == null && routingKey == null) {
            throw new SynapseException("Publisher client initialization failed due to insufficient information." +
                    " Provide at least one: Queue, Exchange, or Routing Key.");
        }
        if (exchangeType == null) {
            exchangeType = DEFAULT_EXCHANGE_TYPE;
        }
        if (exchange != null && routingKey == null) {
            if (exchangeType.equalsIgnoreCase(DIRECT_EXCHANGE_TYPE)) {
                throw new SynapseException("Error while initializing Publisher Client. " +
                        "RoutingKey is required for DIRECT exchange.");
            } else if (exchangeType.equalsIgnoreCase(TOPIC_EXCHANGE_TYPE)) {
                throw new SynapseException("Error while initializing Publisher Client. " +
                        "RoutingKey is required for TOPIC exchange.");
            }
        }
        return connection.getPublisherCache().computeIfAbsent(key, k -> {
            try {
                PublisherBuilder builder = clientConnection.publisherBuilder();

                if (queue != null) {
                    builder.queue(queue);
                }
                if (exchange != null) {
                    builder.exchange(exchange);
                    if (routingKey != null) {
                        builder.key(routingKey);
                    }
                }
                return builder.build();
            } catch (AmqpException.AmqpConnectionException e) {
                throw new SynapseException("Failed to create RabbitMQ Publisher Client! " + e.getMessage(), e);
            }
        });
    }

    /**
     * Retrieves the connection name from the message context. Throws an exception
     * if the connection name is not set.
     *
     * @param messageContext The Synapse message context.
     * @return The connection name.
     * @throws RabbitMQConnectorException If the connection name is not set.
     */
    private String getConnectionName(MessageContext messageContext) throws RabbitMQConnectorException {

        String connectionName = (String) messageContext.getProperty(CONNECTION_NAME);
        if (connectionName == null) {
            throw new RabbitMQConnectorException("Connection name is not set.");
        }
        return connectionName;
    }
}
