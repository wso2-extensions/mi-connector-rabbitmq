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
import com.rabbitmq.client.amqp.RpcClient;
import com.rabbitmq.client.amqp.RpcClientBuilder;

import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.connection.RabbitMQConnection;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.utils.ClientKey;
import org.wso2.integration.connector.utils.Error;
import org.wso2.integration.connector.utils.RabbitMQDeclarationLockManager;
import org.wso2.integration.connector.utils.RabbitMQMessageContext;
import org.wso2.integration.connector.utils.RabbitMQRoundRobinAddressSelector;
import org.wso2.integration.connector.utils.RabbitMQUtils;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.wso2.integration.connector.utils.RabbitMQConstants.CONNECTION_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CONNECTOR_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_REQUEST_TIMEOUT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_NAME;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_TIMEOUT;

/**
 * This class is responsible for sending Remote Procedure Call (RPC) messages
 * to a RabbitMQ broker and handling the response. It encapsulates the logic
 * for constructing, sending, and receiving RPC messages, ensuring reliable
 * communication between the client and the RabbitMQ server.
 * The class can be used to implement synchronous request-response patterns
 * in RabbitMQ-based messaging systems.
 */
public class SendRpc extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SendRpc.class);
    private volatile boolean isDeclared = false;

    /**
     * Executes the RabbitMQ RPC operation. This method retrieves the connection,
     * prepares the RPC client, sends the message, and handles the response or errors.
     *
     * @param messageContext   The Synapse message context containing the message details.
     * @param responseVariable The variable to store the response.
     * @param overwriteBody    Whether to overwrite the message body with the response.
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
            String replyToQueue = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, REPLY_TO_QUEUE_NAME);

            // Declare the queue and exchange if not already declared
            if (!isDeclared) {
                // Retrieve or create the ReentrantLock associated with this unique
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
                            RabbitMQUtils.declareReplyToQueue(management, replyToQueue, rabbitmqProperties);
                            isDeclared = true;
                        } catch (AmqpException e) {
                            String errorDetail = "An AMQP exception occurred while declaring the " +
                                    "RabbitMQ queue or exchange.";
                            handleError(
                                    messageContext, e, RabbitMQUtils.getErrorCode(e),
                                    errorDetail, responseVariable, overwriteBody);
                        } catch (Exception e) {
                            String errorDetail = "Error while declaring the RabbitMQ queue/exchange.";
                            handleError(
                                    messageContext, e, RabbitMQUtils.getErrorCode(e),
                                    errorDetail, responseVariable, overwriteBody);
                        }
                    }
                } finally {
                    connectionSpecificLock.unlock();
                }
            }

            // Parse the request timeout from the message context
            long requestTimeout;
            String requestTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, REQUEST_TIMEOUT);
            if (requestTimeoutStr != null) {
                try {
                    requestTimeout = Long.parseLong(requestTimeoutStr);
                } catch (NumberFormatException e) {
                    log.warn("[" + connectionName + "] Invalid value for " + REQUEST_TIMEOUT
                            + " : '" + requestTimeoutStr + "'. Using default: "
                            + DEFAULT_REQUEST_TIMEOUT, e);
                    requestTimeout = DEFAULT_REQUEST_TIMEOUT;
                }
            } else {
                requestTimeout = DEFAULT_REQUEST_TIMEOUT;
            }

            // Create the RPC client
            RpcClient rpcClient = getRpcClient(queue, replyToQueue, clientConnection, rabbitMQConnection);

            // Prepare the message to be sent
            String messageID = messageContext.getMessageID();
            byte[] messageBody = RabbitMQUtils.getRequestMessageBody(messageContext);
            Message request = rpcClient.message(messageBody).messageId(messageID);
            RabbitMQUtils.populateRequestMessageProperties(messageContext, request);

            try {
                // Send the message and wait for the response
                CompletableFuture<Message> rpcCallBack = rpcClient.publish(request);
                Message response = rpcCallBack.get(requestTimeout, TimeUnit.MILLISECONDS);

                // Handle the response
                RabbitMQRoundRobinAddressSelector addressSelector = rabbitMQConnection.getAddressSelector();
                String host = addressSelector.getCurrentAddress().host();
                int port = addressSelector.getCurrentAddress().port();

                RabbitMQMessageContext rabbitMQMessageContext =
                        new RabbitMQMessageContext(response, host, port, replyToQueue);
                org.apache.axis2.context.MessageContext axis2MsgCtx =
                        ((Axis2MessageContext) messageContext).getAxis2MessageContext();

                RabbitMQUtils.buildResponseMessage(rabbitMQMessageContext, axis2MsgCtx);
                axis2MsgCtx.setProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS,
                        RabbitMQUtils.getResponseTransportHeaders(rabbitMQMessageContext, axis2MsgCtx));

            } catch (TimeoutException e) {
                // Handle timeout errors
                String errorDetail = "Message with message id: " + messageID + " timed out after "
                        + requestTimeout + " ms.";
                handleError(
                        messageContext, e, RabbitMQUtils.getErrorCode(e),
                        errorDetail, responseVariable, overwriteBody);
            } catch (ExecutionException | InterruptedException e) {
                // Handle execution or interruption errors
                String errorDetail = "Error occurred while performing rabbitmq:sendRpc " +
                        "operation with message id: " + messageID;
                handleError(
                        messageContext, e, RabbitMQUtils.getErrorCode(e),
                        errorDetail, responseVariable, overwriteBody);
            } finally {
                // Return the connection to the pool
                handler.returnConnection(CONNECTOR_NAME, connectionName, rabbitMQConnection);
            }

        } catch (AxisFault e) {
            // Handle AxisFault errors
            String errorDetail = "AxisFault error occurred while performing rabbitmq:sendRpc operation.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail, responseVariable, overwriteBody);
        } catch (Exception e) {
            // Handle generic errors
            String errorDetail = "Error occurred while performing rabbitmq:sendRpc operation.";
            handleError(messageContext, e, RabbitMQUtils.getErrorCode(e), errorDetail, responseVariable, overwriteBody);
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
     * @param overwriteBody    Overwrite body
     */
    private void handleError(MessageContext msgCtx, Exception e, Error error, String errorDetail,
                             String responseVariable, boolean overwriteBody) {

        JsonObject resultJSON = RabbitMQUtils.buildErrorResponse(msgCtx, e, error);
        handleConnectorResponse(msgCtx, responseVariable, overwriteBody, resultJSON, null, null);
        handleException(errorDetail, e, msgCtx);
    }

    /**
     * Retrieves or creates an RPC client for the given parameters. The client is
     * cached for reuse to improve performance.
     *
     * @param queue            The name of the RabbitMQ queue.
     * @param replyToQueue     The name of the reply-to queue.
     * @param clientConnection The RabbitMQ connection.
     * @param connection       The RabbitMQ connection wrapper.
     * @return The RPC client instance.
     */
    private RpcClient getRpcClient(String queue, String replyToQueue,
                                   Connection clientConnection,
                                   RabbitMQConnection connection) {
        ClientKey key = new ClientKey(queue, null, null, replyToQueue);
        if (queue == null) {
            throw new SynapseException("Error while initializing the RPC Client. Queue is not provided.");
        }

        if (replyToQueue == null) {
            throw new SynapseException("Error while initializing the RPC client. Reply-To Queue is not provided.");
        }
        return connection.getRpcClientCache().computeIfAbsent(key, k -> {
            try {
                RpcClientBuilder builder = clientConnection.rpcClientBuilder()
                        .requestAddress()
                        .queue(queue)
                        .rpcClient()
                        .replyToQueue(replyToQueue)
                        .requestPostProcessor((msg, corrId) ->
                                msg.correlationId(corrId)
                                        .replyToAddress().queue(replyToQueue).message())
                        .correlationIdExtractor(Message::correlationId);
                return builder.build();
            } catch (AmqpException.AmqpConnectionException e) {
                throw new SynapseException("Failed to create RabbitMQ RPC Client! " + e.getMessage(), e);
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
