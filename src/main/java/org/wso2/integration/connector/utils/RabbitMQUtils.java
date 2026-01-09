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
package org.wso2.integration.connector.utils;

import com.google.gson.JsonObject;
import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.ByteCapacity;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Message;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.impl.builder.StAXBuilder;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.soap.SOAP11Constants;
import org.apache.axiom.soap.SOAP12Constants;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.base.AckDecision;
import org.apache.axis2.transport.base.AckDecisionCallback;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.http.protocol.HTTP;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.util.AXIOMUtils;
import org.apache.synapse.util.InlineExpressionUtil;
import org.jaxen.JaxenException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.exception.InvalidConfigurationException;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import static org.wso2.integration.connector.utils.RabbitMQConstants.ACKNOWLEDGEMENT_DECISION;
import static org.wso2.integration.connector.utils.RabbitMQConstants.AXIS2_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CHARACTER_SET_ENCODING;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CHARSET;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CLASSIC_MAX_PRIORITY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CLASSIC_VERSION;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CONTENT_ENCODING;
import static org.wso2.integration.connector.utils.RabbitMQConstants.CORRELATION_ID;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_CHARSET;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_EXCHANGE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_QUEUE_OVERFLOW_STRATEGY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.DEFAULT_QUEUE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EMPTY_LIST;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_ARGUMENTS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_AUTODECLARE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_AUTO_DELETE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.EXCHANGE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.HEADERS_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.HEADER_EXCHANGE_ARGUMENTS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.INITIAL_MEMBER_COUNT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.INTERNAL_TRANSACTION_COUNTED;
import static org.wso2.integration.connector.utils.RabbitMQConstants.JSON_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.JSON_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.MESSAGE_ID;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_ARGUMENTS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_AUTODECLARE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_AUTO_DELETE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_EXCLUSIVE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_OVERFLOW_STRATEGY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUEUE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUORUM_DEAD_LETTER_STRATEGY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.QUORUM_DELIVERY_LIMIT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.RABBITMQ_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_ARGUMENTS;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_AUTODECLARE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_AUTO_DELETE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_CLASSIC_MAX_PRIORITY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_CLASSIC_VERSION;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_EXCLUSIVE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_INITIAL_MEMBER_COUNT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_OVERFLOW_STRATEGY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_QUORUM_DEAD_LETTER_STRATEGY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_QUORUM_DELIVERY_LIMIT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_STREAM_MAX_AGE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_STREAM_MAX_SEGMENT_SIZE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REPLY_TO_QUEUE_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_BODY_JSON_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_BODY_TEXT_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_BODY_TYPE_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_BODY_XML_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.REQUEST_CHARSET_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.RESPONSE_BODY_TYPE_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.RESPONSE_CHARSET_IDENTIFIER;
import static org.wso2.integration.connector.utils.RabbitMQConstants.ROUTING_KEY;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SINGLE_QUOTE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SOAP11_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SOAP12_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.SOAP_ACTION;
import static org.wso2.integration.connector.utils.RabbitMQConstants.STREAM_MAX_AGE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.STREAM_MAX_SEGMENT_SIZE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.TEXT_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.TEXT_ELEMENT;
import static org.wso2.integration.connector.utils.RabbitMQConstants.TEXT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.XML_CONTENT_TYPE;
import static org.wso2.integration.connector.utils.RabbitMQConstants.XML_TYPE;

/**
 * Utility class for RabbitMQ operations.
 * This class provides methods for managing RabbitMQ queues, exchanges, bindings,
 * and message properties, as well as handling errors and configurations.
 */
public class RabbitMQUtils {
    private static final Log log = LogFactory.getLog(RabbitMQUtils.class);
    /**
     * Sets the acknowledgement decision for the given message context.
     *
     * @param messageContext The message context containing the acknowledgement decision property.
     * @param decisionStr    The decision string to be set
     * (e.g., "ACKNOWLEDGE", "SET_ROLLBACK_ONLY","SET_REQUEUE_ON_ROLLBACK").
     * @throws RabbitMQConnectorException If no valid AckDecisionCallback is found in the message context.
     */
    public static void setDecision(MessageContext messageContext, String decisionStr)
            throws RabbitMQConnectorException {
        // Retrieve the acknowledgement decision property from the message context
        Object ackDecisionProperty = messageContext.getProperty(ACKNOWLEDGEMENT_DECISION);

        // If the property is not found, attempt to retrieve it from the Axis2 message context
        if (ackDecisionProperty == null) {
            ackDecisionProperty = ((Axis2MessageContext) messageContext)
                    .getAxis2MessageContext().getProperty(ACKNOWLEDGEMENT_DECISION);
        }

        // Check if the property is an instance of AckDecisionCallback
        if (ackDecisionProperty instanceof AckDecisionCallback) {
            // Complete the acknowledgement decision using the provided decision string
            ((AckDecisionCallback) ackDecisionProperty).complete(AckDecision.fromString(decisionStr));
        } else {
            throw new RabbitMQConnectorException("No AckDecisionCallback found in message context to " +
                    "set the acknowledgement decision.");
        }
    }

    /**
     * Sets error properties to the message context based on the provided exception and error.
     *
     * @param messageContext The message context to which the error properties will be set.
     * @param e              The exception associated with the error.
     * @param error          The error object containing error details.
     */
    public static void setErrorPropertiesToMessageContext(MessageContext messageContext, Throwable e, Error error) {

        messageContext.setProperty(SynapseConstants.ERROR_CODE, error.getErrorCode());
        messageContext.setProperty(SynapseConstants.ERROR_MESSAGE, error.getErrorMessage());
        messageContext.setProperty(SynapseConstants.ERROR_DETAIL, e.getMessage());
        messageContext.setProperty(SynapseConstants.ERROR_EXCEPTION, e);
    }

    /**
     * Builds a JSON response indicating a successful operation.
     *
     * @param messageId The ID of the successfully processed message.
     * @return A JSON object containing the success status and message ID.
     */
    public static JsonObject buildSuccessResponse(String messageId) {

        // Create a new JSON payload
        JsonObject resultJson = new JsonObject();

        // Add the basic success information
        resultJson.addProperty("success", "true");
        resultJson.addProperty("messageId", messageId);

        return resultJson;
    }

    /**
     * Builds a JSON response indicating an error during the operation.
     *
     * @param messageContext The message context associated with the error.
     * @param e              The exception that occurred.
     * @param error          The error details to include in the response.
     * @return A JSON object containing the error details.
     */
    public static JsonObject buildErrorResponse(MessageContext messageContext, Throwable e, Error error) {

        // Create a new JSON payload
        JsonObject resultJson = new JsonObject();

        // Add the basic success information
        resultJson.addProperty("success", "false");

        JsonObject errorJson = new JsonObject();

        setErrorPropertiesToMessageContext(messageContext, e, error);

        errorJson.addProperty("code", error.getErrorCode());
        errorJson.addProperty("message", error.getErrorMessage());
        errorJson.addProperty("detail", e.getMessage());

        resultJson.add("error", errorJson);

        return resultJson;
    }

    /**
     * Declares a RabbitMQ queue based on the provided properties.
     * This method supports CLASSIC, QUORUM, and STREAM queue types and configures
     * various queue parameters such as auto-delete, exclusive access, overflow strategy,
     * and dead-letter queue settings.
     *
     * @param management The RabbitMQ management interface for queue operations.
     * @param queueName  The name of the queue to be declared.
     * @param properties The properties containing queue configuration details.
     */
    public static void declareQueue(Management management, String queueName, Properties properties) {
        // Validate if the queue name is provided and if auto-declare is enabled
        if (StringUtils.isEmpty(queueName) || !BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(QUEUE_AUTODECLARE)), true)) {
            return;
        }

        // Determine the queue type (CLASSIC, QUORUM, or STREAM)
        Management.QueueType queueType = Management.QueueType.valueOf(
                properties.getProperty(QUEUE_TYPE, DEFAULT_QUEUE_TYPE));
        boolean autoDelete = BooleanUtils.toBoolean(
                properties.getProperty(QUEUE_AUTO_DELETE, "false"));
        Management.QueueSpecification queueBuilder = management.queue()
                .name(queueName)
                .type(queueType)
                .autoDelete(autoDelete); // Set auto-delete property

        // Configure the queue based on its type
        switch (queueType) {
            case CLASSIC:
                boolean exclusive = BooleanUtils.toBoolean(
                        properties.getProperty(QUEUE_EXCLUSIVE, "false"));
                queueBuilder.exclusive(exclusive);
                // Additional CLASSIC queue configurations
                String maxPriority = properties.getProperty(CLASSIC_MAX_PRIORITY);
                String version = properties.getProperty(CLASSIC_VERSION);
                configureClassicQueue(queueBuilder, maxPriority, version);
                break;
            case QUORUM:
                // Additional QUORUM queue configurations
                String initialMembersCountQuorum = properties.getProperty(INITIAL_MEMBER_COUNT);
                String deliveryLimit = properties.getProperty(QUORUM_DELIVERY_LIMIT);
                String deadLetterStrategy = properties.getProperty(QUORUM_DEAD_LETTER_STRATEGY);
                configureQuorumQueue(queueBuilder, initialMembersCountQuorum, deliveryLimit, deadLetterStrategy);
                break;
            case STREAM:
                // Additional STREAM queue configurations
                String initialMembersCountStream = properties.getProperty(INITIAL_MEMBER_COUNT);
                String maxAge = properties.getProperty(STREAM_MAX_AGE);
                String maxSegmentSize = properties.getProperty(STREAM_MAX_SEGMENT_SIZE);
                configureStreamQueue(queueBuilder, initialMembersCountStream, maxAge, maxSegmentSize);
                break;
        }

        // Configure optional queue arguments
        String arguments = properties.getProperty(QUEUE_ARGUMENTS);
        configureQueueArguments(queueBuilder, arguments);

        // Configure overflow strategy and dead-letter queue for non-STREAM queues
        if (queueType != Management.QueueType.STREAM) {
            Management.OverflowStrategy overflowStrategy =
                    properties.containsKey(QUEUE_OVERFLOW_STRATEGY)
                            ? Management.OverflowStrategy.valueOf(
                            properties.getProperty(QUEUE_OVERFLOW_STRATEGY))
                            : DEFAULT_QUEUE_OVERFLOW_STRATEGY;

            // Adjust overflow strategy for QUORUM queues with incompatible dead-letter strategies
            if (queueType == Management.QueueType.QUORUM &&
                    QUORUM_DEAD_LETTER_STRATEGY.equalsIgnoreCase(
                            properties.getProperty(QUORUM_DEAD_LETTER_STRATEGY)) &&
                    overflowStrategy == Management.OverflowStrategy.DROP_HEAD) {
                overflowStrategy = Management.OverflowStrategy.REJECT_PUBLISH;
                log.warn("DROP_HEAD overflow strategy is not compatible with AT_LEAST_ONCE dead letter " +
                        "strategy for Quorum queues. Changing to REJECT_PUBLISH.");
            }

            queueBuilder.overflowStrategy(overflowStrategy); // Set the overflow strategy
        }

        // Declare the queue
        queueBuilder.declare();
    }

    /**
     * Configures CLASSIC queue-specific properties such as max priority and version.
     *
     * @param queueBuilder The queue specification builder.
     * @param maxPriority  The maximum priority for the CLASSIC queue.
     * @param version      The version of the CLASSIC queue.
     */
    private static void configureClassicQueue(Management.QueueSpecification queueBuilder,
                                              String maxPriority, String version) {
        if (maxPriority != null) {
            queueBuilder.classic().maxPriority(NumberUtils.toInt(maxPriority));
        }
        if (version != null) {
            Management.ClassicQueueVersion classicVersion = Management.ClassicQueueVersion.valueOf(version);
            queueBuilder.classic().version(classicVersion);
        }
    }

    /**
     * Configures QUORUM queue-specific properties such as initial member count,
     * delivery limit, and dead-letter strategy.
     *
     * @param queueBuilder       The queue specification builder.
     * @param initialMemberCount The initial member count for the QUORUM queue.
     * @param deliveryLimit      The delivery limit for the QUORUM queue.
     * @param deadLetterStrategy The dead-letter strategy for the QUORUM queue.
     */
    private static void configureQuorumQueue(Management.QueueSpecification queueBuilder,
                                             String initialMemberCount, String deliveryLimit,
                                             String deadLetterStrategy) {
        if (initialMemberCount != null) {
            queueBuilder.quorum().initialMemberCount(NumberUtils.toInt(initialMemberCount));
        }
        if (deliveryLimit != null) {
            queueBuilder.quorum().deliveryLimit(NumberUtils.toInt(deliveryLimit));
        }
        if (deadLetterStrategy != null) {
            queueBuilder.quorum().deadLetterStrategy(
                    Management.QuorumQueueDeadLetterStrategy.valueOf(deadLetterStrategy));
        }
    }

    /**
     * Configures STREAM queue-specific properties such as initial member count,
     * maximum age, and maximum segment size.
     *
     * @param queueBuilder       The queue specification builder.
     * @param initialMemberCount The initial member count for the STREAM queue.
     * @param maxAge             The maximum age for the STREAM queue.
     * @param maxSegmentSize     The maximum segment size for the STREAM queue.
     */
    private static void configureStreamQueue(Management.QueueSpecification queueBuilder, String initialMemberCount,
                                             String maxAge, String maxSegmentSize) {
        if (initialMemberCount != null) {
            queueBuilder.stream().initialMemberCount(NumberUtils.toInt(initialMemberCount));
        }
        if (maxAge != null) {
            queueBuilder.stream().maxAge(Duration.ofSeconds(NumberUtils.toLong(maxAge)));
        }
        if (maxSegmentSize != null) {
            queueBuilder.stream().maxSegmentSizeBytes(
                    ByteCapacity.B(NumberUtils.toLong(maxSegmentSize)));
        }
    }

    /**
     * Configures optional queue arguments based on the provided properties.
     *
     * @param queueBuilder The queue specification builder.
     * @param arguments    The optional queue arguments.
     */
    private static void configureQueueArguments(Management.QueueSpecification queueBuilder, String arguments) {
        if (StringUtils.isNotEmpty(arguments)) {
            Map<String, Object> argumentMap = getOptionalArguments(arguments);
            if (argumentMap != null) {
                argumentMap.forEach(queueBuilder::argument); // Add optional arguments to the queue
            }
        }
    }

    /**
     * Declares a RabbitMQ exchange based on the provided properties.
     * This method supports various exchange types and allows optional arguments.
     *
     * @param management   The RabbitMQ management interface for exchange operations.
     * @param exchangeName The name of the exchange to be declared.
     * @param properties   The properties containing exchange configuration details.
     */
    public static void declareExchange(Management management, String exchangeName, Properties properties) {
        boolean autoDeclare = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(EXCHANGE_AUTODECLARE)), true);

        // Proceed only if the exchange name is valid, auto-declare is enabled,
        // and the exchange is not a system exchange
        if (StringUtils.isNotEmpty(exchangeName) && autoDeclare && !exchangeName.startsWith("amq.")) {
            Management.ExchangeType exchangeType = Management.ExchangeType.valueOf(
                    properties.getProperty(EXCHANGE_TYPE, DEFAULT_EXCHANGE_TYPE));

            boolean autoDelete = BooleanUtils.toBoolean(
                    properties.getProperty(EXCHANGE_AUTO_DELETE, "false"));
            // Build the exchange specification
            Management.ExchangeSpecification exchangeBuilder = management.exchange()
                    .name(exchangeName)
                    .type(exchangeType)
                    .autoDelete(autoDelete);

            // Add optional arguments if specified
            String arguments = properties.getProperty(EXCHANGE_ARGUMENTS);
            if (StringUtils.isNotEmpty(arguments)) {
                Map<String, Object> argumentMap = getOptionalArguments(arguments);
                if (argumentMap != null) {
                    argumentMap.forEach(exchangeBuilder::argument);
                }
            }

            // Declare the exchange
            exchangeBuilder.declare();
        }
    }

    /**
     * Binds a RabbitMQ queue to an exchange based on the provided properties.
     * This method supports different exchange types such as HEADERS, FANOUT, DIRECT, and TOPIC.
     *
     * @param management   The RabbitMQ management interface for binding operations.
     * @param queueName    The name of the queue to bind.
     * @param exchangeName The name of the exchange to bind to.
     * @param properties   The properties containing binding configuration details.
     */
    public static void bindQueueToExchange(Management management, String queueName,
                                           String exchangeName, Properties properties) {
        // Validate that both the exchange name and queue name are provided
        if (StringUtils.isEmpty(exchangeName) || StringUtils.isEmpty(queueName)) {
            return;
        }

        // Build the binding specification
        Management.BindingSpecification bindingBuilder = management.binding()
                .sourceExchange(exchangeName)
                .destinationQueue(queueName);

        // Determine the exchange type and handle binding accordingly
        String exchangeType = properties
                .getProperty(EXCHANGE_TYPE, DEFAULT_EXCHANGE_TYPE);
        switch (Management.ExchangeType.valueOf(exchangeType)) {
            case HEADERS:
                handleHeadersExchange(bindingBuilder, queueName, exchangeName, properties);
                break;
            case FANOUT:
                bindingBuilder.bind();
                break;
            default:
                handleDirectOrTopicExchange(bindingBuilder, queueName, properties);
                break;
        }
    }

    /**
     * Handles the binding of a queue to a headers exchange.
     * This method processes the header arguments and binds the queue to the exchange
     * if valid arguments are provided.
     *
     * @param bindingBuilder The binding specification builder.
     * @param queueName      The name of the queue to bind.
     * @param exchangeName   The name of the headers exchange.
     * @param properties     The properties containing header arguments.
     */
    private static void handleHeadersExchange(Management.BindingSpecification bindingBuilder,
                                              String queueName, String exchangeName, Properties properties) {
        String headerArguments = properties.getProperty(HEADER_EXCHANGE_ARGUMENTS);

        // Return if no header arguments are specified
        if (StringUtils.isEmpty(headerArguments)) {
            if (log.isDebugEnabled()) {
                log.debug("No header arguments specified for the headers exchange. Hence not binding the queue: "
                        + queueName + " to the exchange: " + exchangeName);
            }
            return;
        }

        // Parse header arguments into a map
        Map<String, Object> headerArgsMap = getOptionalArguments(headerArguments);

        // Return if the parsed arguments are empty
        if (headerArgsMap == null || headerArgsMap.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("No header arguments specified for the headers exchange. Hence not binding the queue: "
                        + queueName + " to the exchange: " + exchangeName);
            }
            return;
        }

        // Add default "x-match" argument if not present
        headerArgsMap.putIfAbsent("x-match", "all");

        // Bind the queue to the exchange if there are valid arguments
        headerArgsMap.forEach(bindingBuilder::argument);
        bindingBuilder.bind();
    }

    /**
     * Handles the binding of a queue to a direct or topic exchange.
     * This method uses the routing key from the properties or defaults to the queue name.
     *
     * @param bindingBuilder The binding specification builder.
     * @param queueName      The name of the queue to bind.
     * @param properties     The properties containing the routing key.
     */
    private static void handleDirectOrTopicExchange(Management.BindingSpecification bindingBuilder,
                                                    String queueName, Properties properties) {
        // Use the routing key from properties or default to the queue name
        String routingKey = properties.getProperty(ROUTING_KEY, queueName);
        bindingBuilder.key(routingKey).bind();
    }

    /**
     * Declares a RabbitMQ reply-to queue based on the provided properties.
     * This method supports CLASSIC, QUORUM, and STREAM queue types and configures
     * various queue parameters such as auto-delete, exclusive access, overflow strategy,
     * and dead-letter queue settings.
     *
     * @param management The RabbitMQ management interface for queue operations.
     * @param queueName  The name of the reply-to queue to be declared.
     * @param properties The properties containing reply-to queue configuration details.
     */
    public static void declareReplyToQueue(Management management, String queueName, Properties properties) {
        // Validate if the queue name is provided and if auto-declare is enabled
        if (StringUtils.isEmpty(queueName) || !BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        properties.getProperty(REPLY_TO_QUEUE_AUTODECLARE)), true)) {
            return;
        }

        // Determine the queue type (CLASSIC, QUORUM, or STREAM)
        Management.QueueType queueType = Management.QueueType.valueOf(
                properties.getProperty(REPLY_TO_QUEUE_TYPE, DEFAULT_QUEUE_TYPE));
        boolean autoDelete = BooleanUtils.toBoolean(
                properties.getProperty(REPLY_TO_QUEUE_AUTO_DELETE, "false"));
        Management.QueueSpecification queueBuilder = management.queue()
                .name(queueName)
                .type(queueType)
                .autoDelete(autoDelete); // Set auto-delete property

        // Configure the queue based on its type
        switch (queueType) {
            case CLASSIC:
                boolean exclusive = BooleanUtils.toBoolean(
                        properties.getProperty(REPLY_TO_QUEUE_EXCLUSIVE, "false"));
                queueBuilder.exclusive(exclusive);
                // Additional CLASSIC queue configurations
                String maxPriority =
                        properties.getProperty(REPLY_TO_QUEUE_CLASSIC_MAX_PRIORITY);
                String version =
                        properties.getProperty(REPLY_TO_QUEUE_CLASSIC_VERSION);
                configureClassicQueue(queueBuilder, maxPriority, version);
                break;
            case QUORUM:
                // Additional QUORUM queue configurations
                String initialMemberCount =
                        properties.getProperty(REPLY_TO_QUEUE_INITIAL_MEMBER_COUNT);
                String deliveryLimit =
                        properties.getProperty(REPLY_TO_QUEUE_QUORUM_DELIVERY_LIMIT);
                String deadLetterStrategy =
                        properties.getProperty(REPLY_TO_QUEUE_QUORUM_DEAD_LETTER_STRATEGY);
                configureQuorumQueue(queueBuilder, initialMemberCount, deliveryLimit, deadLetterStrategy);
                break;
            case STREAM:
                // Additional STREAM queue configurations
                String initialMembersCountStream =
                        properties.getProperty(REPLY_TO_QUEUE_INITIAL_MEMBER_COUNT);
                String maxAge =
                        properties.getProperty(REPLY_TO_QUEUE_STREAM_MAX_AGE);
                String maxSegmentSize =
                        properties.getProperty(REPLY_TO_QUEUE_STREAM_MAX_SEGMENT_SIZE);
                configureStreamQueue(queueBuilder, initialMembersCountStream, maxAge, maxSegmentSize);
                break;
        }


        // Configure optional queue arguments
        String arguments = properties.getProperty(REPLY_TO_QUEUE_ARGUMENTS);
        configureQueueArguments(queueBuilder, arguments);

        // Configure overflow strategy and dead-letter queue for non-STREAM queues
        if (queueType != Management.QueueType.STREAM) {
            Management.OverflowStrategy overflowStrategy =
                    properties.containsKey(REPLY_TO_QUEUE_OVERFLOW_STRATEGY)
                            ? Management.OverflowStrategy.valueOf(
                            properties.getProperty(REPLY_TO_QUEUE_OVERFLOW_STRATEGY))
                            : DEFAULT_QUEUE_OVERFLOW_STRATEGY;

            // Adjust overflow strategy for QUORUM queues with incompatible dead-letter strategies
            if (queueType == Management.QueueType.QUORUM &&
                    REPLY_TO_QUEUE_QUORUM_DEAD_LETTER_STRATEGY.equalsIgnoreCase(
                            properties.getProperty(REPLY_TO_QUEUE_QUORUM_DEAD_LETTER_STRATEGY)) &&
                    overflowStrategy == Management.OverflowStrategy.DROP_HEAD) {
                overflowStrategy = Management.OverflowStrategy.REJECT_PUBLISH;
                log.warn("DROP_HEAD overflow strategy is not compatible with AT_LEAST_ONCE dead letter " +
                        "strategy for Quorum queues. Changing to REJECT_PUBLISH.");
            }

            queueBuilder.overflowStrategy(overflowStrategy); // Set the overflow strategy
        }

        // Declare the queue
        queueBuilder.declare();
    }

    /**
     * Parses optional arguments from a comma-separated string into a map.
     *
     * @param arguments The comma-separated string of arguments.
     * @return A map of parsed arguments, or null if no valid arguments are found.
     */
    private static Map<String, Object> getOptionalArguments(String arguments) {
        Map<String, Object> optionalArgs = new HashMap<>();
        String[] keyValuePairs = arguments.split(",");

        // Parse each key-value pair and add to the map
        for (String pair : keyValuePairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                optionalArgs.put(keyValue[0].trim(), parseArgumentValue(keyValue[1].trim()));
            }
        }

        return optionalArgs.isEmpty() ? null : optionalArgs;
    }

    /**
     * Parses a string value into its appropriate data type.
     * Supports Boolean, Integer, Double, and defaults to String if parsing fails.
     *
     * @param value The string value to parse.
     * @return The parsed value as an Object.
     */
    private static Object parseArgumentValue(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value); // Parse as Boolean
        }
        try {
            return Integer.parseInt(value); // Try parsing as Integer
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(value); // Try parsing as Double
            } catch (NumberFormatException ex) {
                return value; // Default to String
            }
        }
    }

    /**
     * Populates RabbitMQ properties from the message context.
     * This method retrieves various RabbitMQ configuration parameters from the message context
     * and sets them into the provided `Properties` object.
     *
     * @param messageContext     The message context containing RabbitMQ configuration parameters.
     * @param rabbitmqProperties The `Properties` object to populate with RabbitMQ configuration.
     */
    public static void populateRabbitMQProperties(MessageContext messageContext, Properties rabbitmqProperties) {
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_TYPE,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_TYPE));
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_AUTODECLARE,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_AUTODECLARE));
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_AUTO_DELETE,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_AUTO_DELETE));
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_ARGUMENTS,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_ARGUMENTS));
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_EXCLUSIVE,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_EXCLUSIVE));
        setPropertyIfNotNull(rabbitmqProperties, CLASSIC_MAX_PRIORITY,
                ConnectorUtils.lookupTemplateParamater(messageContext, CLASSIC_MAX_PRIORITY));
        setPropertyIfNotNull(rabbitmqProperties, CLASSIC_VERSION,
                ConnectorUtils.lookupTemplateParamater(messageContext, CLASSIC_VERSION));
        setPropertyIfNotNull(rabbitmqProperties, QUEUE_OVERFLOW_STRATEGY,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUEUE_OVERFLOW_STRATEGY));
        setPropertyIfNotNull(rabbitmqProperties, QUORUM_DEAD_LETTER_STRATEGY,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUORUM_DEAD_LETTER_STRATEGY));
        setPropertyIfNotNull(rabbitmqProperties, QUORUM_DELIVERY_LIMIT,
                ConnectorUtils.lookupTemplateParamater(messageContext, QUORUM_DELIVERY_LIMIT));
        setPropertyIfNotNull(rabbitmqProperties, INITIAL_MEMBER_COUNT,
                ConnectorUtils.lookupTemplateParamater(messageContext, INITIAL_MEMBER_COUNT));
        setPropertyIfNotNull(rabbitmqProperties, STREAM_MAX_AGE,
                ConnectorUtils.lookupTemplateParamater(messageContext, STREAM_MAX_AGE));
        setPropertyIfNotNull(rabbitmqProperties, STREAM_MAX_SEGMENT_SIZE,
                ConnectorUtils.lookupTemplateParamater(messageContext, STREAM_MAX_SEGMENT_SIZE));
        setPropertyIfNotNull(rabbitmqProperties, EXCHANGE_TYPE,
                ConnectorUtils.lookupTemplateParamater(messageContext, EXCHANGE_TYPE));
        setPropertyIfNotNull(rabbitmqProperties, EXCHANGE_ARGUMENTS,
                ConnectorUtils.lookupTemplateParamater(messageContext, EXCHANGE_ARGUMENTS));
        setPropertyIfNotNull(rabbitmqProperties, EXCHANGE_AUTODECLARE,
                ConnectorUtils.lookupTemplateParamater(messageContext, EXCHANGE_AUTODECLARE));
        setPropertyIfNotNull(rabbitmqProperties, EXCHANGE_AUTO_DELETE,
                ConnectorUtils.lookupTemplateParamater(messageContext, EXCHANGE_AUTO_DELETE));
        setPropertyIfNotNull(rabbitmqProperties, HEADER_EXCHANGE_ARGUMENTS,
                ConnectorUtils.lookupTemplateParamater(messageContext, HEADER_EXCHANGE_ARGUMENTS));
    }

    /**
     * Sets a property in the `Properties` object if the value is not null.
     *
     * @param properties The `Properties` object to set the property in.
     * @param key        The key of the property to set.
     * @param value      The value of the property to set. If null, the property is not set.
     */
    private static void setPropertyIfNotNull(Properties properties, String key, Object value) {
        if (value != null) {
            properties.setProperty(key, value.toString());
        }
    }

    /**
     * Sets the error code and error detail to the message context
     *
     * @param messageContext Message Context
     * @param exception      Exception associated
     * @param error          Error to be set
     */
    public static void setErrorPropertiesToMessage(MessageContext messageContext, Exception exception, Error error) {

        messageContext.setProperty(SynapseConstants.ERROR_CODE, error.getErrorCode());
        messageContext.setProperty(SynapseConstants.ERROR_MESSAGE, exception.getMessage());
        messageContext.setProperty(SynapseConstants.ERROR_DETAIL, getStackTrace(exception));
        messageContext.setProperty(SynapseConstants.ERROR_EXCEPTION, exception);
    }


    /**
     * Retrieves the stack trace from a throwable as a string.
     *
     * @param throwable The throwable to extract the stack trace from.
     * @return The stack trace as a string.
     */
    private static String getStackTrace(Throwable throwable) {

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        throwable.printStackTrace(printWriter);
        return result.toString();
    }

    /**
     * Determines the appropriate error code based on the provided exception.
     * <p>
     * This method maps specific exception types to predefined error codes
     * to facilitate error handling and reporting.
     *
     * @param exception The exception to evaluate.
     * @return The corresponding error code as an `Error` enum value.
     */
    public static Error getErrorCode(Exception exception) {

        if (exception instanceof AxisFault) {
            return Error.AXIS_FAULT_ERROR;
        }
        if (exception instanceof ConnectException) {
            return Error.CONNECTION_ERROR;
        }
        if (exception instanceof AmqpException.AmqpConnectionException) {
            return Error.CONNECTION_ERROR;
        }
        if (exception instanceof InvalidConfigurationException) {
            return Error.INVALID_CONFIGURATION;
        }
        if (exception instanceof RabbitMQConnectorException) {
            return Error.OPERATION_ERROR;
        }
        if (exception instanceof InterruptedException) {
            return Error.OPERATION_ERROR;
        }
        if (exception instanceof ExecutionException) {
            return Error.OPERATION_ERROR;
        }
        if (exception instanceof TimeoutException) {
            return Error.PUBLISH_TIMEOUT;
        }
        if (exception instanceof AmqpException) {
            return Error.AMQP_ERROR;
        }
        return Error.RABBITMQ_GENERAL_ERROR;
    }

    /**
     * Gets the request body from input request bodies.
     *
     * @param messageContext the message context
     * @return the request body
     */
    public static String getRequestBody(MessageContext messageContext) {

        String requestBodyJson = (String) messageContext.getProperty(REQUEST_BODY_JSON_IDENTIFIER);
        String requestBodyXml = (String) messageContext.getProperty(REQUEST_BODY_XML_IDENTIFIER);
        String requestBodyText = (String) messageContext.getProperty(REQUEST_BODY_TEXT_IDENTIFIER);

        if (StringUtils.isNotEmpty(requestBodyJson)) {
            return getProcessedJsonRequestBody(requestBodyJson);
        } else if (StringUtils.isNotEmpty(requestBodyXml)) {
            return requestBodyXml;
        } else if (StringUtils.isNotEmpty(requestBodyText)) {
            return requestBodyText;
        } else {
            return StringUtils.EMPTY;
        }
    }

    /**
     * Creates an OMElement with the specified content as its text.
     *
     * @param content the text content to be set in the OMElement. If null, an empty string is used.
     * @return the created OMElement with the specified text content.
     */
    public static OMElement getTextElement(String content) {

        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement textElement = factory.createOMElement(TEXT_ELEMENT);
        if (content == null) {
            content = "";
        }

        textElement.setText(content);
        return textElement;
    }

    /**
     * Gets the transport headers from the message context.
     *
     * @param messageContext the message context
     * @return the transport headers
     */
    public static Object getRequestTransportHeaders(MessageContext messageContext) {

        Axis2MessageContext axis2smc = (Axis2MessageContext) messageContext;
        org.apache.axis2.context.MessageContext axis2MessageContext =
                axis2smc.getAxis2MessageContext();
        return axis2MessageContext.getProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS);
    }

    /**
     * Initializes the transport headers in the message context.
     *
     * @param messageContext the message context
     */
    public static void initRequestTransportHeaders(MessageContext messageContext) {

        Axis2MessageContext axis2smc = (Axis2MessageContext) messageContext;
        org.apache.axis2.context.MessageContext axis2MessageContext =
                axis2smc.getAxis2MessageContext();
        if (axis2MessageContext.getProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS) == null) {
            Map<String, Object> headersMap = new TreeMap(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareToIgnoreCase(o2);
                }
            });
            axis2MessageContext.setProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS, headersMap);
        }
    }

    /**
     * Handles the payload of the message context based on the request body type.
     * It processes the request body and sets the appropriate content type.
     *
     * @param messageContext the message context
     * @param requestBodyType the type of the request body
     * @param requestBody the request body
     */
    public static void handleRequestPayload(MessageContext messageContext, String requestBodyType, String requestBody)
            throws AxisFault, RabbitMQConnectorException {

        // If the connector inputs are not empty, process them to build the request payload.
        // If the connector inputs are empty, the request payload and content type
        // will be directly retrieved from the message context.
        if (StringUtils.isNotEmpty(requestBodyType)
                && StringUtils.isNotEmpty(requestBody)) {
            org.apache.axis2.context.MessageContext axis2MessageContext =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            try {
                requestBody = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, requestBody);
            } catch (JaxenException e) {
                throw new RabbitMQConnectorException("Error while processing request body");
            }
            if (requestBodyType.equals(XML_TYPE)) {
                try {
                    requestBody = "<pfPadding>" + requestBody + "</pfPadding>";
                    JsonUtil.removeJsonPayload(axis2MessageContext);
                    XMLStreamReader xmlReader = RabbitMQXMLInputFactory
                            .getXMLInputFactory().createXMLStreamReader(new StringReader(requestBody));
                    StAXBuilder builder = new StAXOMBuilder(xmlReader);
                    OMElement omXML = builder.getDocumentElement();
                    if (!checkAndReplaceEnvelope(omXML, messageContext)) {
                        axis2MessageContext.getEnvelope().getBody().addChild(omXML.getFirstElement());
                    }
                } catch (XMLStreamException e) {
                    throw new RabbitMQConnectorException("Error creating SOAP Envelope from source " + requestBody, e);
                }
            } else if (requestBodyType.equals(JSON_TYPE)) {
                try {
                    JsonUtil.getNewJsonPayload(axis2MessageContext, requestBody, true, true);
                } catch (AxisFault axisFault) {
                    throw new AxisFault("Error creating JSON Payload from source " + requestBody , axisFault);
                }
            } else if (requestBodyType.equals(TEXT_TYPE)) {
                JsonUtil.removeJsonPayload(axis2MessageContext);
                axis2MessageContext.getEnvelope().getBody().addChild(getTextElement(requestBody));
            }
            setRequestContentType(messageContext, requestBodyType);
        }
    }

    /**
     * Processes the JSON request body by removing surrounding single quotes, if present.
     * <p>
     * This is typically used when the JSON payload is passed as a string parameter
     * that has been wrapped in single quotes by the preceding message flow or client.
     * </p>
     *
     * @param requestBody The raw request body string, which may be wrapped in single quotes.
     * @return The processed request body string with surrounding single quotes removed,
     * or the original string if no single quotes were found at the start and end.
     */
    public static String getProcessedJsonRequestBody(String requestBody) {

        // Check if the request body starts and ends with the defined single quote character.
        if (requestBody.startsWith(SINGLE_QUOTE)
                && requestBody.endsWith(SINGLE_QUOTE)) {
            // If quotes are present, remove the first character (start quote) and the last character (end quote).
            return requestBody.substring(1, requestBody.length() - 1);
        }
        return requestBody;
    }

    /**
     * Sets the content type of the message context based on the request body type.
     *
     * @param synCtx the message context
     * @param requestBodyType the type of the request body
     */
    private static void setRequestContentType(MessageContext synCtx, String requestBodyType) {

        org.apache.axis2.context.MessageContext a2mc = ((Axis2MessageContext) synCtx).getAxis2MessageContext();
        switch (requestBodyType) {
            case XML_TYPE:
                if (!XML_CONTENT_TYPE.equals(a2mc.getProperty(Constants.Configuration.MESSAGE_TYPE)) &&
                        !SOAP11_CONTENT_TYPE.equals(a2mc.getProperty(Constants.Configuration.MESSAGE_TYPE)) &&
                        !SOAP12_CONTENT_TYPE.equals(a2mc.getProperty(Constants.Configuration.MESSAGE_TYPE))) {
                    a2mc.setProperty(Constants.Configuration.MESSAGE_TYPE, XML_CONTENT_TYPE);
                    a2mc.setProperty(Constants.Configuration.CONTENT_TYPE, XML_CONTENT_TYPE);
                    handleSpecialProperties(XML_CONTENT_TYPE, a2mc);
                }
                break;
            case JSON_TYPE:
                a2mc.setProperty(Constants.Configuration.MESSAGE_TYPE, JSON_CONTENT_TYPE);
                a2mc.setProperty(Constants.Configuration.CONTENT_TYPE, JSON_CONTENT_TYPE);
                handleSpecialProperties(JSON_CONTENT_TYPE, a2mc);
                break;
            case TEXT_TYPE:
                a2mc.setProperty(Constants.Configuration.MESSAGE_TYPE, TEXT_CONTENT_TYPE);
                a2mc.setProperty(Constants.Configuration.CONTENT_TYPE, TEXT_CONTENT_TYPE);
                handleSpecialProperties(TEXT_CONTENT_TYPE, a2mc);
                break;
        }
        a2mc.removeProperty("NO_ENTITY_BODY");
    }

    /**
     * Checks and replaces the SOAP envelope if the result element is a valid SOAP envelope.
     *
     * @param resultElement the result element
     * @param synCtx the message context
     * @return true if the envelope was replaced, false otherwise
     */
    private static boolean checkAndReplaceEnvelope(OMElement resultElement, MessageContext synCtx)
            throws AxisFault, RabbitMQConnectorException {
        OMElement firstChild = resultElement.getFirstElement();

        if (firstChild == null) {
            throw new RabbitMQConnectorException("Generated content is not a valid XML payload");
        }

        QName resultQName = firstChild.getQName();
        if (resultQName.getLocalPart().equals("Envelope") && (
                resultQName.getNamespaceURI().equals(SOAP11Constants.SOAP_ENVELOPE_NAMESPACE_URI) ||
                        resultQName.getNamespaceURI().
                                equals(SOAP12Constants.SOAP_ENVELOPE_NAMESPACE_URI))) {
            SOAPEnvelope soapEnvelope = AXIOMUtils.getSOAPEnvFromOM(resultElement.getFirstElement());
            if (soapEnvelope != null) {
                try {
                    soapEnvelope.buildWithAttachments();
                    synCtx.setEnvelope(soapEnvelope);
                } catch (AxisFault axisFault) {
                    throw new AxisFault("Unable to attach SOAPEnvelope", axisFault);
                }
            }
        } else {
            return false;
        }
        return true;
    }

    /**
     * Handles input headers by processing inline expression templates and setting transport headers.
     * This method processes the headers provided as a string in the JSON array format,
     * evaluates any inline expressions, and sets the transport headers in the message context.
     *
     * @param messageContext the message context
     * @param headers the headers in JSON array format
     */
    public static void handleInputHeaders(MessageContext messageContext, String headers)
            throws RabbitMQConnectorException {

        try {
            if (StringUtils.isNotEmpty(headers)) {
                headers = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, headers);
                JSONArray headersArray = new JSONArray(headers);
                if (headersArray.length() > 0) {
                    initRequestTransportHeaders(messageContext);
                    Object transportHeaders = getRequestTransportHeaders(messageContext);
                    if (transportHeaders instanceof Map) {
                        Map transportHeadersMap = (Map) transportHeaders;
                        for (int i = 0; i < headersArray.length(); i++) {
                            Object headersItem = headersArray.get(i);
                            if (headersItem instanceof JSONArray) {
                                JSONArray headersArrayItem = (JSONArray) headersItem;
                                if (headersArrayItem.length() == 2) {
                                    String headerName = headersArrayItem.getString(0).trim();
                                    String headerValue = headersArrayItem.getString(1).trim();
                                    if (StringUtils.isNotEmpty(headerName)) {
                                        transportHeadersMap.put(headerName, headerValue);
                                    }
                                }
                            } else if (headersItem instanceof JSONObject) {
                                JSONObject headersObjectItem = (JSONObject) headersItem;
                                if (headersObjectItem.keys().hasNext()) {
                                    String headerName = String.valueOf(headersObjectItem.keys().next());
                                    String headerValue = headersObjectItem.getString(headerName);
                                    String trimmedHeaderName = headerName.trim();
                                    String trimmedHeaderValue = headerValue.trim();
                                    if (StringUtils.isNotEmpty(trimmedHeaderName)) {
                                        transportHeadersMap.put(trimmedHeaderName, trimmedHeaderValue);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (JaxenException e) {
            throw new RabbitMQConnectorException("Error while processing headers", e);
        }
    }

    /**
     * Retrieves the message body from the given {@code MessageContext} as a byte array.
     * This method uses the Axis2 message context and a configured message formatter to
     * extract the message payload from the Synapse message flow.
     *
     * @param msgContext The Synapse {@code MessageContext} containing the message.
     * @return The message body as a byte array.
     * @throws IOException If an I/O error occurs while writing the message to the output stream.
     * @throws RabbitMQConnectorException If an error occurs during payload handling or message body retrieval.
     */
    public static byte[] getRequestMessageBody(MessageContext msgContext)
            throws IOException, RabbitMQConnectorException {

        // Retrieve the request body type and the body content from the connector input
        String requestBodyType = msgContext.getProperty(REQUEST_BODY_TYPE_IDENTIFIER) != null ?
                (String) msgContext.getProperty(REQUEST_BODY_TYPE_IDENTIFIER) : StringUtils.EMPTY;
        String requestBody = getRequestBody(msgContext);

        // Process the message context to set the payload based connector input.
        // If the connector inputs are empty, the request payload and content type
        // will be directly retrieved from the message context.
        handleRequestPayload(msgContext, requestBodyType, requestBody);

        // Retrieve the request payload from the message context
        // as a byte stream using the content-type-specific formatter
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((Axis2MessageContext) msgContext).getAxis2MessageContext();

        OMOutputFormat format = BaseUtils.getOMOutputFormat(axis2MsgCtx);
        byte[] messageBody;

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Select the appropriate MessageFormatter based on the message content type.
            MessageFormatter messageFormatter = MessageProcessorSelector.getMessageFormatter(axis2MsgCtx);
            messageFormatter.writeTo(axis2MsgCtx, format, out, false);
            // Convert the content written to the stream into a byte array.
            messageBody = out.toByteArray();
        } catch (AxisFault axisFault) {
            // Wrap the AxisFault into a more specific exception if the formatter cannot be found or used.
            throw new AxisFault("Unable to get the message formatter to use", axisFault);
        }
        return messageBody;
    }

    /**
     * Populates the message properties from the given `MessageContext` into the provided `Message` object.
     * This method extracts headers, message ID, correlation ID, content type, content encoding, and other
     * relevant properties from the Axis2 message context and sets them into the RabbitMQ `Message`.
     *
     * @param msgContext The Synapse `MessageContext` containing the message details.
     * @param message    The RabbitMQ `Message` object to populate with properties.
     */
    public static void populateRequestMessageProperties(MessageContext msgContext, Message message)
            throws RabbitMQConnectorException {

        String inputHeaders = msgContext.getProperty(HEADERS_IDENTIFIER) != null ? (String) msgContext.getProperty(
                HEADERS_IDENTIFIER) : EMPTY_LIST;

        handleInputHeaders(msgContext, inputHeaders);

        // Retrieve the Axis2 message context from the Synapse message context
        org.apache.axis2.context.MessageContext axis2MsgCtx =
                ((Axis2MessageContext) msgContext).getAxis2MessageContext();

        // Retrieve transport headers from the Axis2 message context
        Map<String, Object> headers =
                (Map<String, Object>) axis2MsgCtx.getProperty(
                        org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS
                );
        if (headers == null) {
            headers = new HashMap<>();
        }

        // Set the message ID if available
        String messageId = axis2MsgCtx.getMessageID();
        if (messageId != null) {
            message.messageId(messageId);
        }

        // Set the correlation ID; default to message ID if correlation ID is not provided
        String correlationId = (String) axis2MsgCtx.getProperty(CORRELATION_ID);
        if ((correlationId == null) || (correlationId.isEmpty())) {
            correlationId = messageId;
        }
        message.correlationId(correlationId);

        // Retrieve the content type previously set during payload handling (handlePayload).
        String contentType = (String) axis2MsgCtx.getProperty(Constants.Configuration.MESSAGE_TYPE);
        // Check if the content type was successfully retrieved from the Axis2 Message Context.
        if (contentType == null) {
            // If null, retrieve the content type from the connector input
            String requestBodyType = msgContext.getProperty(REQUEST_BODY_TYPE_IDENTIFIER) != null ?
                    (String) msgContext.getProperty(REQUEST_BODY_TYPE_IDENTIFIER) : StringUtils.EMPTY;
            setRequestContentType(msgContext, requestBodyType);

            // Re-attempt to retrieve the updated content type from the Axis2 Message Context.
            contentType = (String) axis2MsgCtx.getProperty(Constants.Configuration.MESSAGE_TYPE);

            // If content type is now available, set it on the message object
            // If content type is still unavailable after processing, set a default content type (e.g., text/plain).
            message.contentType(Objects.requireNonNullElse(contentType, TEXT_CONTENT_TYPE));
        } else {
            message.contentType(contentType);
        }

        // Set the content encoding if available
        String contentEncoding = StringUtils.defaultIfEmpty(
                (String) axis2MsgCtx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING),
                (String) msgContext.getProperty(REQUEST_CHARSET_IDENTIFIER)
        );
        message.contentEncoding(
                StringUtils.defaultIfEmpty(contentEncoding, DEFAULT_CHARSET));

        // Add the SOAP action to headers if available
        String soapAction = axis2MsgCtx.getSoapAction();
        if (soapAction != null) {
            headers.put(SOAP_ACTION, soapAction);
        }

        // Add the internal transaction count to headers if it exists in the Axis2 message context
        if (axis2MsgCtx.getProperties().containsKey(BaseConstants.INTERNAL_TRANSACTION_COUNTED)) {
            headers.put(BaseConstants.INTERNAL_TRANSACTION_COUNTED,
                    axis2MsgCtx.getProperty(BaseConstants.INTERNAL_TRANSACTION_COUNTED));
        }

        // Add all headers as properties to the RabbitMQ message
        for (String key : headers.keySet()) {
            Object value = headers.get(key);
            // Set the property in the new message based on its type
            if (value instanceof Boolean) {
                message.property(key, (Boolean) value);
            } else if (value instanceof Integer) {
                message.property(key, (Integer) value);
            } else if (value instanceof Long) {
                message.property(key, (Long) value);
            } else if (value instanceof Double) {
                message.property(key, (Double) value);
            } else if (value instanceof String) {
                message.property(key, (String) value);
            } else if (value instanceof Byte) {
                message.property(key, (Byte) value);
            } else if (value instanceof Float) {
                message.property(key, (Float) value);
            } else if (value instanceof UUID) {
                message.property(key, (UUID) value);
            } else if (value instanceof Short) {
                message.property(key, (Short) value);
            }

        }
    }

    /**
     * Used to change the content type.
     *
     * @param resultValue the content type
     * @param axis2MessageCtx the Axis2 message context
     */
    private static void handleSpecialProperties(Object resultValue,
                                                org.apache.axis2.context.MessageContext axis2MessageCtx) {

        axis2MessageCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, resultValue);
        Object o = axis2MessageCtx.getProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS);
        Map headers = (Map) o;
        if (headers != null) {
            headers.remove(HTTP.CONTENT_TYPE);
            headers.put(HTTP.CONTENT_TYPE, resultValue);
        }
    }

    /**
     * Builds the message context by setting the correlation ID, content type, content encoding,
     * transaction properties, and SOAP envelope based on the RabbitMQ message context.
     *
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     * @param msgContext             The Axis2 message context to be populated.
     * @throws AxisFault If an error occurs while setting the SOAP envelope.
     */
    public static void buildResponseMessage(RabbitMQMessageContext rabbitMQMessageContext,
                                            org.apache.axis2.context.MessageContext msgContext) throws AxisFault {
        setResponseCorrelationId(msgContext, rabbitMQMessageContext);
        String contentType = setResponseContentType(msgContext, rabbitMQMessageContext);
        setResponseContentEncoding(msgContext, rabbitMQMessageContext);
        setTransactionCountedProperty(msgContext, rabbitMQMessageContext);
        setSoapEnvelop(msgContext, rabbitMQMessageContext.getBody(), contentType);
    }

    /**
     * Sets the correlation ID in the message context. The correlation ID is determined
     * based on the RabbitMQ message context or the Axis2 message context.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     */
    private static void setResponseCorrelationId(org.apache.axis2.context.MessageContext msgContext,
                                                 RabbitMQMessageContext rabbitMQMessageContext) {
        String correlationID;
        if (org.apache.commons.lang.StringUtils.isNotEmpty(rabbitMQMessageContext.getCorrelationId())) {
            correlationID = rabbitMQMessageContext.getCorrelationId();
        } else if (org.apache.commons.lang.StringUtils.isNotEmpty(rabbitMQMessageContext.getMessageID())) {
            correlationID = rabbitMQMessageContext.getMessageID();
        } else {
            // Fallback to the Axis2 message ID.
            correlationID = msgContext.getMessageID();
        }

        msgContext.setProperty(CORRELATION_ID, correlationID);
    }

    /**
     * Determines and sets the content type in the message context. The content type is
     * derived from the RabbitMQ message context or connector input.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     * @return The determined content type.
     */

    private static String setResponseContentType(org.apache.axis2.context.MessageContext msgContext,
                                                 RabbitMQMessageContext rabbitMQMessageContext) {
        String contentTypeFromConnectorInput = (String) msgContext.getProperty(RESPONSE_BODY_TYPE_IDENTIFIER);
        String contentTypeFromMessage = rabbitMQMessageContext.getContentType();
        String contentType;

        if (StringUtils.isNotEmpty(contentTypeFromMessage)) {
            contentType = contentTypeFromMessage;
        } else if (StringUtils.isNotEmpty(contentTypeFromConnectorInput)) {
            contentType = contentTypeFromConnectorInput;
        } else {
            log.warn("Unable to determine content type for message " + msgContext.getMessageID()
                    + ". Setting to text/plain.");
            contentType = TEXT_CONTENT_TYPE;
        }
        msgContext.setProperty(RABBITMQ_CONTENT_TYPE , contentType);
        msgContext.setProperty(AXIS2_CONTENT_TYPE , contentType);
        handleSpecialProperties(contentType, msgContext);
        return contentType;
    }

    /**
     * Sets the content encoding in the message context. The content encoding is
     * derived from the RabbitMQ message context or connector input.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     */
    private static void setResponseContentEncoding(org.apache.axis2.context.MessageContext msgContext,
                                                   RabbitMQMessageContext rabbitMQMessageContext) {
        String contentEncoding = StringUtils.defaultIfEmpty(
                rabbitMQMessageContext.getContentEncoding(),
                (String) msgContext.getProperty(RESPONSE_CHARSET_IDENTIFIER)
        );
        msgContext.setProperty(CONTENT_ENCODING,
                StringUtils.defaultIfEmpty(contentEncoding, DEFAULT_CHARSET));
        msgContext.setProperty(CHARACTER_SET_ENCODING,
                StringUtils.defaultIfEmpty(contentEncoding, DEFAULT_CHARSET));
    }

    /**
     * Sets the transaction counted property in the message context.
     * This property indicates whether the transaction has been counted
     * and is retrieved from the RabbitMQ message context's application properties.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing application properties.
     */
    private static void setTransactionCountedProperty(org.apache.axis2.context.MessageContext msgContext,
                                                      RabbitMQMessageContext rabbitMQMessageContext) {
        if (rabbitMQMessageContext.hasProperties()) {
            Object transactionCounted = rabbitMQMessageContext.getApplicationProperties()
                    .get(INTERNAL_TRANSACTION_COUNTED);
            if (transactionCounted != null) {
                msgContext.setProperty(INTERNAL_TRANSACTION_COUNTED, transactionCounted);
            }
        }
    }

    /**
     * Sets the SOAP envelope in the message context based on the provided message body
     * and content type.
     *
     * @param msgContext  The Axis2 message context to be updated.
     * @param body        The message body as a byte array.
     * @param contentType The content type of the message.
     * @throws AxisFault If an error occurs while processing the SOAP envelope.
     */
    private static void setSoapEnvelop(org.apache.axis2.context.MessageContext msgContext, byte[] body,
                                       String contentType) throws AxisFault {
        Builder builder = getBuilder(msgContext, contentType);
        OMElement documentElement = builder.processDocument(new ByteArrayInputStream(body),
                contentType, msgContext);
        msgContext.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
    }

    /**
     * Retrieves the appropriate message builder for the given content type.
     * If no builder is found, defaults to the SOAP builder.
     *
     * @param msgContext     The Axis2 message context.
     * @param rawContentType The raw content type of the message.
     * @return The message builder for the content type.
     * @throws AxisFault If an error occurs while parsing the content type.
     */
    private static Builder getBuilder(org.apache.axis2.context.MessageContext msgContext,
                                      String rawContentType) throws AxisFault {
        try {
            ContentType contentType = new ContentType(rawContentType);
            String charset = contentType.getParameter(CHARSET);
            msgContext.setProperty(CHARACTER_SET_ENCODING, charset);

            Builder builder = BuilderUtil.getBuilderFromSelector(contentType.getBaseType(), msgContext);
            if (builder == null) {
                if (log.isDebugEnabled()) {
                    log.debug("No message builder found for type '"
                            + contentType.getBaseType() + "'. Falling back to SOAP.");
                }
                builder = new SOAPBuilder(); // Default to the SOAP builder.
            }

            return builder;
        } catch (ParseException e) {
            throw new AxisFault("Error parsing content type: " + rawContentType, e);
        }
    }

    /**
     * Extracts transport headers from the RabbitMQ message context and Axis2 message context.
     * The method combines headers from both contexts, prioritizing RabbitMQ headers.
     *
     * @param rabbitMQMessageContext The RabbitMQ message context containing message-specific headers.
     * @param msgContext             The Axis2 message context containing additional headers.
     * @return A map of transport headers.
     */
    public static Map<String, String> getResponseTransportHeaders(RabbitMQMessageContext rabbitMQMessageContext,
                                                                  org.apache.axis2.context.MessageContext msgContext) {
        Map<String, String> map = new HashMap<>();

        // Add the correlation ID from the RabbitMQ message context if available.
        if (rabbitMQMessageContext.getCorrelationId() != null) {
            map.put(CORRELATION_ID, rabbitMQMessageContext.getCorrelationId());
        }
        // Add the message ID, prioritizing the RabbitMQ message context over the Axis2 message context.
        String messageId = rabbitMQMessageContext.getMessageID();
        if (messageId != null) {
            map.put(MESSAGE_ID, messageId);
        } else if (msgContext.getMessageID() != null) {
            map.put(MESSAGE_ID, msgContext.getMessageID());
        }
        // Add application properties from the RabbitMQ message context, excluding internal transaction properties.
        if (rabbitMQMessageContext.hasProperties()) {
            for (Map.Entry<String, Object> headerEntry : rabbitMQMessageContext
                    .getApplicationProperties().entrySet()) {
                String key = headerEntry.getKey();
                Object value = headerEntry.getValue();
                if (key != null && value != null && !INTERNAL_TRANSACTION_COUNTED.equals(key)) {
                    map.put(key, value.toString());
                }
            }
        }
        return map;
    }

}
