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

import com.rabbitmq.client.amqp.Management;
import javax.xml.namespace.QName;

/**
 * This class contains constants used for RabbitMQ integration.
 * It defines configuration keys, default values, and other
 * constants required for managing RabbitMQ connections, exchanges,
 * queues, and message handling.
 */
public class RabbitMQConstants {

    // --- Connector Information ---
    public static final String CONNECTOR_NAME = "rabbitmq";
    public static final String CONNECTION_NAME = "name";

    // --- Server Configuration ---
    public static final String SERVER_URLS = "serverUrls";
    public static final String SERVER_USERNAME = "username";
    public static final String SERVER_PASSWORD = "password";

    // --- Authentication ---
    public static final String SASL_MECHANISM = "saslMechanism";
    public static final String OAUTH2_ENABLED = "oAuth2Enabled";
    public static final String TOKEN_ENDPOINT = "tokenEndpoint";
    public static final String GRANT_TYPE = "grantType";
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String OAUTH2_USERNAME = "oAuth2UserName";
    public static final String OAUTH2_PASSWORD = "oAuth2Password";
    public static final String DEFAULT_SASL_MECHANISM = "PLAIN";
    public static final String OAUTH2_PASSWORD_GRANT_TYPE = "password";
    public static final String DEFAULT_OAUTH2_GRANT_TYPE = "client_credentials";

    // --- Connection Configuration ---
    public static final String VIRTUAL_HOST = "virtualHost";
    public static final String CONNECTION_IDLE_TIMEOUT = "connectionIdleTimeout";
    public static final String CONNECTION_ESTABLISH_RETRY_COUNT = "connectionEstablishRetryCount";
    public static final String CONNECTION_ESTABLISH_RETRY_INTERVAL = "connectionEstablishRetryInterval";
    public static final String CONNECTION_RECOVERY_POLICY_TYPE = "connectionRecoveryPolicyType";
    public static final String CONNECTION_RECOVERY_INITIAL_DELAY = "connectionRecoveryInitialDelay";
    public static final String CONNECTION_RECOVERY_INTERVAL = "connectionRecoveryInterval";
    public static final String CONNECTION_RECOVERY_TIMEOUT = "connectionRecoveryTimeout";
    public static final long DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY = 10000;
    public static final long DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL = 10000;
    public static final long DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT = 60000;

    // --- SSL Configuration ---
    public static final String SSL_ENABLED = "sslEnabled";
    public static final String TRUST_EVERYTHING = "trustEverything";
    public static final String HOSTNAME_VERIFICATION_ENABLED = "hostnameVerificationEnabled";
    public static final String KEYSTORE_LOCATION = "keyStoreLocation";
    public static final String KEYSTORE_TYPE = "keyStoreType";
    public static final String KEYSTORE_PASSWORD = "keyStorePassword";
    public static final String TRUSTSTORE_LOCATION = "trustStoreLocation";
    public static final String TRUSTSTORE_TYPE = "trustStoreType";
    public static final String TRUSTSTORE_PASSWORD = "trustStorePassword";
    public static final String SSL_VERSION = "sslVersion";
    public static final String SERVER_TRUSTSTORE = "javax.net.ssl.trustStore";
    public static final String SERVER_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
    public static final String SERVER_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
    public static final String SERVER_TLS = "TLS";
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS";
    public static final String DEFAULT_SSL_VERSION = "TLS";

    // --- Default Values ---
    public static final String DEFAULT_VIRTUAL_HOST = "/";
    public static final int DEFAULT_IDLE_TIMEOUT = 60000;
    public static final long DEFAULT_RETRY_INTERVAL = 30000;
    public static final int DEFAULT_RETRY_COUNT = 3;

    // --- Acknowledgement Decisions ---
    public static final String PUBLISHER_CONFIRMS = "publisherConfirms";
    public static final String ACKNOWLEDGEMENT_DECISION = "ACK_DECISION";
    public static final String ACKNOWLEDGE = "ACKNOWLEDGE";
    public static final String SET_ROLLBACK_ONLY = "SET_ROLLBACK_ONLY";
    public static final String SET_REQUEUE_ON_ROLLBACK = "SET_REQUEUE_ON_ROLLBACK";

    // --- Exchange Properties ---
    public static final String EXCHANGE_NAME = "exchange";
    public static final String EXCHANGE_TYPE = "exchangeType";
    public static final String DEFAULT_EXCHANGE_TYPE = "DIRECT"; // Using String value of Management.ExchangeType.DIRECT

    public static final String DIRECT_EXCHANGE_TYPE = "DIRECT";
    public static final String TOPIC_EXCHANGE_TYPE = "TOPIC";
    public static final String EXCHANGE_ARGUMENTS = "exchangeArguments";
    public static final String EXCHANGE_AUTODECLARE = "exchangeAutoDeclare";
    public static final String EXCHANGE_AUTO_DELETE = "exchangeAutoDelete";
    public static final String HEADER_EXCHANGE_ARGUMENTS = "headerExchangeArguments";
    public static final String ROUTING_KEY = "routingKey";

    // --- Queue Properties ---
    public static final String QUEUE_NAME = "queue";
    public static final String QUEUE_TYPE = "queueType";
    public static final String DEFAULT_QUEUE_TYPE = "CLASSIC";
    public static final String QUEUE_ARGUMENTS = "queueArguments";
    public static final String QUEUE_AUTODECLARE = "queueAutoDeclare";
    public static final String QUEUE_AUTO_DELETE = "queueAutoDelete";
    public static final String QUEUE_EXCLUSIVE = "queueExclusive";

    // --- Reply To Queue Properties ---
    public static final String REPLY_TO_QUEUE_NAME = "replyToQueue";
    public static final String REPLY_TO_QUEUE_AUTODECLARE = "replyToQueueAutoDeclare";
    public static final String REPLY_TO_QUEUE_TYPE = "replyToQueueType";
    public static final String REPLY_TO_QUEUE_ARGUMENTS = "replyToQueueArguments";
    public static final String REPLY_TO_QUEUE_AUTO_DELETE = "replyToQueueAutoDelete";
    public static final String REPLY_TO_QUEUE_EXCLUSIVE = "replyToQueueExclusive";

    // --- Classic Queue Specific Properties ---
    public static final String CLASSIC_MAX_PRIORITY = "maxPriority";
    public static final String CLASSIC_VERSION = "classicVersion";
    public static final String REPLY_TO_QUEUE_CLASSIC_MAX_PRIORITY = "replyToQueueMaxPriority";
    public static final String REPLY_TO_QUEUE_CLASSIC_VERSION = "replyToQueueClassicVersion";

    // --- Quorum Queue Specific Properties ---
    public static final String INITIAL_MEMBER_COUNT = "initialMemberCount";
    public static final String QUORUM_DEAD_LETTER_STRATEGY = "deadLetterStrategy";
    public static final String QUORUM_DELIVERY_LIMIT = "deliveryLimit";

    public static final String REPLY_TO_QUEUE_INITIAL_MEMBER_COUNT = "replyToQueueInitialMemberCount";
    public static final String REPLY_TO_QUEUE_QUORUM_DEAD_LETTER_STRATEGY = "replyToQueueDeadLetterStrategy";
    public static final String REPLY_TO_QUEUE_QUORUM_DELIVERY_LIMIT = "replyToQueueDeliveryLimit";

    // --- Stream Filtering ---
    public static final String STREAM_MAX_AGE = "maxAge";
    public static final String STREAM_MAX_SEGMENT_SIZE = "maxSegmentSize";

    public static final String REPLY_TO_QUEUE_STREAM_MAX_AGE = "replyToQueueMaxAge";
    public static final String REPLY_TO_QUEUE_STREAM_MAX_SEGMENT_SIZE = "replyToQueueMaxSegmentSize";

    // --- Queue Overflow Strategy ---
    public static final String QUEUE_OVERFLOW_STRATEGY = "queueOverflowStrategy";
    public static final String REPLY_TO_QUEUE_OVERFLOW_STRATEGY = "replyToQueueOverflowStrategy";
    public static final Management.OverflowStrategy DEFAULT_QUEUE_OVERFLOW_STRATEGY
            = Management.OverflowStrategy.DROP_HEAD;

    // --- Timeouts ---
    public static final String PUBLISH_TIMEOUT = "publishTimeout";
    public static final String REQUEST_TIMEOUT = "requestTimeout";
    public static final long DEFAULT_PUBLISH_TIMEOUT = 60000;
    public static final long DEFAULT_REQUEST_TIMEOUT = 60000;

    // --- Message Properties and headers ---
    public static final String CORRELATION_ID = "rabbitmq.message.correlation.id";
    public static final String SOAP_ACTION = "SOAP_ACTION";
    public static final String MESSAGE_ID = "rabbitmq.message.id";
    public static final String INTERNAL_TRANSACTION_COUNTED = "INTERNAL_TRANSACTION_COUNTED";
    public static final String RABBITMQ_REPLY_TO = "RABBITMQ_REPLY_TO";

    // --- Payload Properties ---
    public static final QName TEXT_ELEMENT = new QName("http://ws.apache.org/commons/ns/payload", "text");
    public static final String JSON_TYPE = "JSON";
    public static final String XML_TYPE = "XML";
    public static final String TEXT_TYPE = "TEXT";
    public static final String SINGLE_QUOTE = "\'";
    public static final String XML_CONTENT_TYPE = "application/xml";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String TEXT_CONTENT_TYPE = "text/plain";
    public static final String SOAP11_CONTENT_TYPE = "text/xml";
    public static final String SOAP12_CONTENT_TYPE = "application/soap+xml";
    public static final String HEADERS_IDENTIFIER = "RABBITMQ_CONN_HEADERS";
    public static final String REQUEST_BODY_TYPE_IDENTIFIER = "RABBITMQ_CONN_REQUEST_BODY_TYPE";
    public static final String REQUEST_CHARSET_IDENTIFIER = "RABBITMQ_CONN_REQUEST_CHARSET";
    public static final String REQUEST_BODY_JSON_IDENTIFIER = "RABBITMQ_CONN_REQUEST_BODY_JSON";
    public static final String REQUEST_BODY_XML_IDENTIFIER = "RABBITMQ_CONN_REQUEST_BODY_XML";
    public static final String REQUEST_BODY_TEXT_IDENTIFIER = "RABBITMQ_CONN_REQUEST_BODY_TEXT";
    public static final String RESPONSE_BODY_TYPE_IDENTIFIER = "RABBITMQ_CONN_RESPONSE_BODY_TYPE";
    public static final String RESPONSE_CHARSET_IDENTIFIER = "RABBITMQ_CONN_RESPONSE_CHARSET";
    public static final String RABBITMQ_CONTENT_TYPE = "rabbitmq.message.content.type";
    public static final String AXIS2_CONTENT_TYPE = "ContentType";
    public static final String CONTENT_ENCODING = "rabbitmq.message.content.encoding";
    public static final String EMPTY_LIST = "[]";
    public static final String XML_INPUT_FACTORY_PROPERTIES = "XMLInputFactory.properties";
    /**
     * Property key for the character set encoding.
     */
    public static final String CHARACTER_SET_ENCODING = "CHARACTER_SET_ENCODING";
    public static final String CHARSET = "charset";
    public static final String DEFAULT_CHARSET = "UTF-8";

    /**
     * Enum representing the different connection recovery policies for RabbitMQ.
     * - FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT: Recovery with a fixed delay, initial delay, and timeout.
     * - FIXED_WITH_INITIAL_DELAY: Recovery with a fixed delay and initial delay.
     * - FIXED: Recovery with a fixed delay only.
     */
    public enum ConnectionRecoveryPolicy {
        FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT,
        FIXED_WITH_INITIAL_DELAY,
        FIXED;
    }
}
