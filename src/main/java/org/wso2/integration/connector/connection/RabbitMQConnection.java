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

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.RpcClient;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionConfig;
import org.wso2.integration.connector.exception.InvalidConfigurationException;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.pojo.ConnectionConfiguration;
import org.wso2.integration.connector.utils.ClientKey;
import org.wso2.integration.connector.utils.RabbitMQRoundRobinAddressSelector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages the RabbitMQ connection and provides utilities for interacting with RabbitMQ.
 * It handles the creation and caching of publishers and RPC clients, ensuring efficient communication
 * with the RabbitMQ broker. The class also manages the round-robin address selection for RabbitMQ nodes.
 * <p>
 * Additionally, it provides methods to close and clean up resources, preventing memory leaks and ensuring
 * proper resource management. This class is designed to integrate with the WSO2 framework.
 */
public class RabbitMQConnection implements org.wso2.integration.connector.core.connection.Connection {

    protected Log log = LogFactory.getLog(this.getClass());
    private final Connection clientConnection;
    private final RabbitMQRoundRobinAddressSelector addressSelector;

    private final Map<ClientKey, Publisher> publisherCache = new ConcurrentHashMap<>();
    private final Map<ClientKey, RpcClient> rpcClientCache = new ConcurrentHashMap<>();

    /**
     * Constructs a `RabbitMQConnection` instance using the provided configuration.
     * Initializes the RabbitMQ connection, address selector, and environment builder.
     *
     * @param configuration The RabbitMQ connection configuration.
     * @throws RabbitMQConnectorException            If there is an error in the RabbitMQ connector.
     * @throws AmqpException.AmqpConnectionException If there is an error establishing the AMQP connection.
     * @throws InvalidConfigurationException         If the provided configuration is invalid.
     */
    public RabbitMQConnection(ConnectionConfiguration configuration)
            throws RabbitMQConnectorException, AmqpException.AmqpConnectionException, InvalidConfigurationException {
        AmqpEnvironmentBuilder environmentBuilder = new AmqpEnvironmentBuilder();
        RabbitMQConnectionSetup connectionSetup = new RabbitMQConnectionSetup();
        ConnectionBuilder connectionBuilder =
                connectionSetup.constructConnectionBuilder(configuration, environmentBuilder);
        addressSelector = new RabbitMQRoundRobinAddressSelector();
        connectionBuilder.addressSelector(addressSelector);
        this.clientConnection = connectionBuilder.build();
    }

    /**
     * Retrieves the RabbitMQ client connection.
     *
     * @return The RabbitMQ client connection.
     */
    public Connection getClientConnection() {

        return clientConnection;
    }

    /**
     * Retrieves the cache of publishers.
     *
     * @return A map containing cached publishers.
     */
    public Map<ClientKey, Publisher> getPublisherCache() {

        return publisherCache;
    }

    /**
     * Retrieves the cache of RPC clients.
     *
     * @return A map containing cached RPC clients.
     */
    public Map<ClientKey, RpcClient> getRpcClientCache() {

        return rpcClientCache;
    }

    /**
     * Retrieves the round-robin address selector used for RabbitMQ connections.
     *
     * @return The `RabbitMQRoundRobinAddressSelector` instance.
     */
    public RabbitMQRoundRobinAddressSelector getAddressSelector() {
        return addressSelector;
    }

    /**
     * Adds a publisher to the cache with the specified key.
     *
     * @param key       The key associated with the publisher.
     * @param publisher The publisher instance to be cached.
     */
    public void putPublisher(ClientKey key, Publisher publisher) {
        publisherCache.put(key, publisher);
    }

    /**
     * Retrieves a publisher from the cache using the specified key.
     *
     * @param key The key associated with the publisher.
     * @return The cached publisher instance, or null if not found.
     */
    public Publisher getPublisher(ClientKey key) {
        return publisherCache.get(key);
    }

    /**
     * Adds an RPC client to the cache with the specified key.
     *
     * @param key       The key associated with the RPC client.
     * @param rpcClient The RPC client instance to be cached.
     */
    public void putRpcClient(ClientKey key, RpcClient rpcClient) {
        rpcClientCache.put(key, rpcClient);
    }

    /**
     * Retrieves an RPC client from the cache using the specified key.
     *
     * @param key The key associated with the RPC client.
     * @return The cached RPC client instance, or null if not found.
     */
    public RpcClient getRpcClient(ClientKey key) {
        return rpcClientCache.get(key);
    }

    /**
     * Connects to the RabbitMQ broker. This method is a placeholder and does not perform any operations.
     *
     * @param connectionConfig The connection configuration.
     * @throws ConnectException If there is an error during the connection process.
     */
    @Override
    public void connect(ConnectionConfig connectionConfig) throws ConnectException {
        //Nothing to do here
    }

    @Override
    public void close() {

        try {
            // Close all cached publishers
            for (Publisher publisher : publisherCache.values()) {
                if (publisher != null) {
                    try {
                        publisher.close(); // Close the publisher
                    } catch (AmqpException e) {
                        log.error("Error closing RabbitMQ Publisher", e);
                    }
                }
            }


            // Close all cached RPC clients
            for (RpcClient rpcClient : rpcClientCache.values()) {
                if (rpcClient != null) {
                    try {
                        rpcClient.close(); // Close the RPC client
                    } catch (AmqpException e) {
                        log.error("Error closing RabbitMQ RpcClient", e);
                    }
                }
            }

            // Close the RabbitMQ client connection
            if (clientConnection != null) {
                try {
                    clientConnection.close(); // Close the connection
                } catch (AmqpException e) {
                    log.error("Error closing RabbitMQ Connection", e);
                }
            }
        } finally {
            publisherCache.clear(); // Clear the publisher cache
            rpcClientCache.clear(); // Clear the RPC client cache
        }
    }
}
