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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * A utility class that provides a thread-safe registry for managing RabbitMQ declaration locks.
 * This class uses a `ConcurrentHashMap` to store locks associated with specific keys.
 * It ensures proper synchronization and supports clearing the lock map when needed.
 */
public final class RabbitMQDeclarationLockManager {

    /**
     * Retrieves the declaration lock map, which is a thread-safe map
     * used to manage locks for RabbitMQ declarations.
     *
     * @return A `ConcurrentHashMap` containing the declaration locks.
     */
    public static ConcurrentHashMap<String, Lock> getDeclarationLockMap() {
        return Holder.DECLARATION_LOCK_MAP;
    }

    /**
     * Clears all the locks in the declaration lock map.
     * This method ensures thread-safe clearing of the map by synchronizing on it.
     */
    public static void destroyMap() {
        synchronized (Holder.DECLARATION_LOCK_MAP) {
            Holder.DECLARATION_LOCK_MAP.clear();
        }
    }

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private RabbitMQDeclarationLockManager() {
    }

    /**
     * The Initialization-on-demand Holder class (unchanged).
     */
    private static class Holder {

        // The JVM guarantees one-time, thread-safe initialization.
        private static final ConcurrentHashMap<String, Lock> DECLARATION_LOCK_MAP =
                new ConcurrentHashMap<>();
    }
}
