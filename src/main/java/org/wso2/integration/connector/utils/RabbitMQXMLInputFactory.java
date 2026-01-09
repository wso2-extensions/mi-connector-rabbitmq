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

import org.apache.axiom.om.util.StAXUtils;

import java.util.Map;
import javax.xml.stream.XMLInputFactory;

/**
 * Utility class to provide a secure and configured XMLInputFactory instance.
 * This factory ensures that DTD support is disabled and coalescing is enabled
 * for enhanced security and simplified XML parsing within the RabbitMQ connector.
 * It uses the Initialization-on-demand holder idiom to ensure thread-safe, lazy
 * initialization of the singleton XMLInputFactory instance.
 */
public class RabbitMQXMLInputFactory {

    /**
     * Retrieves the singleton instance of the configured XMLInputFactory.
     *
     * @return The thread-safe, configured XMLInputFactory instance.
     */
    public static XMLInputFactory getXMLInputFactory() {
        return FactoryHolder.instance;
    }

    /**
     * Destroys the singleton instance of the XMLInputFactory.
     * This method resets the instance to null, allowing it to be reinitialized.
     */
    public static void destroyXMLInputFactoryHolder() {
        synchronized (FactoryHolder.class) {
            FactoryHolder.instance = null;
        }
    }


    private static class FactoryHolder {
        // The JVM guarantees that this static initialization block runs exactly once,
        // making it inherently thread-safe and lazy. This is the Initialization-on-demand holder idiom.
        private static XMLInputFactory instance;

        static {
            // Create the new factory instance
            instance = XMLInputFactory.newFactory();

            // Disable DTD support for security reasons
            instance.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
            // Enable coalescing to merge CDATA sections with character data
            instance.setProperty(XMLInputFactory.IS_COALESCING, true);

            // Load additional properties from the configuration file if available
            Map<?, ?> props = StAXUtils.loadFactoryProperties(RabbitMQConstants.XML_INPUT_FACTORY_PROPERTIES);
            if (props != null) {
                // Apply all loaded properties to the factory instance
                for (Map.Entry<?, ?> entry : props.entrySet()) {
                    instance.setProperty((String) entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
