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
package org.wso2.integration.connector.operations;

import com.google.gson.JsonObject;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.exception.RabbitMQConnectorException;
import org.wso2.integration.connector.utils.Error;
import org.wso2.integration.connector.utils.RabbitMQUtils;

import static org.wso2.integration.connector.utils.RabbitMQConstants.ACKNOWLEDGE;
/**
 * This class represents the operation to accept a message in a RabbitMQ integration.
 * It sets the decision to acknowledge the message, ensuring that the message is
 * marked as successfully processed. This class is part of the WSO2 Integration framework
 * and extends the `AbstractConnectorOperation` to provide custom behavior.
 */
public class AcceptMessage extends AbstractConnectorOperation {

    /**
     * Executes the operation to acknowledge a RabbitMQ message. This method sets the
     * decision in the message context to acknowledge the message.
     *
     * @param messageContext   The Synapse message context containing the message details.
     * @param responseVariable The variable to store the response (not used in this method).
     * @param overwriteBody    Whether to overwrite the message body with the response (not used in this method).
     */
    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String messageID = messageContext.getMessageID();
        try {
            // Set the decision in the message context to acknowledge the message
            RabbitMQUtils.setDecision(messageContext, ACKNOWLEDGE);
            handleConnectorResponse(messageContext, responseVariable, false,
                    RabbitMQUtils.buildSuccessResponse(messageID), null, null);
        } catch (RabbitMQConnectorException e) {
            String errorDetail = "Error occurred while performing rabbitmq:accept operation with "
                    + "message id: " + messageID;
            handleError(
                    messageContext, e, RabbitMQUtils.getErrorCode(e),
                    errorDetail, responseVariable, false);
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

        JsonObject resultJSON = RabbitMQUtils.buildErrorResponse(msgCtx.getMessageID(), msgCtx, e, error);
        handleConnectorResponse(msgCtx, responseVariable, overwriteBody, resultJSON, null, null);
        handleException(errorDetail, e, msgCtx);
    }
}
