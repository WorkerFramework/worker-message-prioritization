/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.microfocus.fas.worker.prioritization.rerouting.mutators;

import com.google.common.base.Strings;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowQueueNameMutator extends QueueNameMutator {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowQueueNameMutator.class);

    @Override
    public void mutateSuccessQueueName(Document document) {
        final String customDataWorkflowName = document.getCustomData("workflowName");
        final Field fieldWorkflowName = document.getField("CAF_WORKFLOW_NAME");

        final String workflowName;
        if(fieldWorkflowName != null && fieldWorkflowName.hasValues()) {
            workflowName = fieldWorkflowName.getStringValues().get(0);
        }
        else {
            workflowName = customDataWorkflowName;
        }
        
        if(Strings.isNullOrEmpty(workflowName)) {
            LOGGER.trace("No workflow name, unable to mutate queueName {}.",
                    document.getTask().getResponse().getSuccessQueue().getName());
            return;
        }

        setCurrentSuccessQueueName(document, getCurrentSuccessQueueName(document) + "/" + workflowName);

    }
}
