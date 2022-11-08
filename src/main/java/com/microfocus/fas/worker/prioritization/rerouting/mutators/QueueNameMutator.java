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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class QueueNameMutator {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueNameMutator.class);    
    
    public abstract void mutateSuccessQueueName(final Document document);

    protected String getCurrentSuccessQueueName(final Document document) {
        return document.getTask().getResponse().getSuccessQueue().getName();
    }
    
    protected void setCurrentSuccessQueueName(final Document document, final String name) {
        Objects.requireNonNull(document);
        
        if(Strings.isNullOrEmpty(name)) {
            LOGGER.error("Cannot change success queue from {} to null or empty string.", 
                    document.getTask().getResponse().getSuccessQueue().getName());
            return;
        }
        
        if(name.length() > 255) {
            LOGGER.error(
                "Cannot change success queue from {} to to {} as it will exceed the maximum queue name length of 255.",
                document.getTask().getResponse().getSuccessQueue().getName(), name);
            return;
        }
        
        document.getTask().getResponse().getSuccessQueue().set(name);        
    }
}
