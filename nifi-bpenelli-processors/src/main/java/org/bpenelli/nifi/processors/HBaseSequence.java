/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bpenelli.nifi.processors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bpenelli.nifi.processors.utils.FlowUtils;
import org.bpenelli.nifi.processors.utils.HBaseUtils;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"sequence", "auto", "increment", "assign", "cache", "flowfile", "hbase", "bpenelli"})
@CapabilityDescription("Assigns the next increment of a sequence stored in a HBase table "
	+ "to a FlowFile attribute and updates the table. Provides concurrency safeguards "
	+ "by rolling back the Session if the sequence value changes between read and update. "
	+ "Uses a HBaseMapCacheClientService controller to perform operations on HBase.")
public class HBaseSequence extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("Any FlowFile that is successfully processed")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("Any FlowFile with an IO exception")
		.build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
        .name("Table Name")
        .description("The name of the HBase table to use to maintain the sequence.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor SEQ_NAME = new PropertyDescriptor.Builder()
        .name("Sequence Name")
        .description("The HBase row key where the current value is maintained. If it doesn't already exist in the HBase table, it will be created.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor COL_FAMILY = new PropertyDescriptor.Builder()
        .name("Column Family")
        .description("The name of the column family where the sequence value will be stored.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("f")
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor COL_QUALIFIER = new PropertyDescriptor.Builder()
        .name("Column Qualifier")
        .description("The name of the column qualifier where the sequence value will be stored.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("q")
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor START_NO = new PropertyDescriptor.Builder()
        .name("Start With")
        .description("The number to start with when the sequence doesn't yet exist and has to be created.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("1")
        .addValidator(StandardValidators.LONG_VALIDATOR)
        .build();

    public static final PropertyDescriptor INC_BY = new PropertyDescriptor.Builder()
        .name("Increment By")
        .description("The number to increment by.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("1")
        .addValidator(StandardValidators.LONG_VALIDATOR)
        .build();

    @SuppressWarnings("WeakerAccess")
    public static final PropertyDescriptor OUT_ATTR = new PropertyDescriptor.Builder()
        .name("Output Attribute")
        .description("The name of the attribute on the FlowFile to write the next Sequence value to.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("sequence.value")
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("HBase Client Service")
        .description("Specifies the HBase Client Controller Service to use for accessing HBase.")
        .required(true)
        .identifiesControllerService(HBaseClientService.class)
        .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
    * init
    **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TABLE_NAME);
        descriptors.add(SEQ_NAME);
        descriptors.add(COL_FAMILY);
        descriptors.add(COL_QUALIFIER);
        descriptors.add(START_NO);
        descriptors.add(INC_BY);
        descriptors.add(OUT_ATTR);
        descriptors.add(HBASE_CLIENT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**************************************************************
    * getRelationships
    **************************************************************/
    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    /**************************************************************
    * getSupportedPropertyDescriptors
    **************************************************************/
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    /**************************************************************
    * onScheduled
    **************************************************************/
    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
    
    /**************************************************************
    * onTrigger
    **************************************************************/
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    	FlowFile flowFile = session.get();
        if (flowFile == null) return;
        
        // Get property values.
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String seqName = context.getProperty(SEQ_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String colFam = context.getProperty(COL_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final String colQual = context.getProperty(COL_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String startNo = context.getProperty(START_NO).evaluateAttributeExpressions(flowFile).getValue();
        final long incBy = context.getProperty(INC_BY).evaluateAttributeExpressions(flowFile).asLong();
        final String outAttr = context.getProperty(OUT_ATTR).evaluateAttributeExpressions(flowFile).getValue();
        final HBaseClientService hbaseService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
		
		try {
			// Read the current sequence value if any.
			final String currentValue = HBaseUtils.get(hbaseService, tableName, colFam, colQual, seqName);

			if (currentValue != null) {
				// Increment the value by the amount of the supplied increment.
				final String newValue = Objects.toString((Long.parseLong(currentValue) + incBy));
				// Only save if the value hasn't changed since it was read.
				if (HBaseUtils.checkAndPut(hbaseService, tableName, colFam, colQual, seqName, newValue, currentValue)) {
					flowFile = session.putAttribute(flowFile, outAttr, newValue);
					session.transfer(flowFile, REL_SUCCESS);
					session.commit();
				} else {
					// Rolling back will put the FlowFile back on the queue to be tried again.
					session.rollback();
				}
			} else {
				// The sequence doesn't exist, so add it with the supplied starting value.
				if (HBaseUtils.putIfAbsent(hbaseService, tableName, colFam, colQual, seqName, startNo, 
						FlowUtils.stringSerializer, FlowUtils.stringSerializer)) {
					flowFile = session.putAttribute(flowFile, outAttr, startNo);
					session.transfer(flowFile, REL_SUCCESS);
				} else {
					// Rolling back will put the FlowFile back on the queue to be tried again.
					session.rollback();	
				}
			}
		} catch (IOException e) {
			session.transfer(flowFile, REL_FAILURE);
			getLogger().error("Unable to process {} due to {}", new Object[] {flowFile, e});
		}
    }
}
