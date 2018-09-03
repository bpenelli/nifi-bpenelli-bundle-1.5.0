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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bpenelli.nifi.services.HBaseMapCacheClient;

@Tags({"sequence", "auto", "increment", "assign", "cache", "flowfile", "hbase", "bpenelli"})
@CapabilityDescription("Assigns the next increment of a sequence stored in a HBase table "
	+ "to a FlowFile attribute and updates the table. Provides concurrency safeguards "
	+ "by rolling back the Session if the sequence value changes between read and update. "
	+ "Uses a HBaseMapCacheClientService controller to perform operations on HBase.")
@SeeAlso(classNames = {"org.bpenelli.nifi.services.HBaseMapCacheClientService"})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class HBaseSequence extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("Any FlowFile that is successfully processed")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("Any FlowFile with an IO exception")
		.build();

    public static final PropertyDescriptor SEQ_NAME = new PropertyDescriptor.Builder()
        .name("Sequence Name")
        .description("The name of the sequence (cache entry key) where the current value is maintained. If it doesn't already exist in cache, it will be created.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor START_NO = new PropertyDescriptor.Builder()
        .name("Start With")
        .description("The number to start with if the sequence doesn't yet exist and has to be created.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.LONG_VALIDATOR)
        .defaultValue("1")
        .build();

    public static final PropertyDescriptor INC_BY = new PropertyDescriptor.Builder()
        .name("Increment By")
        .description("The number to increment by.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.LONG_VALIDATOR)
        .defaultValue("1")
        .build();

    public static final PropertyDescriptor OUT_ATTR = new PropertyDescriptor.Builder()
        .name("Output Attribute")
        .description("The name of the attribute on the FlowFile to write the next Sequence value to.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("sequence.value")
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .build();

    public static final PropertyDescriptor HBASE_SVC = new PropertyDescriptor.Builder()
        .name("HBase Map Cache Client Service")
        .description("The Controller providing HBase map cache services.")
        .required(true)
        .expressionLanguageSupported(false)
        .identifiesControllerService(HBaseMapCacheClient.class)
        .addValidator(Validator.VALID)
        .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
    * init
    **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SEQ_NAME);
        descriptors.add(START_NO);
        descriptors.add(INC_BY);
        descriptors.add(OUT_ATTR);
        descriptors.add(HBASE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
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
        final String seqName = context.getProperty(SEQ_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String startNo = context.getProperty(START_NO).evaluateAttributeExpressions(flowFile).getValue();
        final long incBy = context.getProperty(INC_BY).evaluateAttributeExpressions(flowFile).asLong();
        final String outAttr = context.getProperty(OUT_ATTR).evaluateAttributeExpressions(flowFile).getValue();
        final HBaseMapCacheClient hbaseService = context.getProperty(HBASE_SVC).asControllerService(HBaseMapCacheClient.class);
		
		try {
			if (hbaseService.containsKey(seqName, Utils.stringSerializer)) {
				// Read the current sequence value.
				final String currentValue = hbaseService.get(seqName, Utils.stringSerializer, Utils.stringDeserializer);
				// Increment the value by the amount of the supplied increment.
				final String newValue = Objects.toString((Long.parseLong(currentValue) + incBy));
				// Only save if the value hasn't changed since it was read.
				if (hbaseService.checkAndPut(seqName, newValue, currentValue, Utils.stringSerializer, Utils.stringSerializer)) {
					flowFile = session.putAttribute(flowFile, outAttr, newValue);
					session.transfer(flowFile, REL_SUCCESS);
					session.commit();
				} else {
					// Rolling back will put the FlowFile back on the queue to be tried again.
					session.rollback();
				}
			} else {
				// The sequence doesn't exist, so add it with the supplied starting value.
				if (hbaseService.putIfAbsent(seqName, startNo, Utils.stringSerializer, Utils.stringSerializer)) {
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
