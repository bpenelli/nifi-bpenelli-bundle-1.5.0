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
import java.nio.charset.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"hold, release, topic, key, cache, flowfile, bpenelli"})
@CapabilityDescription("Allows one FlowFile through for a given topic and key, and holds up the remaining " +
	"FlowFiles for the same topic and key, until the first one is released by a companion Release processor.")
@SeeAlso(classNames = {"org.bpenelli.nifi.processors.Release"})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Hold extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("Any FlowFile that is successfully processed")
		.build();

    public static final Relationship REL_BUSY = new Relationship.Builder()
		.name("busy")
		.description("Any FlowFile whose topic and key haven't been released yet")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("Any FlowFile with an IO exception")
		.build();

    public static final PropertyDescriptor KEY_TOPIC = new PropertyDescriptor.Builder()
        .name("Topic")
        .description("The hold topic name.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor KEY_VALUE = new PropertyDescriptor.Builder()
        .name("Key")
        .description("The hold key.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
        .name("Distributed Map Cache Service")
        .description("The Controller Service providing map cache services.")
        .required(true)
        .expressionLanguageSupported(false)
        .identifiesControllerService(DistributedMapCacheClient.class)
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
        descriptors.add(KEY_TOPIC);
        descriptors.add(KEY_VALUE);
        descriptors.add(CACHE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_BUSY);
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
        
        final String keyTopic = context.getProperty(KEY_TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String keyValue = context.getProperty(KEY_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String holdKey = keyTopic + "." + keyValue;
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);
        
		try {
			if (cacheService.containsKey(holdKey, Utils.stringSerializer)) {
		        session.transfer(flowFile, REL_BUSY);
			} else {
				cacheService.put(holdKey, "holding", Utils.stringSerializer, Utils.stringSerializer);
		        session.transfer(flowFile, REL_SUCCESS);
			}
		} catch (IOException e) {
			session.transfer(flowFile, REL_FAILURE);
			getLogger().error("Unable to Hold topic and key for {} due to {}", new Object[] {flowFile, e});
		} finally {
			session.commit();
		}
    }
}
