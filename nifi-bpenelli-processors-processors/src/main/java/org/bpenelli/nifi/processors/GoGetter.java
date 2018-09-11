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

import groovy.json.JsonBuilder;
import static groovy.json.JsonParserType.LAX;
import groovy.json.JsonSlurper;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"gogetter, json, cache, attribute, sql, bpenelli"})
@CapabilityDescription("Retrieves values and outputs FlowFile attributes and/or a JSON object in the FlowFile's content based on a GOG configuration. " +
	"Values can be optionally retrieved from cache using a given key, or a database using given SQL.")
@SeeAlso({})
@WritesAttributes({@WritesAttribute(attribute="gog.error", description="The exception message for FlowFiles routed to failure.")})
public class GoGetter extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("Any FlowFile that is successfully processed")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("Any FlowFile with an exception")
		.build();

    public static final PropertyDescriptor GOG_TEXT = new PropertyDescriptor.Builder()
        .name("GOG Text")
        .description("The text of a GOG configuration JSON. If left empty, and 'Attribute Name' is empty, the FlowFile's content will be used.")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor GOG_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("Attribute Name")
        .description("The name of an attribute containing the GOG configuration JSON. If 'GOG Text' is empty, and this is left empty, the FlowFile's content will be used.")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
        .name("Distributed Map Cache Service")
        .description("The Controller Service containing the cached key map entries to retrieve.")
        .required(false)
        .expressionLanguageSupported(false)
        .identifiesControllerService(DistributedMapCacheClient.class)
        .addValidator(Validator.VALID)
        .build();
            
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
        .name("Database Connection Pooling Service")
        .description("The Controller service to use to obtain a database connection.")
        .required(false)
        .identifiesControllerService(DBCPService.class)
        .build();
        
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
    * init
    **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(GOG_TEXT);
        descriptors.add(GOG_ATTRIBUTE);
        descriptors.add(CACHE_SVC);
        descriptors.add(DBCP_SERVICE);
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
        
        final String gogAtt = context.getProperty(GOG_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final String gogText = context.getProperty(GOG_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        AtomicReference<String> gogConfig = new AtomicReference<String>();

        // Get the GOG configuration JSON.
        if (gogText != null && !gogText.isEmpty()) {
            gogConfig.set(gogText);
        } else if (gogAtt != null && !gogAtt.isEmpty()) {
            gogConfig.set(flowFile.getAttribute(gogAtt));
        } else {
        	gogConfig = Utils.readContent(session, flowFile);
        }

        final Map<String, Object> gog = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(gogConfig.get());
        
        try {
            
        	if (gog.containsKey("extract-to-json")) {
            	Extractor.extract((Map)gog.get("extract-to-json"), "extract-to-json", session, 
            			context, flowFile, cacheService, dbcpService);
            }
            
        	if (gog.containsKey("extract-to-attributes")) {
            	Extractor.extract((Map)gog.get("extract-to-attributes"), "extract-to-attributes", session, 
            			context, flowFile, cacheService, dbcpService);
            }
            
        	// Transfer the FlowFile to success.
            session.transfer(flowFile, REL_SUCCESS);
            
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.toString();
            flowFile = session.putAttribute(flowFile, "gog.error", msg);
            session.transfer(flowFile, REL_FAILURE);
        	getLogger().error("Unable to process {} due to {}", new Object[] {flowFile, e});
        }
    }

    private static class Extractor { 

    	@SuppressWarnings("rawtypes")
		final public static void extract (Map<String, Object> gogMap, String gogKey, ProcessSession session,
    			ProcessContext context, FlowFile flowFile, DistributedMapCacheClient cacheService, DBCPService dbcpService) throws Exception {

    		final Map<String, Object> valueMap = new TreeMap<String, Object>();
    		final Map<String, String> keysAndCacheKeys = new HashMap<String, String>();
    		final Map<String, String> keysAndTypes = new HashMap<String, String>();
    		final Map<String, String> keysAndDefaults = new HashMap<String, String>();
    		
    		for (final String key : gogMap.keySet()) {

    			final Object expression = gogMap.get(key);
    			boolean isCache = false;
	            String result = "";
	            String defaultValue = null;

	            if (expression instanceof Map) {
	            	Object value = ((Map) expression).get("value");

	            	if (value == null) {
	            		valueMap.put(key, null);
	            		continue;
	            	}

	            	result = Utils.evaluateExpression(context, flowFile, value.toString());

	                if (((Map) expression).containsKey("default")) {
	                	final Object itemDefault = ((Map) expression).get("default");
	                	if (itemDefault != null) defaultValue = itemDefault.toString();
	                }

	                final String valType = ((Map) expression).containsKey("type") ? ((Map) expression).get("type").toString() : "";

	                switch (valType) {
	                    case "CACHE_KEY": case "CACHE":
	                    	isCache = true;
	                    	if (!keysAndTypes.containsKey(result)) {
	                    		keysAndCacheKeys.put(key, result);
	                    		keysAndTypes.put(key, null);
	                    		keysAndDefaults.put(key, defaultValue);
	                    	}
	                        break;
	                    case "SQL":
	                        Sql sql = null;
	                        // Get the value from a SQL source.
	                        try {
	                            Connection conn = dbcpService.getConnection();
	                            sql = new Sql(conn);
	                            GroovyRowResult row = sql.firstRow(result);
	                            if (row != null) {
	                                final Object col = row.getAt(0);
	                                result = Utils.getColValue(col, defaultValue);
	                            } else {
	                                result = defaultValue;
	                            }
	                        } catch (Exception e) {
	                            throw e;
	                        } finally {
	                        	if (sql != null) sql.close();
	                        }
	                        break;
	                    default:
	                        // Use the literal, or the result of an expression if supplied.
	                        if (result == null || result.length() == 0) result = defaultValue;
	                        break;
	                }                
	            } else {
	            	if (expression == null) {
	            		valueMap.put(key, null);
	            		continue;
	            	}
	                result = Utils.evaluateExpression(context, flowFile, (String)expression);
	            }
	            
	            // Add the result to our value map.
	            if (expression instanceof Map && result != null && ((Map) expression).containsKey("to-type")) {
	            	final String newType = (String)((Map) expression).get("to-type");
	            	if (isCache) {
	            		keysAndTypes.put(key, newType);
	            	} else {
	            		valueMap.put(key, Utils.convertString(result, newType));
	            	}
	            } else {
	                if (!isCache) valueMap.put(key, result);
	            }
	            	            
	        }

            // Get requested cache values.
    		if (keysAndCacheKeys.size() > 0) {
    			Set<String> cacheKeys = new HashSet<String>();
    			for (String key : keysAndCacheKeys.keySet()) {
    				cacheKeys.add(keysAndCacheKeys.get(key));
    			}
	            Map<String, String> cacheMap = cacheService.subMap(cacheKeys, Utils.stringSerializer, Utils.stringDeserializer);
	            for (String key : keysAndCacheKeys.keySet()) {
	            	String cacheKey = keysAndCacheKeys.get(key);
	            	String newType = keysAndTypes.get(key);
	            	Object value = keysAndDefaults.get(key);
	            	String cacheValue = cacheMap.get(cacheKey);
	            	if (cacheValue != null && cacheValue.length() > 0) {
		            	if (newType != null) {
		            		value = Utils.convertString(cacheValue, newType);
		            	} else {
			            	value = cacheValue;
		            	}
	            	} else if (value != null && value.toString().length() > 0 && newType != null) {
		            	value = Utils.convertString(value.toString(), newType);
	            	}
	            	valueMap.put(key, value);
	            }
    		}
    		
    		switch (gogKey) {
    			case "extract-to-json":
    	            // Build a JSON object for these results and put it in the FlowFile's content.
    	            final JsonBuilder builder = new JsonBuilder();
    	            builder.call(valueMap);
    	            flowFile = Utils.writeContent(session, flowFile, builder.toString());
    	            break;
    			case "extract-to-attributes":
    	            // Add FlowFile attributes for these results.
    	            for (final String key : valueMap.keySet()) {
    	            	flowFile = session.putAttribute(flowFile, key, (String)valueMap.get(key));
    	            }
    	            break;
    		}    		
    	}
    }
}
