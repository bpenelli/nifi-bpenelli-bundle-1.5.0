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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.*;
import java.sql.Clob;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

@Tags({"gogetter, json, cache, attribute, sql, bpenelli"})
@CapabilityDescription("Retrieves values and builds a JSON object in the FlowFile's content based on a GOG configuration.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="gog.error", description="The execution error message for FlowFiles routed to failure.")})
public class GoGetter extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("FlowFiles that were successfully processed")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("FlowFiles with GoGetter execution errors")
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
        
        String gogAtt = context.getProperty(GOG_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        String gogText = context.getProperty(GOG_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);
        DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        String gogConfig = "";

        // Get the GOG configuration JSON.
        if (gogText != null && !gogText.isEmpty()) {
            gogConfig = gogText;
        } else if (gogAtt != null && !gogAtt.isEmpty()) {
            gogConfig = flowFile.getAttribute(gogAtt);
        } else {
        	ScopeFix sf = new ScopeFix();
            session.read(flowFile, new InputStreamCallback() {
            	@Override
                public void process(final InputStream inputStream) throws IOException {
            		sf.content = IOUtils.toString(inputStream, java.nio.charset.StandardCharsets.UTF_8);
            	}
            });
            gogConfig = sf.content;
        }

        Map<String, Object> gog = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(gogConfig);
        
        try {
            if (gog.containsKey("extract-to-json")) {
            	goGetter.get((Map)gog.get("extract-to-json"), "extract-to-json", session, 
            			context, flowFile, cacheService, dbcpService);
            }
            if (gog.containsKey("extract-to-attributes")) {
            	goGetter.get((Map)gog.get("extract-to-attributes"), "extract-to-attributes", session, 
            			context, flowFile, cacheService, dbcpService);
            }
            // Transfer the FlowFile to success.
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
        	e.printStackTrace();
            String msg = e.getMessage();
            if (msg == null) msg = e.toString();
            getLogger().error(msg);
            flowFile = session.putAttribute(flowFile, "gog.error", msg);
            // Transfer the FlowFile to failure.
            session.transfer(flowFile, REL_FAILURE);
        }
    
        session.commit();
        
    }

    ///////////////////////////////////////////////////////////////
    /// goGetter Class
    ///////////////////////////////////////////////////////////////
    private static class goGetter { 
        private static Serializer<String> stringSerializer = new Serializer<String>() {
        	@Override
        	public void serialize(String stringValue, OutputStream out)
        			throws SerializationException, IOException {
        		out.write(stringValue.getBytes(StandardCharsets.UTF_8));
        	}
		};
        private static Deserializer<String> stringDeserializer = new Deserializer<String>() {
        	@Override
        	public String deserialize(byte[] bytes) throws DeserializationException, IOException {
        		return new String(bytes);
        	}	                        	
		};
        private static String evaluateExpression(final ProcessContext context, final FlowFile flowFile, final String expression) {
            PropertyValue newPropVal = context.newPropertyValue(expression);
            String result = newPropVal.evaluateAttributeExpressions(flowFile).getValue();
            return result;
        }

        /**************************************************************
         * get
         **************************************************************/
    	@SuppressWarnings("rawtypes")
		public static void get (Map<String, Object> gogMap, String gogKey, ProcessSession session,
    			ProcessContext context, FlowFile flowFile, DistributedMapCacheClient cacheService, DBCPService dbcpService) throws Exception {
    		Map<String, Object> valueMap = new TreeMap<String, Object>();
    		for (String key : gogMap.keySet()) {
    			Object expression = gogMap.get(key); 
	            String result = "";
	            String defaultValue = null;
	            if (expression instanceof Map) {
	            	Object value = ((Map) expression).get("value");
	            	if (value == null) {
	            		valueMap.put(key, null);
	            		continue;
	            	}
	                result = goGetter.evaluateExpression(context, flowFile, value.toString());
	                if (((Map) expression).containsKey("default")) {
	                	Object itemDefault = ((Map) expression).get("default");
	                	if (itemDefault != null) defaultValue = itemDefault.toString();
	                }
	                String valType = ((Map) expression).containsKey("type") ? ((Map) expression).get("type").toString() : "";
	                switch (valType) {
	                    case "CACHE_KEY":
	                        // Get the value from a cache source.
	                        if (cacheService.containsKey(result, goGetter.stringSerializer)) {
	                            result = cacheService.get(result, goGetter.stringSerializer, goGetter.stringDeserializer);
	                        } else {
	                            result = defaultValue;
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
	                                Object col = row.getAt(0);
	                                if (col instanceof Clob) {
	                                    Reader stream = ((Clob)col).getCharacterStream();
	                                    StringWriter writer = new StringWriter();
	                                    IOUtils.copy(stream, writer);
	                                    result = writer.toString();
	                                } else {
	                                    result = col != null ? col.toString() : defaultValue;
	                                }
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
	                result = goGetter.evaluateExpression(context, flowFile, (String)expression);
	            }
	            
	            // Add the result to our value map.
	            if (expression instanceof Map && result != null && ((Map) expression).containsKey("to-type")) {
	            	String newType = (String)((Map) expression).get("to-type");
	            	switch (newType) {
		            	case "int" : 
		            		valueMap.put(key, Integer.parseInt(result));
		            		break;
		            	case "long" : 
		            		valueMap.put(key, Long.parseLong(result));
		            		break;
		            	case "decimal" :
		            		valueMap.put(key, Float.parseFloat(result));
		            		break;
		            	default:
		            		valueMap.put(key, result);
		            		break;
	            	}
	            } else {
	                valueMap.put(key, result);
	            }
	        }
    		
	        if (gogKey == "extract-to-json") {
	            // Build a JSON object for these results and put it in the FlowFile's content.
	            JsonBuilder builder = new JsonBuilder();
	            builder.call(valueMap);
	            flowFile = session.write(flowFile, new OutputStreamCallback() {
	            	@Override
	                public void process(final OutputStream outputStream) throws IOException {
	            		outputStream.write(builder.toString().getBytes("UTF-8"));
	            	}
	            });
	        }
	        if (gogKey == "extract-to-attributes") {
	            // Add FlowFile attributes for these results.
	            for (String key : valueMap.keySet()) {
	            	flowFile = session.putAttribute(flowFile, key, (String)valueMap.get(key));
	            }
	        }
    	}
    }

}
