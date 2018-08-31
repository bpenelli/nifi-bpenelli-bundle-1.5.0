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

import static groovy.json.JsonParserType.LAX;
import groovy.json.JsonSlurper;
import groovy.json.internal.ValueList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

@Tags({"convert, json, csv, schema, bpenelli"})
@CapabilityDescription("Converts JSON data to CSV data.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConvertJSONToCSV extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("FlowFiles that were successfully processed")
		.build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("FlowFiles with execution errors")
		.build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Schema")
        .description("The schema to use to map JSON to CSV.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor DELIM = new PropertyDescriptor.Builder()
        .name("Delimiter")
        .description("The the delimiter to use.")
        .required(true)
        .defaultValue(",")
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor COL_HEADERS = new PropertyDescriptor.Builder()
        .name("Include Column Headers")
        .description("If true, will output column headers.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor QUOTED = new PropertyDescriptor.Builder()
        .name("Output Quoted Fields")
        .description("If true, will output quoted fields.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor IS_DF = new PropertyDescriptor.Builder()
        .name("Is DataFrame Output")
        .description("Set to true if the JSON was created by a Spark DataFrame.ToJSON() call.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .expressionLanguageSupported(false)
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
        descriptors.add(SCHEMA);
        descriptors.add(DELIM);
        descriptors.add(COL_HEADERS);
        descriptors.add(QUOTED);
        descriptors.add(IS_DF);
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
	@SuppressWarnings("unchecked")
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    	FlowFile flowFile = session.get();
        if (flowFile == null) return;
        
        String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        String delim = context.getProperty(DELIM).evaluateAttributeExpressions(flowFile).getValue();
        boolean headers = context.getProperty(COL_HEADERS).asBoolean();
        boolean quoted = context.getProperty(QUOTED).asBoolean();
        boolean isDF = context.getProperty(IS_DF).asBoolean();

    	// Read content.
        ScopeFix sf = new ScopeFix();
        session.read(flowFile, new InputStreamCallback() {
        	@Override
            public void process(final InputStream inputStream) throws IOException {
        		sf.objectData = new JsonSlurper().setType(LAX).parseText(IOUtils.toString(inputStream, java.nio.charset.StandardCharsets.UTF_8));
        	}
        });
                
        Map<String, Object> schemaData = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(schema);
        
        final StringBuilder csv = new StringBuilder();
        boolean isFirstLine = true;
        boolean isFirstCol = true;
        
        try {

	        // Add CSV headers if requested.
        	if (headers) {
	        	ValueList fieldList = (ValueList) schemaData.get("fields");
	        	for (Object item : fieldList) {
	        		Map<String, Object> field = (Map<String, Object>) item;
	        		if (!isFirstCol) csv.append(delim);
	        		if (quoted) csv.append("\"");
	        		csv.append(field.get("name").toString());
	        		if (quoted) csv.append("\"");
	        		isFirstCol = false;
	        	}
	            isFirstLine = false;
	        }
	        
	        // Add CSV data lines.
        	while (((ValueList) sf.objectData).size() > 0) {
	        	Object rawRecord = ((ValueList) sf.objectData).get(0);
	        	Map<String, Object> record = null; 
	        	if (rawRecord instanceof String && isDF) {
        			record = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(rawRecord.toString());
        		} else {	        	
        			record = (Map<String, Object>) rawRecord;
        		}
	    		if (!isFirstLine) csv.append("\n");
	        	isFirstCol = true;
	        	for (Object item : (ValueList) schemaData.get("fields")) {
	        		Map<String, Object> field = (Map<String, Object>) item;
	        		String fieldName = field.get("name").toString();
	        		if (!isFirstCol) csv.append(delim);
		    		if (quoted) csv.append("\"");
		    		if (record.containsKey(fieldName)) {
		    			String fieldValue = record.get(fieldName).toString();
		    			if (quoted) fieldValue = fieldValue.replace("\"", "\"\"");
		    			csv.append(fieldValue);
		    		} else {
		    			csv.append("");
		    		}
		    		if (quoted) csv.append("\"");
		    		isFirstCol = false;
	        	}
	            isFirstLine = false;
	            ((ValueList) sf.objectData).remove(rawRecord);
	        }
	        
	        // Write CSV to the FlowFile's content.
            flowFile = session.write(flowFile, new OutputStreamCallback() {
            	@Override
                public void process(final OutputStream outputStream) throws IOException {
            		outputStream.write(csv.toString().getBytes("UTF-8"));
            	}
            });
	    
            session.transfer(flowFile, REL_SUCCESS);	        

        } catch (Exception e) {
        	e.printStackTrace();
            session.transfer(flowFile, REL_FAILURE);
        }
        session.commit();
    }
}