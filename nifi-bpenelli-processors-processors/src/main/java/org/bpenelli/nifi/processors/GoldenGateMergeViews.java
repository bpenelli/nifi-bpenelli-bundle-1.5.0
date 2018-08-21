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
import groovy.json.JsonSlurper;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

///////////////////////////////////////////////////////////////
/// GoldenGateMergeViews Class
///////////////////////////////////////////////////////////////
@Tags({"goldengate, merge, trail, json, bpenelli"})
@CapabilityDescription("Merges the before and after views of an Oracle GoldenGate trail file to create a merged view, and outputs it as JSON.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GoldenGateMergeViews extends AbstractProcessor {

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("FlowFiles that were successfully processed")
		.build();

	public static final Relationship REL_UNSUPPORTED = new Relationship.Builder()
			.name("unsupported op_type")
			.description("FlowFiles containing an unsupported Golden Gate op_type")
			.build();

	public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
        .name("Trail File Format")
        .description("The format of the trail file.")
        .required(true)
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .allowableValues("JSON")
        .defaultValue("JSON")
        .build();

    public static final PropertyDescriptor TO_CASE = new PropertyDescriptor.Builder()
        .name("Convert Case")
        .description("Convert table and column name case.")
        .required(true)
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .allowableValues("None", "Upper", "Lower")
        .defaultValue("None")
        .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Target Schema")
        .description("The target schema name. Only needed if overriding the one in the trail file.")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();
        
    public static final PropertyDescriptor GG_FIELDS = new PropertyDescriptor.Builder()
        .name("Include Golden Gate Fields")
        .description("A comma delimited list of Golden Gate single value header fields that should be added to the final JSON.")
        .required(false)
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("Output Attribute Name")
        .description("The name of the attribute to output the JSON to. If left empty, it will be written to the FlowFile's content.")
        .required(false)
        .expressionLanguageSupported(true)
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
        descriptors.add(FORMAT);
        descriptors.add(TO_CASE);
        descriptors.add(SCHEMA);
        descriptors.add(GG_FIELDS);
        descriptors.add(ATTRIBUTE_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_UNSUPPORTED);
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
    public void onScheduled(final ProcessContext context) {}

    /**************************************************************
    * applyCase
    **************************************************************/
    String applyCase(String stringVal, String toCase) {
        switch (toCase) {
            case "Upper": return stringVal.toUpperCase(); 
            case "Lower": return stringVal.toLowerCase();
            default: return stringVal;
        }
    }

    /**************************************************************
    * applyColMap
    **************************************************************/
    String applyColMap(ProcessContext context, FlowFile flowFile, String sourceTableName, 
    		String sourceColName, String toCase) {
        String colMapKey = sourceTableName + "." + sourceColName;
        String colName = sourceColName;
        for (PropertyDescriptor p : context.getProperties().keySet()) {
            if (p.isDynamic() && p.getName() == colMapKey) {
                PropertyValue propVal = context.getProperty(p);
                colName = propVal.evaluateAttributeExpressions(flowFile).getValue();
                break;
            }
        }
        colName = applyCase(colName, toCase);
        return colName;
    }

    /**************************************************************
    * onTrigger
    **************************************************************/
    @SuppressWarnings({ "unchecked" })
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
        
    	// Read the FlowFile's contents in.
        ScopeFix sf = new ScopeFix();
        session.read(flowFile, new InputStreamCallback() {
        	@Override
            public void process(final InputStream inputStream) throws IOException {       		
        		sf.content = IOUtils.toString(inputStream, java.nio.charset.StandardCharsets.UTF_8);
        	}
        });

        // Verify it's a supported op_type, i.e. Insert or Update.
        JsonSlurper slurper = new JsonSlurper();
        Map<String, Object> content = (Map<String, Object>)slurper.parseText(sf.content);
        String opType = (String)content.get("op_type");
        if (!opType.equals("I") && !opType.equals("U")) {
            // Unsupported
            session.transfer(flowFile, REL_UNSUPPORTED);
            session.commit();
            return;
        }

        String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        String ggFieldsCSV = context.getProperty(GG_FIELDS).getValue();
        String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions().getValue();
        String toCase = context.getProperty(TO_CASE).getValue();
    	String table = (String)content.get("table");
        String tableName = table.substring(table.indexOf(".") + 1);
        Map<String, Object> before = (Map<String, Object>)content.get("before");
        Map<String, Object> after = (Map<String, Object>)content.get("after");
        Map<String, Object> jsonMap = new TreeMap<String, Object>();

        content.remove("primary_keys");
        content.remove("before");
        content.remove("after");
        
        String[] ggFields = null;
        
        if (ggFieldsCSV != null) {
        	ggFields = ggFieldsCSV.split(",");
        }

        if (schema != null) {
            if (schema.length() > 0) {
                table = schema + table.substring(table.indexOf("."));
            } else {
                table = tableName;
            }
        }
        table = applyCase(table, toCase);

        if (ggFields != null && ggFields.length > 0) {
        	for (String field : ggFields) {
		        for (String key : content.keySet()) {
	        		if (field.equals(key)) {
			        	Object val = content.get(key);
			        	if (!(val instanceof Map) && !(val instanceof ArrayList)) {
			        		if (key.equals("table")) val = table;
			        		jsonMap.put(this.applyCase(key, toCase), val);
			        	}
			        	break;
	        		}
		        }
        	}
    	}
        if (before != null) {
	        for (String key : before.keySet()) {
	        	jsonMap.put(this.applyCase(key, toCase), before.get(key));
	        }
        }
        if (after != null) {
	        for (String key : after.keySet()) {
	        	jsonMap.put(this.applyCase(key, toCase), after.get(key));
	        }
        }
        
        // Build a JSON object for these results and put it in the FlowFile's content.
        JsonBuilder builder = new JsonBuilder();
        builder.call(jsonMap);
        String json = builder.toString();
        
        // Output the JSON
        if (attName != null && !attName.isEmpty()) {
            flowFile = session.putAttribute(flowFile, attName, json);
        } else {
        	sf.content = json;
            flowFile = session.write(flowFile, new OutputStreamCallback() {
            	@Override
                public void process(final OutputStream outputStream) throws IOException {
            		outputStream.write(sf.content.getBytes("UTF-8"));
            	}
            });
        }
        
        // Success!
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
        
    }
}
