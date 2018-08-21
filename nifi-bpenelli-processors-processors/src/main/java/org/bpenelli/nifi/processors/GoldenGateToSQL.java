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

///////////////////////////////////////////////////////////////
/// GoldenGateToSQL Class
///////////////////////////////////////////////////////////////
@Tags({"goldengate, sql, trail, json, bpenelli"})
@CapabilityDescription("Parses an Oracle GoldenGate trail file and builds a corresponding SQL statement.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GoldenGateToSQL extends AbstractProcessor {

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("FlowFiles that were successfully processed")
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
        
    public static final PropertyDescriptor SEMICOLON = new PropertyDescriptor.Builder()
        .name("Include Semicolon")
        .description("If true, a semicolon will be added to the end of the SQL statement.")
        .required(true)
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("SQL Attribute")
        .description("The name of the attribute to output the SQL to. If left empty, it will be written to the FlowFile's content.")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor KEYCOLS = new PropertyDescriptor.Builder()
        .name("Key Columns")
        .description("Comma separated list of key column names. Only needed if the primary_keys property is not in the trail file, or to override.")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(Validator.VALID)
        .build();

    public static final String colMapDesc = "Ex: SRCTBL.CUST_ID | CUSTOMER_ID ";
        
    public static final PropertyDescriptor COLMAP = new PropertyDescriptor.Builder()
        .name("*** Add Column Mapping Below ***")
        .description("For column mapping, add dynamic properties, i.e. PropertyName: <SrcTableName>.<SrcColName>, PropertyValue: <TargColName>.")
        .required(true)
        .expressionLanguageSupported(false)
        .addValidator(Validator.VALID)
        .allowableValues(colMapDesc)
        .defaultValue(colMapDesc)
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
        descriptors.add(SEMICOLON);
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(COLMAP);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
        
    	ScopeFix sf = new ScopeFix();
        session.read(flowFile, new InputStreamCallback() {
        	@Override
            public void process(final InputStream inputStream) throws IOException {       		
        		sf.content = IOUtils.toString(inputStream, java.nio.charset.StandardCharsets.UTF_8);
        	}
        });
        JsonSlurper slurper = new JsonSlurper();
        Map content = (Map)slurper.parseText(sf.content);

        int i = 0;
        String sql = "";
        String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        boolean includeSemicolon = context.getProperty(SEMICOLON).asBoolean();
        String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions().getValue();
        String keyCols = context.getProperty(KEYCOLS).evaluateAttributeExpressions().getValue();
        String toCase = context.getProperty(TO_CASE).getValue();
        String[] pk = new String[0];

        if (keyCols != null && keyCols.length() > 0) {
        	pk = keyCols.split(",");
        } else if (content.containsKey("primary_keys")) {
        	pk = ((ArrayList<String>)content.get("primary_keys")).toArray(pk);
        }
        
    	String table = (String)content.get("table");
        String tableName = table.substring(table.indexOf(".") + 1);
        String opType = (String)content.get("op_type");
        Map before = (Map)content.get("before");
        Map after = (Map)content.get("after");
        
        if (schema != null) {
            if (schema.length() > 0) {
                table = schema + table.substring(table.indexOf("."));
            } else {
                table = tableName;
            }
        }
        table = applyCase(table, toCase);

        // Build the SQL
        if (opType.equals("I")) {
            // Insert statement
            String cols = "(";
            String vals = "VALUES (";
            sql += "INSERT INTO " + table + " ";
            for (String item : (Set<String>)after.keySet() ) {
            	//String value = null;
            	//if (itemVal != null) value = itemVal.toString();
                String colName = applyColMap(context, flowFile, tableName, item, toCase);
                if (i > 0) {
                    cols += ", ";
                    vals += ", ";
                }
                cols += colName;
            	Object value = after.get(item);
                if (value != null) {
                    String val = value.toString().replace("'", "''");
                    vals += "'" + val + "'";
                } else {
                    vals += "null";
                }
                i++;
            }
            cols += ")";
            vals += ")";
            sql += cols + " " + vals;
        } else {
            if (pk.length == 0) throw new ProcessException("Primary key column(s) are required for this operation.");
            if (opType.equals("U")) {
                 // Update statement
                sql += "UPDATE " + table + " SET ";
                for (String col : (Set<String>)after.keySet() ) {
                	boolean isPk = false; 
                	for (String p : pk) {
                		if (col.equals(p)) {
                			isPk = true;
                			break;
                		}
                	}
                	if (!isPk) {
	                    String colName = applyColMap(context, flowFile, tableName, col, toCase);
	                    if (i > 0) sql += ", ";
	                    sql += colName + " = ";
	                	Object colValue = after.get(col);
	                    if (colValue != null) {
	                        String val = colValue.toString().replace("'", "''");
	                        sql += "'" + val + "'";
	                    } else {
	                        sql += "null";
	                    }                    
	                    i++;
                	}
                }
            } else if (opType.equals("D")) {
                // Delete statement
                sql += "DELETE FROM " + table;
            }
           // Where clause for the update or delete.
            i = 0;
            sql += " WHERE ";
            for (String col : pk) {
                String colName = applyColMap(context, flowFile, tableName, col, toCase);
                if (i > 0) sql += " AND ";
                sql += colName + " ";
                Object colValue = before.get(col);
                if (colValue != null) {
                    String val = colValue.toString().replace("'", "''");
                    sql += "= '" + val + "'";
                } else {
                    sql += "IS null";
                }                    
                i++;
            }
        }
        
        if (includeSemicolon) sql += ";";

        // Output the SQL
        if (attName != null && !attName.isEmpty()) {
            flowFile = session.putAttribute(flowFile, attName, sql);
        } else {
        	sf.content = sql;
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
