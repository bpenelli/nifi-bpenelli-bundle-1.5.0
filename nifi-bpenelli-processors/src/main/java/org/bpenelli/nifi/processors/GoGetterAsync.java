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
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import groovyjarjarcommonscli.MissingArgumentException;
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
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.bpenelli.nifi.processors.utils.*;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

import static groovy.json.JsonParserType.LAX;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"gogetter", "get", "json", "cache", "attribute", "sql", "hbase", "bpenelli"})
@CapabilityDescription("Retrieves values and outputs FlowFile attributes and/or a JSON object in the FlowFile's " +
        "content based on a GOG configuration. Values can be optionally retrieved from cache using a given key, " +
        "and/or a database using given SQL, and/or a HBase table scan using a given filter expression.")
@SeeAlso()
@ReadsAttributes({@ReadsAttribute(attribute = "")})
@WritesAttributes({
        @WritesAttribute(attribute = "gog.failure.reason", description = "The reason the FlowFile was sent to failure relationship."),
        @WritesAttribute(attribute = "gog.failure.sql", description = "The SQL assigned when the FlowFile was sent to failure relationship."),
        @WritesAttribute(attribute = "gog.failure.hbase.filter", description = "The HBase filter expression assigned when the FlowFile was sent to failure relationship."),
        @WritesAttribute(attribute = "gog.failure.hbase.table", description = "The HBase table assigned when the FlowFile was sent to failure relationship.")
})
public class GoGetterAsync extends AbstractProcessor {

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

    public static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the HBase Client Controller Service to use for accessing HBase.")
            .required(false)
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
        descriptors.add(GOG_TEXT);
        descriptors.add(GOG_ATTRIBUTE);
        descriptors.add(CACHE_SVC);
        descriptors.add(DBCP_SERVICE);
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
    @SuppressWarnings({"unchecked"})
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        final String gogAtt = context.getProperty(GOG_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final String gogText = context.getProperty(GOG_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final HBaseClientService hbaseService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        String gogConfig;

        // Get the GOG configuration JSON.
        if (gogText != null && !gogText.isEmpty()) {
            gogConfig = gogText;
        } else if (gogAtt != null && !gogAtt.isEmpty()) {
            gogConfig = flowFile.getAttribute(gogAtt);
        } else {
            gogConfig = FlowUtils.readContent(session, flowFile).get();
        }

        // Process GOG.
        try {

            final Map<String, Object> gog = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(gogConfig);

            // Process extract-to-attributes.
            if (gog.containsKey("extract-to-attributes")) {
                Extractor.extract((Map<String, Object>) gog.get("extract-to-attributes"), "extract-to-attributes", session,
                        context, flowFile, cacheService, dbcpService, hbaseService);
            }

            // Process extract-to-json.
            if (gog.containsKey("extract-to-json")) {
                Extractor.extract((Map<String, Object>) gog.get("extract-to-json"), "extract-to-json", session,
                        context, flowFile, cacheService, dbcpService, hbaseService);
            }

            // Transfer the FlowFile to success.
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            flowFile = session.putAttribute(flowFile, "gog.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to process {} due to {}", new Object[]{flowFile, e});
        }
    }

    private static class Extractor {

        /**************************************************************
         * extract
         **************************************************************/
        @SuppressWarnings({"unchecked"})
        public static void extract(Map<String, Object> gogMap, String gogKey, ProcessSession session,
                                   ProcessContext context, FlowFile flowFile, DistributedMapCacheClient cacheService,
                                   DBCPService dbcpService, HBaseClientService hbaseService) throws Exception {

            final Map<String, Object> valueMap = new TreeMap<>();
            ExecutorService executor = Executors.newWorkStealingPool();
            List<Callable<GoGetterCallResult>> goGetterCalls = new ArrayList<>();

            for (final String key : gogMap.keySet()) {

                final Object expression = gogMap.get(key);

                // Handle simple type property.
                if (!(expression instanceof Map)) {
                    if (expression == null) {
                        valueMap.put(key, null);
                        continue;
                    }
                    // Evaluate any supplied expression language.
                    final String result = FlowUtils.evaluateExpression(context, flowFile, expression.toString());
                    // Add the result to our value map.
                    valueMap.put(key, result);
                    continue;
                }

                // Handle complex type property.
                Object defaultValue = null;
                String result;
                Map<String, Object> propMap = (Map<String, Object>) expression;

                // Get default property.
                if (propMap.containsKey("default")) {
                    defaultValue = propMap.get("default");
                }

                // Get to-type property.
                String toType = null;
                if (propMap.containsKey("to-type")) {
                    toType = propMap.get("to-type").toString();
                }

                // Get value property.
                Object value = propMap.get("value");
                if (value == null || value.toString().isEmpty()) {
                    valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                    continue;
                }

                // Get value expression language result.
                result = FlowUtils.evaluateExpression(context, flowFile, value.toString());

                // If value result is null or empty then use default value.
                if (result == null || result.isEmpty()) {
                    valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                    continue;
                }

                // Get type property.
                final String valType = propMap.containsKey("type") ? propMap.get("type").toString() : "";

                // Type handler.
                switch (valType) {
                    case "CACHE_KEY":
                    case "CACHE":
                        // Get the value from a DistributedMapCacheClient source asynchronously.
                        GoGetterCacheCallable callable = new GoGetterCacheCallable(key, defaultValue, toType, cacheService, result);
                        goGetterCalls.add(callable);
                        continue;
                    case "HBASE_FILTER":
                    case "HBASE_SCAN":
                    case "HBASE":
                        // Get the value from a HBaseClientService source asynchronously.
                        if (!propMap.containsKey("hbase-table")) {
                            throw new MissingArgumentException("hbase-table argument missing for " + key);
                        }
                        final String hbaseTable = propMap.get("hbase-table").toString();
                        GoGetterHBaseCallable getHbase = new GoGetterHBaseCallable(key, defaultValue, toType, hbaseService, hbaseTable, result);
                        goGetterCalls.add(getHbase);
                        continue;
                    case "SQL":
                        Sql sql = null;
                        final String sqlText = result;
                        // Get the value from a DBCPService source.
                        try {
                            Connection conn = dbcpService.getConnection();
                            sql = new Sql(conn);
                            GroovyRowResult row = sql.firstRow(sqlText);
                            if (row != null) {
                                final Object col = row.getAt(0);
                                result = FlowUtils.getColValue(col, null);
                                if (result == null || result.isEmpty()) {
                                    valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                                    continue;
                                }
                            } else {
                                valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                                continue;
                            }
                        } catch (Exception e) {
                            //noinspection UnusedAssignment
                            flowFile = session.putAttribute(flowFile, "gog.failure.sql", sqlText);
                            throw e;
                        } finally {
                            if (sql != null) sql.close();
                        }
                        break;
                    default:
                        // No type specified, so value result is a literal.
                        break;
                }

                // Add the result to our value map, after any specified type conversion.
                valueMap.put(key, FlowUtils.convertString(result, toType));
            }

            // Invoke and gather the results of the GoGetter async calls.
            try {
                List<Future<GoGetterCallResult>> futureList = executor.invokeAll(goGetterCalls);
                for (Future<GoGetterCallResult> future : futureList) {
                    GoGetterCallResult callResult = future.get();
                    if (callResult.result == null || callResult.result.isEmpty()) {
                        valueMap.put(callResult.key, FlowUtils.convertString(callResult.defaultValue,
                                callResult.toType));
                    } else {
                        valueMap.put(callResult.key, FlowUtils.convertString(callResult.result,
                                callResult.toType));
                    }
                }
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof GoGetterCallException) {
                    GoGetterCallException ce = (GoGetterCallException) e.getCause();
                    for (String attName : ce.failureAttributes.keySet()) {
                        flowFile = session.putAttribute(flowFile, attName, ce.failureAttributes.get(attName));
                    }
                    throw ce.originalException;
                }
            }

            // Shutdown the executor service.
            executor.shutdownNow();

            // Output the extracted results.
            if (Objects.equals(gogKey, "extract-to-json")) {
                // Build a JSON object for these results and put it in the FlowFile's content.
                final JsonBuilder builder = new JsonBuilder();
                builder.call(valueMap);
                FlowUtils.writeContent(session, flowFile, builder);
            }
            if (Objects.equals(gogKey, "extract-to-attributes")) {
                // Add FlowFile attributes for these results.
                for (final String key : valueMap.keySet()) {
                    flowFile = session.putAttribute(flowFile, key, valueMap.get(key).toString());
                }
            }
        }
    }
}