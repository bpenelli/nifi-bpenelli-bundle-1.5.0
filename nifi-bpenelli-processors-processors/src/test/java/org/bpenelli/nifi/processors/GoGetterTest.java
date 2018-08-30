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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import static org.junit.Assert.*;

public class GoGetterTest {

    /**
     * Test of onTrigger method, of class GoGetter.
     */
    @org.junit.Test
    public void testOnTrigger() throws IOException {
        // Add content.
        InputStream content = new ByteArrayInputStream("Hello World!".getBytes());

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new GoGetter());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(GoGetter.GOG_TEXT, "{\"extract-to-attributes\":{\"test.result\":\"Good!\"},\"extract-to-json\":{\"test.result\":\"Hello World!\",\"test.no\":{\"value\":21020,\"to-type\":\"long\"}}}");
        //runner.setProperty(GoGetter.GOG_TEXT, "{\"extract-to-attributes\":{\"test.result\":\"Good!\"},\"extract-to-json\":{\"test.result\":null,\"test.no\":{\"value\":null,\"to-type\":\"long\"}}}");

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GoGetter.REL_SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        // Test attributes and content.
        result.assertAttributeEquals("test.result", "Good!");
        result.assertContentEquals("{\"test.no\":21020,\"test.result\":\"Hello World!\"}");
    }

}