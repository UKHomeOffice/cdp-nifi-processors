/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class JsonProcessorTest
{

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException
  {
    // Content to be mock a json file
    InputStream header = new ByteArrayInputStream("header1,header2\n".getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new CSVToJSONProcessor());

    // Add properties
    runner.setProperty(CSVToJSONProcessor.CSV_SEPARATOR, ",");
    runner.setProperty(CSVToJSONProcessor.CSV_HEADERS, "header1,header2");
    runner.setProperty(CSVToJSONProcessor.CSV_USE_FIRST_LINE_AS_HEADERS, "true");

    // Add the content to the runner
    runner.enqueue(header);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);
    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(CSVToJSONProcessor.NEED_MORE);
    assertTrue("1 match", headerResults.size() == 1);

    InputStream val = new ByteArrayInputStream("val1,val2\n".getBytes());
    runner.enqueue(val);

    runner.run(1);

    // All results were processed with out failure
    runner.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner.getFlowFilesForRelationship(CSVToJSONProcessor.SUCCESS);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = new String(runner.getContentAsByteArray(result));
    System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));

    // Test attributes and content
    //        result.assertAttributeEquals(JsonProcessor.MATCH_ATTR, "nifi rocks");
    result.assertContentEquals("[{'header1':'val1','header2':'val2'}]");

  }

}
