/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class WaitAndBatchTest
{

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException, InitializationException
  {
    // Content to be mock a json file
    InputStream header = new ByteArrayInputStream("header1,header2\n".getBytes());

    // Generate a test runner to mock a processor in a flow
    WaitAndBatch throttle = new WaitAndBatch();
    TestRunner runner = TestRunners.newTestRunner(throttle);



    // Add properties

    runner.setProperty(WaitAndBatch.NUM_MESSAGES_TO_READ_TXT,"3");
    runner.setProperty(WaitAndBatch.WAIT_TIME_IN_SECONDS_TXT,"5");


    // Add the content to the runner
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);

    long startTime = System.currentTimeMillis();
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(2);

    long deltaTime = System.currentTimeMillis() - startTime;

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(WaitAndBatch.SUCCESS);
    assertEquals("6 flow files, because we are waiting for 10 seconds", 6, headerResults.size());

    assertTrue("took greter or equal to 2 * 5 seconds, or 10000 ms", deltaTime >= 10000);

    headerResults.clear();
    runner.enqueue(header);

    runner.setProperty(WaitAndBatch.NUM_MESSAGES_TO_READ_TXT,"15");
    runner.setProperty(WaitAndBatch.WAIT_TIME_IN_SECONDS_TXT,"10");

    runner.run(1);

    headerResults = runner.getFlowFilesForRelationship(WaitAndBatch.SUCCESS);
    assertEquals("9 flow files", 9, headerResults.size());

    runner.setProperty(WaitAndBatch.NUM_MESSAGES_TO_READ_TXT,"15");
    runner.setProperty(WaitAndBatch.WAIT_TIME_IN_SECONDS_TXT,"1");

    runner.run(1);

    headerResults = runner.getFlowFilesForRelationship(WaitAndBatch.SUCCESS);
    assertEquals("9 flow files, as no more messages were enqueued", 9, headerResults.size());


    //    headerResults = runner.getFlowFilesForRelationship(LeakyBucketThrottle.WAITING);
//
//    assertTrue(
//        "Waiting Queue size is still two because we haven't had any more messages arriving to increment the counter.",
//        headerResults.size() == 2);




  }

}
