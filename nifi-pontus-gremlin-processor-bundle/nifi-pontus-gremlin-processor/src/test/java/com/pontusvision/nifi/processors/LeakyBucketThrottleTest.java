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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class LeakyBucketThrottleTest
{

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException, InitializationException
  {
    // Content to be mock a json file
    InputStream header = new ByteArrayInputStream("header1,header2\n".getBytes());

    // Generate a test runner to mock a processor in a flow
    LeakyBucketThrottle throttle = new LeakyBucketThrottle();
    TestRunner runner = TestRunners.newTestRunner(throttle);
    TestRunner runner2 = TestRunners.newTestRunner(throttle);

//    runner.setIncomingConnection(true);
//    runner.addConnection(LeakyBucketThrottle.WAITING);


    LeakyBucketThrottleControllerServiceInterface service = new LeakyBucketThrottleControllerService();

    Map<String, String> controllerSvcProps = new HashMap<>();
    controllerSvcProps.put(LeakyBucketThrottleControllerServiceInterface.INITIAL_COUNT_STR,"5");


    runner.addControllerService("LeakyBucketControllerSvc", service, controllerSvcProps);
    runner2.addControllerService("LeakyBucketControllerSvc", service, controllerSvcProps);
    // Add properties
    runner.setProperty(LeakyBucketThrottle.LEAKY_BUCKET_CONTROLLER_SERVICE_STR, "LeakyBucketControllerSvc" );
    runner2.setProperty(LeakyBucketThrottle.LEAKY_BUCKET_CONTROLLER_SERVICE_STR, "LeakyBucketControllerSvc" );

    runner.setProperty(LeakyBucketThrottle.LEAKY_BUCKET_IS_NOTIFIER_STR,"false");
    runner2.setProperty(LeakyBucketThrottle.LEAKY_BUCKET_IS_NOTIFIER_STR,"true");

    Map<String, String> attribs = new HashMap<>();
    attribs.put("incremenent", "1");
    // Add the content to the runner
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner2.enqueue(header, attribs);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);
    runner.enqueue(header);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(3);
    runner2.run(1);
    runner.run(2);
    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(LeakyBucketThrottle.SUCCESS);
    assertTrue("4 flow files, because one was swallowed up", headerResults.size() == 4);
    assertTrue(
        "Count is two because we added one increment message, which did not contribute to the down count, and added one extra.",
        throttle.service.getCounter() == 2);

    runner.run(2);


    assertTrue(
        "Count is zero because we ran 2 more messages.",
        throttle.service.getCounter() == 0);


    assertTrue(
        "Queue size is two.",
        runner.getQueueSize().getObjectCount() == 2);

    runner.run(4,false, false,5000);


//    headerResults = runner.getFlowFilesForRelationship(LeakyBucketThrottle.WAITING);
//
//    assertTrue(
//        "Waiting Queue size is still two because we haven't had any more messages arriving to increment the counter.",
//        headerResults.size() == 2);




  }

}
