/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.curator.test.TestingServer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class JWTStoreClientProcessorTest
{
  TestingServer zkServer;

  @Before public void setUp() throws Exception
  {
    zkServer = new TestingServer(52181, true);
  }

  @After public void tearDown() throws Exception
  {
    zkServer.stop();
  }

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\",\"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTStoreClientProcessor());

    // Add properties
    runner.setProperty(JWTStoreClientProcessor.JWT_JSON, "${raw}");
    runner.setProperty(JWTStoreClientProcessor.JWT_ZK_HOST, "localhost:52181");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("raw", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    String rawJwt2 = runner.getProcessContext().getProperty(JWTStoreClientProcessor.JWT_JSON)
        .evaluateAttributeExpressions(ff).getValue();

    assertTrue("ensure rawJWT is still the same", rawJwtStr.equals(rawJwt2));

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTStoreClientProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

  }

}
