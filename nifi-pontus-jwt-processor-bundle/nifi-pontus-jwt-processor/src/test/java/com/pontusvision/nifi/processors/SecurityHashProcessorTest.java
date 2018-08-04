/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class SecurityHashProcessorTest
{

  SecurityHashStandardSSLContextService sslContextService = new MockSSLContextService("sslContext");

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\",\"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new SecurityHashProcessor());

    try
    {
      runner.addControllerService("sslContext", sslContextService);
      runner.enableControllerService(sslContextService);
    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service", false);
    }
    // Add properties
    runner.setProperty(SecurityHashProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(SecurityHashProcessor.HASH_KEY_ALGO, "HmacSHA256");
    runner.setProperty(SecurityHashProcessor.HASH_KEY_ALIAS, "jwthashkey");
    runner.setProperty(SecurityHashProcessor.HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME, "out");
    runner.setProperty(SecurityHashProcessor.HASH_RAW_STRING, "${raw}");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("raw", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    runner.getProcessContext().getProperty(SecurityHashProcessor.HASH_RAW_STRING).evaluateAttributeExpressions(ff);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

    String out = headerResults.get(0).getAttribute("out");

    assertTrue(out != null);
    assertTrue("NsZAzuIE8c/FMLXCa+oW6ha4OhiOGFJZ8lXdI7Gaa9M=".equals(out));

  }

  class MockSSLContextService extends SecurityHashStandardSSLContextService
  {
    String description;

    public MockSSLContextService(String description)
    {
      this.description = description;
    }

    @Override public String getTrustStoreFile()
    {
      return "security/secret.client.truststore.jks";
    }

    @Override public String getTrustStoreType()
    {
      return "jceks";
    }

    @Override public String getTrustStorePassword()
    {
      return "pa55word";
    }

    @Override public boolean isTrustStoreConfigured()
    {
      return true;
    }

    @Override public String getKeyStoreFile()
    {
      return "security/secret.client.keystore.jks";
    }

    @Override public String getKeyStoreType()
    {
      return "jceks";
    }

    @Override public String getKeyStorePassword()
    {
      return "pa55word";
    }

    @Override public String getKeyPassword()
    {
      return "pa55word";
    }

    @Override public boolean isKeyStoreConfigured()
    {
      return true;
    }

    @Override public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue)
    {

    }

    @Override public String getIdentifier()
    {
      return description;
    }
  }

}
