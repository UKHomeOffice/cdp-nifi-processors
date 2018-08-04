/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.nimbusds.jose.util.JSONObjectUtils;
import net.minidev.json.JSONObject;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * @author phillip
 */
public class JWTProcessorsTest
{
  SSLContextService sslContextService = new MockSSLContextService("sslContext");

  ;
  SSLContextService sslContextService2 = new MockSSLContextService("sslContext2");

  /**
   * Test of onTrigger method, of class JsonProcessor.
   */
  @org.junit.Test public void testOnTrigger() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\",\"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file
    InputStream fil = new ByteArrayInputStream(rawJwtStr.getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTCreatorProcessor());

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
    runner.setProperty(JWTCreatorProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALGO, "RS512");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner.setProperty(JWTCreatorProcessor.JWT_ATTRIBUTE_NAME, "");

    // Add the content t
    //
    // o the runner
    runner.enqueue(fil);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

    InputStream fil2 = new ByteArrayInputStream(headerResults.get(0).toByteArray());
    //    InputStream fil2 = new ByteArrayInputStream(rawJwtStr.getBytes());

    TestRunner runner2 = TestRunners.newTestRunner(new JWTDecoderProcessor());
    try
    {
      runner2.addControllerService("sslContext2", sslContextService2);
      runner2.enableControllerService(sslContextService2);

    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service to runner 2", false);
    }
    // Add properties
    runner2.setProperty(JWTDecoderProcessor.SSL_CONTEXT, "sslContext2");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALGO, "RS512");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner2.setProperty(JWTDecoderProcessor.JWT_ATTRIBUTE_NAME, "");

    // Add the content to the runner
    runner2.enqueue(fil2);

    runner2.run(1);

    // All results were processed with out failure
    runner2.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner2.getFlowFilesForRelationship(JWTDecoderProcessor.SUCCESS);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = new String(runner2.getContentAsByteArray(result));
    result.assertContentEquals(rawJwtStr);

  }

  @org.junit.Test public void testOnTriggerWithAttributes() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\",\"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file
    InputStream fil = new ByteArrayInputStream(rawJwtStr.getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTCreatorProcessor());

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
    runner.setProperty(JWTCreatorProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALGO, "RS512");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner.setProperty(JWTCreatorProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("jwt", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

    MockFlowFile ff2 = new MockFlowFile(1231313L);
    Map<String, String> attribsMap2 = new HashMap<>(ff2.getAttributes());
    attribsMap2.put("jwt", headerResults.get(0).getAttribute("jwt"));

    ff2.putAttributes(attribsMap2);

    //    InputStream fil2 = new ByteArrayInputStream(rawJwtStr.getBytes());

    TestRunner runner2 = TestRunners.newTestRunner(new JWTDecoderProcessor());
    try
    {
      runner2.addControllerService("sslContext2", sslContextService2);
      runner2.enableControllerService(sslContextService2);

    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service to runner 2", false);
    }
    // Add properties
    runner2.setProperty(JWTDecoderProcessor.SSL_CONTEXT, "sslContext2");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALGO, "RS512");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner2.setProperty(JWTDecoderProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    // Add the content to the runner
    runner2.enqueue(ff2);

    runner2.run(1);

    // All results were processed with out failure
    runner2.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner2.getFlowFilesForRelationship(JWTDecoderProcessor.SUCCESS);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = result.getAttribute("jwt");
    assertTrue("Values Match", rawJwtStr.equals(resultValue));

  }

  @org.junit.Test public void testOnTriggerWithTimeoutProp() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\",\"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file
    InputStream fil = new ByteArrayInputStream(rawJwtStr.getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTCreatorProcessor());

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
    runner.setProperty(JWTCreatorProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALGO, "RS512");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner.setProperty(JWTCreatorProcessor.JWT_ATTRIBUTE_NAME, "jwt");
    runner.setProperty(JWTCreatorProcessor.JWT_TIME_TO_LIVE_MS, "1000");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("jwt", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

    MockFlowFile ff2 = new MockFlowFile(1231313L);
    Map<String, String> attribsMap2 = new HashMap<>(ff2.getAttributes());
    attribsMap2.put("jwt", headerResults.get(0).getAttribute("jwt"));

    ff2.putAttributes(attribsMap2);

    //    InputStream fil2 = new ByteArrayInputStream(rawJwtStr.getBytes());

    TestRunner runner2 = TestRunners.newTestRunner(new JWTDecoderProcessor());
    try
    {
      runner2.addControllerService("sslContext2", sslContextService2);
      runner2.enableControllerService(sslContextService2);

    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service to runner 2", false);
    }
    // Add properties
    runner2.setProperty(JWTDecoderProcessor.SSL_CONTEXT, "sslContext2");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALGO, "RS512");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner2.setProperty(JWTDecoderProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    // Add the content to the runner
    runner2.enqueue(ff2);

    runner2.run(1);

    // All results were processed with out failure
    runner2.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner2.getFlowFilesForRelationship(JWTDecoderProcessor.SUCCESS);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = result.getAttribute("jwt");
    assertTrue("Values Don't Match", !rawJwtStr.equals(resultValue));

    try
    {
      JSONObject resultValObj = JSONObjectUtils.parse(resultValue);
      JSONObject rawJwtObj = JSONObjectUtils.parse(rawJwtStr);

      assertTrue("sub values match", resultValObj.get("sub").equals(rawJwtObj.get("sub")));
      assertTrue("iss values match", resultValObj.get("iss").equals(rawJwtObj.get("iss")));
      assertTrue("bizctx values match", resultValObj.get("bizctx").equals(rawJwtObj.get("bizctx")));

      assertTrue("expiration only in results", resultValObj.containsKey("exp") && !rawJwtObj.containsKey("exp"));

    }
    catch (ParseException e)
    {
      e.printStackTrace();
    }

  }

  @org.junit.Test public void testOnTriggerWithTimeoutPropAndOverridingPayload() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\", \"exp\":100,  \"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file
    InputStream fil = new ByteArrayInputStream(rawJwtStr.getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTCreatorProcessor());

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
    runner.setProperty(JWTCreatorProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALGO, "RS512");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner.setProperty(JWTCreatorProcessor.JWT_ATTRIBUTE_NAME, "jwt");
    runner.setProperty(JWTCreatorProcessor.JWT_TIME_TO_LIVE_MS, "1000");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("jwt", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.SUCCESS);

    assertTrue("1 match", headerResults.size() == 1);

    MockFlowFile ff2 = new MockFlowFile(1231313L);
    Map<String, String> attribsMap2 = new HashMap<>(ff2.getAttributes());
    attribsMap2.put("jwt", headerResults.get(0).getAttribute("jwt"));

    ff2.putAttributes(attribsMap2);

    //    InputStream fil2 = new ByteArrayInputStream(rawJwtStr.getBytes());

    TestRunner runner2 = TestRunners.newTestRunner(new JWTDecoderProcessor());
    try
    {
      runner2.addControllerService("sslContext2", sslContextService2);
      runner2.enableControllerService(sslContextService2);

    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service to runner 2", false);
    }
    // Add properties
    runner2.setProperty(JWTDecoderProcessor.SSL_CONTEXT, "sslContext2");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALGO, "RS512");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner2.setProperty(JWTDecoderProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    // Add the content to the runner
    runner2.enqueue(ff2);

    runner2.run(1);

    // All results were processed with out failure
    runner2.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner2.getFlowFilesForRelationship(JWTDecoderProcessor.SUCCESS);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = result.getAttribute("jwt");
    assertTrue("Values Match", !rawJwtStr.equals(resultValue));

    try
    {
      JSONObject resultValObj = JSONObjectUtils.parse(resultValue);
      JSONObject rawJwtObj = JSONObjectUtils.parse(rawJwtStr);

      assertTrue("sub values match", resultValObj.get("sub").equals(rawJwtObj.get("sub")));
      assertTrue("iss values match", resultValObj.get("iss").equals(rawJwtObj.get("iss")));
      assertTrue("bizctx values match", resultValObj.get("bizctx").equals(rawJwtObj.get("bizctx")));

      assertTrue("expiration in both results", resultValObj.containsKey("exp") && rawJwtObj.containsKey("exp"));

    }
    catch (ParseException e)
    {
      e.printStackTrace();
    }

  }

  @org.junit.Test public void testOnTriggerWithInvalidExpPayload() throws IOException
  {

    String rawJwtStr = "{\"sub\":\"bob\",\"iss\":\"Pontus\", \"exp\":\"100\",  \"bizctx\":\"/blah/blah/blah\"}";
    // Content to be mock a jwtRequest file
    InputStream fil = new ByteArrayInputStream(rawJwtStr.getBytes());

    // Generate a test runner to mock a processor in a flow
    TestRunner runner = TestRunners.newTestRunner(new JWTCreatorProcessor());

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
    runner.setProperty(JWTCreatorProcessor.SSL_CONTEXT, "sslContext");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALGO, "RS512");
    runner.setProperty(JWTCreatorProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner.setProperty(JWTCreatorProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    MockFlowFile ff = new MockFlowFile(1231312L);
    Map<String, String> attribsMap = new HashMap<>(ff.getAttributes());
    attribsMap.put("jwt", rawJwtStr);

    ff.putAttributes(attribsMap);

    // Add the content t
    //
    // o the runner
    runner.enqueue(ff);

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1);

    List<MockFlowFile> headerResults = runner.getFlowFilesForRelationship(JWTCreatorProcessor.FAILURE);

    assertTrue("1 match", headerResults.size() == 1);

    MockFlowFile ff2 = new MockFlowFile(1231313L);
    Map<String, String> attribsMap2 = new HashMap<>(ff2.getAttributes());
    attribsMap2.put("jwt", headerResults.get(0).getAttribute("jwt"));

    ff2.putAttributes(attribsMap2);

    //    InputStream fil2 = new ByteArrayInputStream(rawJwtStr.getBytes());

    TestRunner runner2 = TestRunners.newTestRunner(new JWTDecoderProcessor());
    try
    {
      runner2.addControllerService("sslContext2", sslContextService2);
      runner2.enableControllerService(sslContextService2);

    }
    catch (InitializationException e)
    {
      e.printStackTrace();
      assertTrue("added controller service to runner 2", false);
    }
    // Add properties
    runner2.setProperty(JWTDecoderProcessor.SSL_CONTEXT, "sslContext2");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALGO, "RS512");
    runner2.setProperty(JWTDecoderProcessor.JWT_KEY_ALIAS, "jwtkey");
    runner2.setProperty(JWTDecoderProcessor.JWT_ATTRIBUTE_NAME, "jwt");

    // Add the content to the runner
    runner2.enqueue(ff2);

    try
    {
      runner2.run(1);
    }
    catch (Exception e)
    {
      assertTrue("invalid entry", true);
    }

    // All results were processed with out failure
    runner2.assertQueueEmpty();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = runner2.getFlowFilesForRelationship(JWTDecoderProcessor.FAILURE);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);
    String resultValue = result.getAttribute("jwt");
    assertTrue("Values Match", rawJwtStr.equals(resultValue));

  }

  class MockSSLContextService implements SSLContextService
  {
    String description;

    public MockSSLContextService(String description)
    {
      this.description = description;
    }

    @Override public SSLContext createSSLContext(ClientAuth clientAuth) throws ProcessException
    {
      return null;
    }

    @Override public String getTrustStoreFile()
    {
      return "security/kafka.client.truststore.jks";
    }

    @Override public String getTrustStoreType()
    {
      return "JKS";
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
      return "security/kafka.client.keystore.jks";
    }

    @Override public String getKeyStoreType()
    {
      return "JKS";
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

    @Override public String getSslAlgorithm()
    {
      return "TLS";
    }

    @Override public void initialize(ControllerServiceInitializationContext context) throws InitializationException
    {

    }

    @Override public Collection<ValidationResult> validate(ValidationContext context)
    {
      ValidationResult.Builder builder = new ValidationResult.Builder();

      boolean isValid = true;

      String message = "Incompatible values for TLS Key, alias, and sign algorithm:";

      builder.valid(isValid);
      builder.input("input");
      builder.subject("subject");

      ValidationResult res = builder.build();

      ArrayList<ValidationResult> retVal = new ArrayList<>();
      retVal.add(res);

      return retVal;

    }

    @Override public PropertyDescriptor getPropertyDescriptor(String name)
    {
      PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
      builder.name(name);

      return builder.build();
    }

    @Override public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue)
    {

    }

    @Override public List<PropertyDescriptor> getPropertyDescriptors()
    {
      ArrayList<PropertyDescriptor> retVal = new ArrayList<>();
      return retVal;

    }

    @Override public String getIdentifier()
    {
      return description;
    }
  }

}
