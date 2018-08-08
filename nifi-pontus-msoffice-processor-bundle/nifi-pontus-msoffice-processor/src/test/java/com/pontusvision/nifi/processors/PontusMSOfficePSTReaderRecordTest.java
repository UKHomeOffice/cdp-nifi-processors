/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

/**
 * @author phillip
 */
public class PontusMSOfficePSTReaderRecordTest
{

  private TestRunner testRunner;

  public static final String ATTRIBUTE_INPUT_NAME = "sentence";

  @Before public void init()
  {
    testRunner = TestRunners.newTestRunner(PontusMSOfficePSTReaderRecord.class);
  }

  @Test public void testProcessor()
  {

    //testRunner.setProperty(MY_PROPERTY, "Tim Spann wrote some code to test NLP with Susan Smith and Doug Jones in New York City, NY and in London, UK on Jan 5, 2018.");
    //		testRunner.setProperty(PontusNLPProcessor., "/Volumes/seagate/models");

    //    testRunner.setProperty(TOKENIZER_MODEL_JSON_PROP,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(TOKEN_NAME_FINDER_MODEL_JSON_PROP,TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);
    try
    {
      testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.pst")));
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }

    testRunner.setValidateExpressionUsage(false);
    testRunner.run();
    testRunner.assertValid();
    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PontusMSOfficePSTReaderRecord.PARSED);

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        Map<String, String> attributes = mockFile.getAttributes();

        for (String attribute : attributes.keySet())
        {
          System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
        }

      }
      catch (Throwable e)
      {
        e.printStackTrace();
      }
    }
  }


  @Test public void testProcessorWithFormat()
  {

    testRunner.setProperty(PontusMSOfficePSTReaderRecord.CONTENT_FORMATTER, "{\"text\":\"%s\","
        + "\"features\":{\"entities\":{}}}");

    //    testRunner.setProperty(TOKENIZER_MODEL_JSON_PROP,TOKENIZER_MODEL_JSON_DEFAULT_VAL);
    //    testRunner.setProperty(TOKEN_NAME_FINDER_MODEL_JSON_PROP,TOKEN_NAME_FINDER_MODEL_JSON_DEFAULT_VAL);
    try
    {
      testRunner.enqueue(new FileInputStream(new File("src/test/resources/test.pst")));
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }

    testRunner.setValidateExpressionUsage(false);
    testRunner.run();
    testRunner.assertValid();
    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PontusMSOfficePSTReaderRecord.PARSED);

    for (MockFlowFile mockFile : successFiles)
    {
      try
      {
        Map<String, String> attributes = mockFile.getAttributes();

        for (String attribute : attributes.keySet())
        {
          System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
        }


      }
      catch (Throwable e)
      {
        e.printStackTrace();
      }
    }
  }

}
