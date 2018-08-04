/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Leo Martins
 */
@Tags({ "JSON", "CSV" }) @CapabilityDescription("Convert CSV to JSON.") public class CSVToJSONProcessor
    extends AbstractProcessor
{

  private List<PropertyDescriptor> properties;
  private Set<Relationship> relationships;
  private int numBatchedEntries = -1;
  private String[] csvHeaders = null;

  private String separatorChar = null;

  Pattern safeCSVSplitPattern = null;

  public static final String MATCH_ATTR = "match";

  final static PropertyDescriptor CSV_USE_FIRST_LINE_AS_HEADERS = new PropertyDescriptor.Builder()
      .name("Use first line as headers").defaultValue("true").required(true)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

  final static PropertyDescriptor CSV_MAX_NUM_BATCHED_ENTRIES = new PropertyDescriptor.Builder()
      .name("The number of batched entries").defaultValue("1").required(true)
      .addValidator(StandardValidators.INTEGER_VALIDATOR).build();

  final static PropertyDescriptor CSV_HEADERS = new PropertyDescriptor.Builder().name("CSV Headers").required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  final static PropertyDescriptor CSV_SEPARATOR = new PropertyDescriptor.Builder().name("CSV Separator")
      .defaultValue(",").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();

  public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("Failure relationship").build();
  public static final Relationship NEED_MORE = new Relationship.Builder().name("NEED_MORE").description(
      "Need More relationship, used if the header is being read and we need more data to process the next record")
      .build();

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(CSV_SEPARATOR);
    properties.add(CSV_USE_FIRST_LINE_AS_HEADERS);
    properties.add(CSV_HEADERS);
    properties.add(CSV_MAX_NUM_BATCHED_ENTRIES);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    relationships.add(NEED_MORE);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  public static void writeDataToFile(FlowFile flowfile, ProcessSession session, StringBuilder sb, int numBatchedEntries)
  {
    if (numBatchedEntries > 1)
    {
      sb.append("]");
    }
    flowfile = session.write(flowfile, new OutputStreamCallback()
    {

      @Override public void process(OutputStream out) throws IOException
      {
        out.write(sb.toString().getBytes());
      }
    });

    session.transfer(flowfile, SUCCESS);

    if (numBatchedEntries > 1)
    {
      sb.setLength(0);
      sb.append("[");
    }

  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    FlowFile flowfile = session.get();

    if (numBatchedEntries == -1)
    {
      PropertyValue numBatchedEntriesPropVal = context.getProperty(CSV_MAX_NUM_BATCHED_ENTRIES);
      numBatchedEntries = Integer.parseInt(numBatchedEntriesPropVal.getValue());
    }

    if (separatorChar == null)
    {
      PropertyValue separatorCharPropVal = context.getProperty(CSV_SEPARATOR);
      separatorChar = Pattern.quote(separatorCharPropVal.getValue());
      safeCSVSplitPattern = Pattern
          .compile(String.format("%s(?=([^\"]*\"[^\"]*\")*[^\"]*$)", separatorChar, separatorChar));
    }
    if (csvHeaders == null)
    {
      PropertyValue csvHeadersPropVal = context.getProperty(CSV_HEADERS);
      PropertyValue csvHeadersUseFirstLineAsVal = context.getProperty(CSV_USE_FIRST_LINE_AS_HEADERS);

      if (!csvHeadersUseFirstLineAsVal.asBoolean() && !csvHeadersPropVal.isSet())
      {
        flowfile = session.putAttribute(flowfile, "error",
            "Must either set the option 'Use first line as headers' to true or set the option 'CSV Headers' to a comma-separated string of values.");
        session.transfer(flowfile, FAILURE);
        return;
      }
      else if (!csvHeadersUseFirstLineAsVal.asBoolean() && csvHeadersPropVal.isSet())
      {
        csvHeaders = safeCSVSplitPattern.split(csvHeadersPropVal.getValue());
      }
    }

    session.read(flowfile, new InputStreamCallback()
    {
      @Override public void process(InputStream in) throws IOException
      {
        try
        {

          LineIterator li = IOUtils.lineIterator(in, Charset.defaultCharset().toString());

          if (csvHeaders == null)
          {
            FlowFile flowfile = session.create();

            if (li.hasNext())
            {
              String firstLine = li.nextLine();
              csvHeaders = safeCSVSplitPattern.split(firstLine);
            }
            if (!li.hasNext())
            {
              flowfile = session.write(flowfile, new OutputStreamCallback()
              {

                @Override public void process(OutputStream out) throws IOException
                {
                  out.write(csvHeaders.toString().getBytes());
                }
              });

              session.transfer(flowfile, NEED_MORE);
              return;

            }
          }
          int counter = 0;
          final StringBuilder sb = new StringBuilder();

          FlowFile flowfile = null;
          boolean alreadySent = false;
          if (numBatchedEntries > 1)
          {
            sb.append("[");
          }

          while (li.hasNext())
          {
            alreadySent = false;
            if (counter % numBatchedEntries == 0)
            {
              flowfile = session.create();
              //              sb.setLength(0);
            }
            else
            {
              sb.append(",");

            }

            String line = li.nextLine();
            String[] csvSplit = safeCSVSplitPattern.split(line);

            if (csvSplit.length != csvHeaders.length)
            {
              flowfile = session
                  .putAttribute(flowfile, "error", "Number of columns doesn't match the number of items in the row.");
              flowfile = session.putAttribute(flowfile, "headers", csvHeaders.toString());
              flowfile = session.putAttribute(flowfile, "line", line);
              session.transfer(flowfile, FAILURE);
              return;
            }

            counter++;

            sb.append("{\"").append(csvHeaders[0]).append("\":\"").append(csvSplit[0]).append("\"");
            for (int i = 1, ilen = csvSplit.length; i < ilen; i++)
            {
              sb.append(",\"").append(csvHeaders[i]).append("\":\"").append(csvSplit[i]).append("\"");
            }
            sb.append('}');

            if (counter % numBatchedEntries == 0)
            {
              writeDataToFile(flowfile, session, sb, numBatchedEntries);
              alreadySent = true;
            }

            //            if (counter >= numBatchedEntries){
            //              break;
            //            }
          }
          if (!alreadySent)
          {
            writeDataToFile(flowfile, session, sb, numBatchedEntries);
          }

        }
        catch (Exception ex)
        {
          ex.printStackTrace();
          log.error("Failed to read json string.");
        }
      }
    });

    session.remove(flowfile);
    session.commit();

  }

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

}
