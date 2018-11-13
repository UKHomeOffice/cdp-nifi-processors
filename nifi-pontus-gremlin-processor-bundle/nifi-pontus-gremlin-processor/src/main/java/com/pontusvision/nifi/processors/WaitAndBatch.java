/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

/**
 * @author Leo Martins
 */
@TriggerSerially @Tags({ "Pontus", "Throttle",
    "Sleep", "Wait", "Batch" }) @CapabilityDescription("Very simple processor that sleeps for a configurable amount of time and reads up to a configurable number of records from the queue.  If the queue uses a Priority orderer, this enables some level of ordering")

public class WaitAndBatch extends AbstractProcessor
{

  private List<PropertyDescriptor> properties;

  private Set<Relationship> relationships;

  long waitTimeInSeconds = 120;
  int numMessagesToRead = 1000000;


  public final static String WAIT_TIME_IN_SECONDS_TXT = "Wait Time In Seconds";
  public static PropertyDescriptor WAIT_TIME_IN_SECONDS = new PropertyDescriptor.Builder()
      .name(WAIT_TIME_IN_SECONDS_TXT)
      .description("Specifies the amount of time to sleep in seconds.")
      .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
      .required(true).defaultValue("120").build();

  public final static String NUM_MESSAGES_TO_READ_TXT = "Num messages to read once the processor awakens";

  public static PropertyDescriptor NUM_MESSAGES_TO_READ = new PropertyDescriptor.Builder().name(NUM_MESSAGES_TO_READ_TXT)
      .description(
          "The number of messages to be read once the processor awakens.")
      .required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("1000000")
      .build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();


  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(WAIT_TIME_IN_SECONDS);
    properties.add(NUM_MESSAGES_TO_READ);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    //    relationships.add(WAITING);

    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {

    if (descriptor.equals(WAIT_TIME_IN_SECONDS))
    {
      waitTimeInSeconds = Long.parseLong(newValue);
    }

    if (descriptor.equals(NUM_MESSAGES_TO_READ))
    {
      numMessagesToRead = Integer.parseInt(newValue);
    }
  }

  //  FlowFileFilter filter = flowfile -> {
  //
  //    if (flowfile.getAttribute("increment") != null)
  //    {
  //      return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
  //    }
  //
  //    if (initialCount <= 0)
  //    {
  //      initialCount = 0;
  //      return FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
  //    }
  //
  //    initialCount --;
  //    return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
  //  };

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {

    try
    {
      Thread.sleep(waitTimeInSeconds * 1000);
    }
    catch (InterruptedException e)
    {
       // ignore
    }
    List<FlowFile> superset = new ArrayList<>(numMessagesToRead);
    List<FlowFile> flowfiles;

    do
    {
      flowfiles = session.get(numMessagesToRead);

      if (flowfiles == null)
      {
        return;
      }

      superset.addAll(flowfiles);
      

    }while (!flowfiles.isEmpty() && superset.size() < numMessagesToRead);

//    for (int i = 0, ilen = flowfiles.size(); i< ilen; i++){
//      FlowFile flowfile = flowfiles.get(i);
//
//      if (flowfile == null)
//      {
//        continue;
//      }
//
//
//      session.transfer(flowfile,SUCCESS);
//    }


    session.transfer(superset,SUCCESS);




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
