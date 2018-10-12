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
@TriggerSerially @Tags({ "Pontus","Throttle", "Leaky Bucket" }) @CapabilityDescription("Very basic leaky bucket throttling mechanism.")

public class LeakyBucketThrottle extends AbstractProcessor
{

  private List<PropertyDescriptor> properties;

  private Set<Relationship> relationships;

  long initialCount = 100L;


  final static PropertyDescriptor INITIAL_COUNT = new PropertyDescriptor.Builder()
      .name("The initial message count)").defaultValue("100").required(false)
      .addValidator(StandardValidators.NUMBER_VALIDATOR).build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();
  public static final Relationship WAITING = new Relationship.Builder().name("WAITING")
      .description("Waiting relationship; usually point this to yourself").build();


  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(INITIAL_COUNT);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    relationships.add(WAITING);

    this.relationships = Collections.unmodifiableSet(relationships);
  }


  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {

    if (descriptor.equals(INITIAL_COUNT))
    {
      initialCount = Long.parseLong(newValue);
    }



  }

  FlowFileFilter filter = flowfile -> {

    if (flowfile.getAttribute("incremenent") != null){
      return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
    }

    if (initialCount <= 0){
      initialCount = 0;
      return FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
    }

    return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
  };

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final List<FlowFile> flowfiles = session.get(filter);

    int numflowFiles = flowfiles.size();

    for (int i = 0; i < numflowFiles;i++)
    {
      FlowFile flowfile = flowfiles.get(i);

      if (flowfile == null)
      {
        continue;
      }

      if (flowfile.getAttribute("incremenent") != null){
        initialCount ++;
        continue;
      }

      if (initialCount <= 0)
      {
        initialCount = 0;
        session.transfer(flowfile, WAITING);
        continue;
      }

      else
      {
        initialCount--;
        session.transfer(flowfile, SUCCESS);
      }

    }


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
