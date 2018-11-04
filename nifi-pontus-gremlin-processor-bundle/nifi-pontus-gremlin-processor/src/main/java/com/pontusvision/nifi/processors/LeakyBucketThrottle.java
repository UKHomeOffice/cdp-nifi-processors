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
import java.util.concurrent.TimeUnit;

/**
 * @author Leo Martins
 */
@TriggerSerially @Tags({ "Pontus", "Throttle",
    "Leaky Bucket" }) @CapabilityDescription("Very basic leaky bucket throttling mechanism.")

public class LeakyBucketThrottle extends AbstractProcessor
{

  private List<PropertyDescriptor> properties;

  private Set<Relationship> relationships;

  boolean isNotifier = false;

  LeakyBucketThrottleControllerServiceInterface service = null;

  public final static String LEAKY_BUCKET_CONTROLLER_SERVICE_STR = "Leaky Bucket Controller";
  public static PropertyDescriptor LEAKY_BUCKET_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
      .name(LEAKY_BUCKET_CONTROLLER_SERVICE_STR)
      .description("Specifies the controller service to link a Leaky bucket throttle with a leaky bucket notifier.")
      .required(false).identifiesControllerService(LeakyBucketThrottleControllerServiceInterface.class).build();

  public final static String LEAKY_BUCKET_IS_NOTIFIER_STR = "Leaky Bucket Is Notifier";

  public static PropertyDescriptor LEAKY_BUCKET_IS_NOTIFIER = new PropertyDescriptor.Builder().name(LEAKY_BUCKET_IS_NOTIFIER_STR)
      .description(
          "If true, this leaky bucket will increment a counter whenever a message arrives; if false, it will decrement the counter, and wait until a new item arrives when the counter <= 0.")
      .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();
  public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("Failure relationship; failed to get the lock after 60 seconds").build();

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(LEAKY_BUCKET_IS_NOTIFIER);
    properties.add(LEAKY_BUCKET_CONTROLLER_SERVICE);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    //    relationships.add(WAITING);

    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {

    if (descriptor.equals(LEAKY_BUCKET_IS_NOTIFIER))
    {
      isNotifier = Boolean.parseBoolean(newValue);
    }

    if (descriptor.equals(LEAKY_BUCKET_CONTROLLER_SERVICE))
    {
      service = null;
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
    FlowFile flowfile = session.get();

    if (flowfile == null)
    {
      return;
    }

    if (service == null)
    {
      service = context.getProperty(LEAKY_BUCKET_CONTROLLER_SERVICE)
          .asControllerService(LeakyBucketThrottleControllerServiceInterface.class);
    }

    try
    {
      if (service.getLock().tryLock(60, TimeUnit.SECONDS))
      {

        while (!isNotifier && service.getCounter() <= 0)
        {
          service.getCondition().await(60, TimeUnit.SECONDS);

        }

        if (isNotifier)
        {
          service.incrementCounter();
          if (service.getCounter() == 1)
          {
            service.getCondition().signalAll();
          }
        }
        else
        {
          service.decrementCounter();

        }

        service.getLock().unlock();
        session.transfer(flowfile, SUCCESS);

        return;

      }


    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }


    session.transfer(flowfile, FAILURE);


    //
    //      else
    //      {
    //      initialCount--;
    //      }

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
