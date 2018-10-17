/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Tags({ "Pontus", "Leaky Bucket", "Service", "Controller",
    "Janusgraph" }) @CapabilityDescription("Leaky Bucket Notifier Controller Service.")
public class LeakyBucketThrottleControllerService extends AbstractControllerService
    implements LeakyBucketThrottleControllerServiceInterface

{


  Lock lock = null;
  Condition  cond = null;

  int counter = 100;
  int initCounter = 100;


  @Override public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue)
  {
    if (descriptor.equals(INITIAL_COUNT)){
      initCounter = Integer.parseInt(newValue);
      counter = initCounter;
    }

  }


  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return Collections.singletonList(INITIAL_COUNT);
  }

  @Override public void onEnabled(ConfigurationContext context) throws InitializationException
  {
    lock = new ReentrantLock();
    cond = lock.newCondition();

    counter = initCounter;

  }

  @Override public void shutdown()
  {
//    if (lock.tryLock())
//    {
//      counter = initCounter;
//      cond.signalAll();
//      lock.unlock();
//    }

  }

  @Override public Lock getLock()
  {
    return lock;
  }

  @Override public Condition getCondition()
  {
    return cond;
  }

  @Override public int incrementCounter()
  {
    return counter ++;

  }

  @Override public int decrementCounter()
  {
    return counter --;
  }

  @Override public int resetCounter()
  {
    counter = initCounter;
    return counter;
  }

  @Override public int getCounter()
  {
    return counter;
  }

}
