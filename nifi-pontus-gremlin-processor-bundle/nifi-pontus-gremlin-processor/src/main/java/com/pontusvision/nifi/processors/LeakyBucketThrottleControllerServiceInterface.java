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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@Tags({ "Pontus", "Leaky Bucket", "Service", "Controller",
    "Janusgraph" }) @CapabilityDescription("Leaky Bucket Notifier Controller Service.") public interface LeakyBucketThrottleControllerServiceInterface
    extends ControllerService
{

  String INITIAL_COUNT_STR = "The initial message count";

  PropertyDescriptor INITIAL_COUNT = new PropertyDescriptor.Builder().name(INITIAL_COUNT_STR)
      .defaultValue("100").required(false).addValidator(StandardValidators.NUMBER_VALIDATOR).build();


  List<PropertyDescriptor> getSupportedPropertyDescriptors();

  /**
   * @param context the configuration context
   * @throws InitializationException if unable to create a database connection
   */
  @OnEnabled  void onEnabled(final ConfigurationContext context) throws InitializationException;
  @OnDisabled void shutdown();

  Lock getLock();

  Condition getCondition();

  int incrementCounter();
  int decrementCounter();

  int resetCounter();

  int getCounter();


}
