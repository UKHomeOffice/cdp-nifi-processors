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

@Tags({ "Pontus", "Tinkerpop", "Service", "GraphDB",
    "Janusgraph" }) @CapabilityDescription("Tinkerpop Service.") public interface PontusTinkerpopControllerServiceInterface
    extends ControllerService
{

  final static PropertyDescriptor TINKERPOP_CLIENT_CONF_FILE_URI = new PropertyDescriptor.Builder()
      .name("Tinkerpop Client configuration URI").description(
          "Specifies the configuration file to configure this connection to tinkerpop (if embedded, this is the gremlin-server.yml file).")
      .required(false).addValidator(StandardValidators.URI_VALIDATOR)
      .build();


  List<PropertyDescriptor> getSupportedPropertyDescriptors();

  /**
   * @param context the configuration context
   * @throws InitializationException if unable to create a database connection
   */
  @OnEnabled  void onEnabled(final ConfigurationContext context) throws InitializationException;
  @OnDisabled void shutdown();

  ClusterClientService getService();

}
