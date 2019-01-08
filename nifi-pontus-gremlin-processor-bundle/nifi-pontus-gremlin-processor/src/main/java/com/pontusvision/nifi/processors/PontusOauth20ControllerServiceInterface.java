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

import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.oauth.OAuth20Service;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Tags({ "Pontus", "Tinkerpop", "Service", "GraphDB",
    "Janusgraph" }) @CapabilityDescription("Tinkerpop Service.") public interface PontusOauth20ControllerServiceInterface
    extends ControllerService
{

  public final static PropertyDescriptor OAUTH2_CLIENT_ID = new PropertyDescriptor.Builder().name("OAuth2 client ID")
      .description("specifies the Oauth2 client id").required(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .sensitive(false).build();

  public final static PropertyDescriptor OAUTH2_CLIENT_API_KEY = new PropertyDescriptor.Builder().name("OAuth2 client API Key")
      .description("specifies the Oauth2 client API Key").required(true)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR).sensitive(false).build();

  public final static PropertyDescriptor OAUTH2_CLIENT_SECRET = new PropertyDescriptor.Builder().name("OAuth2 client Secret")
      .description("specifies the Oauth2 client Secret").required(true)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

  public final static PropertyDescriptor OAUTH2_CALLBACK_URL = new PropertyDescriptor.Builder().name("OAuth2 callback URL")
      .description("specifies the Oauth2 callback URL").required(true).addValidator(StandardValidators.URL_VALIDATOR)
      .defaultValue("http://localhost:9999").build();

  public final static PropertyDescriptor OAUTH2_CLIENT_API = new PropertyDescriptor.Builder().name("OAuth2 client API")
      .description("Specifies the client API to optionally use to connect").defaultValue("").required(false)
      .addValidator((subject, input, context) -> {
        try
        {
          loadAPIfromClassString(input);

        return Validator.VALID.validate(subject, input, context);
        }
        catch (Throwable e)
        {
          e.printStackTrace();
          ValidationResult.Builder resBuilder = new ValidationResult.Builder();
          return resBuilder.input(input).valid(false).subject(subject)
              .explanation("Failed to load client API class " + e.getMessage())
              .build();

        }
      }).build();

  public  static DefaultApi20 loadAPIfromClassString(String classString)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    DefaultApi20 retVal = null;
    Class clazz =  Class.forName(classString);
    if (clazz.isAssignableFrom(DefaultApi20.class)){
      Method m = clazz.getMethod("instance",null);
      retVal = (DefaultApi20) m.invoke(null);


    }

    return retVal;
  }


  List<PropertyDescriptor> getSupportedPropertyDescriptors();

  /**
   * @param context the configuration context
   * @throws InitializationException if unable to create a database connection
   */
  @OnEnabled  void onEnabled(final ConfigurationContext context) throws InitializationException;
  @OnDisabled void shutdown() throws IOException;

  OAuth20Service getService();

}
