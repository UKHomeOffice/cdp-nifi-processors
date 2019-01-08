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

import com.github.scribejava.apis.MicrosoftAzureActiveDirectory20Api;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.oauth.OAuth20Service;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({ "Pontus", "OAuth2", "Service",
    "Openid" }) @CapabilityDescription("OAuth2 Service.") public class PontusOauth20ControllerService
    extends AbstractControllerService implements PontusOauth20ControllerServiceInterface
{

  private static final List<PropertyDescriptor> properties;

  static
  {
    final List<PropertyDescriptor> props = new ArrayList<>();
    props.add(OAUTH2_CLIENT_ID);
    props.add(OAUTH2_CLIENT_SECRET);
    props.add(OAUTH2_CLIENT_API_KEY);

    props.add(OAUTH2_CALLBACK_URL);
    props.add(OAUTH2_CLIENT_API);

    properties = Collections.unmodifiableList(props);
  }

  String clientId = "your client id";
  String clientApiKey = "your key";
  String clientSecret = "your client secret";
  String callbackURLStr = "https://www.example.com/callback";
  String clientApiClassStr = "";
  DefaultApi20 clientApi = MicrosoftAzureActiveDirectory20Api.instance();

  public OAuth20Service oAuth20Service;

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

  /**
   * @param context the configuration context
   * @throws InitializationException if unable to create a database connection
   */
  @OnEnabled public void onEnabled(final ConfigurationContext context) throws InitializationException
  {
    callbackURLStr = context.getProperty(OAUTH2_CALLBACK_URL).getValue();
    clientId = context.getProperty(OAUTH2_CLIENT_ID).getValue();
    clientApiKey = context.getProperty(OAUTH2_CLIENT_API_KEY).getValue();
    clientSecret = context.getProperty(OAUTH2_CLIENT_SECRET).getValue();
    clientApiClassStr = context.getProperty(OAUTH2_CLIENT_API).getValue();

    try
    {
      clientApi = PontusOauth20ControllerServiceInterface.loadAPIfromClassString(clientApiClassStr);
    }
    catch (Throwable t)
    {
      throw new InitializationException(t);
    }

    try
    {
      oAuth20Service = new ServiceBuilder(clientId).apiKey(clientApiKey).apiSecret(clientSecret)
          .callback(callbackURLStr).build(clientApi);

    }
    catch (Throwable t)
    {
      throw new InitializationException(t);
    }

  }

  @OnDisabled public void shutdown() throws IOException
  {
    oAuth20Service.close();

  }

  @Override public OAuth20Service getService()
  {
    return oAuth20Service;
  }

}
