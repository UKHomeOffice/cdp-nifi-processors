/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

//import com.google.common.base.CharMatcher;
//import org.apache.nifi.hbase.HBaseClientService;
//import org.apache.nifi.hbase.put.PutColumn;
//import org.apache.nifi.hbase.scan.Column;
//import org.apache.nifi.hbase.scan.ResultCell;
//import org.apache.nifi.hbase.scan.ResultHandler;
//import org.apache.tinkerpop.gremlin.driver.Settings;
//import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
//import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * @author Leo Martins
 */

@TriggerSerially @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@Tags({ "pontus", "TINKERPOP",
    "GREMLIN" }) @CapabilityDescription("Reads a query from a flowfile body and executes it against a TinkerPop Gremlin Server.")

public class PontusTinkerPopClientQueryFromFlowFile extends PontusTinkerPopClient
{

  public PontusTinkerPopClientQueryFromFlowFile()
  {
    super();

  }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    final List<PropertyDescriptor> properties = new ArrayList<>();

    properties.add(PONTUS_GRAPH_EMBEDDED_SERVER);
    properties.add(TINKERPOP_CLIENT_CONF_FILE_URI);
    properties.add(TINKERPOP_QUERY_PARAM_PREFIX);

    return properties;
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(TINKERPOP_CLIENT_CONF_FILE_URI))
    {
      confFileURI = null;
    }

  }



  @Override public String getQueryStr(ProcessSession session)
  {
    final FlowFile localFlowFile = session.get();
    final StringBuilder sb = new StringBuilder();

    session.read(localFlowFile, in -> {
      try
      {
        String queryStr = IOUtils.toString(in, Charset.defaultCharset());
        sb.append(queryStr);
      }
      catch (Exception e)
      {
        getLogger().error("Failed to run query against Tinkerpop server; error: {}", e);
        throw new IOException(e);
      }
    });

    return sb.toString();
  }


}
