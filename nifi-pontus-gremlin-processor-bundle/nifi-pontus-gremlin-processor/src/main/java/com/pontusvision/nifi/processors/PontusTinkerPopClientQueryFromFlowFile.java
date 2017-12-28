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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

  //  @OnScheduled public void parseProps(final ProcessContext context) throws IOException
  //  {
  //
  //    final ComponentLog log = this.getLogger();
  //
  //    if (queryStr == null)
  //    {
  //      queryStr = context.getProperty(TINKERPOP_QUERY_STR).getValue();
  //    }
  //    if (aliasStr == null)
  //    {
  //      aliasStr = context.getProperty(TINKERPOP_ALIAS).getValue();
  //    }
  //    if (confFileURI == null)
  //    {
  //      PropertyValue confFileURIProp = context.getProperty(TINKERPOP_CLIENT_CONF_FILE_URI);
  //      confFileURI = confFileURIProp.getValue();
  //
  //      if (StringUtils.isEmpty(confFileURI))
  //      {
  //        try
  //        {
  //          setDefaultConfigs();
  //        }
  //        catch (Exception e2)
  //        {
  //          log.error("Failed set Default URL config", e2);
  //
  //          return;
  //        }
  //
  //      }
  //      else
  //      {
  //        try
  //        {
  //
  //          URI uri = new URI(confFileURI);
  //          DefaultConfigurationBuilder confBuilder = new DefaultConfigurationBuilder(new File(uri));
  //          conf = confBuilder.getConfiguration(true);
  //        }
  //        catch (Exception e)
  //        {
  //          log.warn("Failed to read URL config; using default values", e);
  //
  //          try
  //          {
  //            setDefaultConfigs();
  //          }
  //          catch (Exception e2)
  //          {
  //            log.error("Failed set Default URL config", e2);
  //
  //            return;
  //          }
  //
  //        }
  //      }
  //      if (client != null)
  //      {
  //        client.close();
  //      }
  //      if (cluster != null)
  //      {
  //        cluster.close();
  //      }
  //
  //      cluster = Cluster.open(conf);
  //
  //      Client unaliasedClient = cluster.connect();
  //
  //      client = unaliasedClient; //.alias(aliasStr);
  //
  //    }
  //
  //  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {

    final ComponentLog log = this.getLogger();
    final FlowFile localFlowFile = session.get();
    if (localFlowFile == null)
    {
      log.error("Got a NULL flow file");
      return;

    }

    try
    {
      Map<String, String> allAttribs = localFlowFile.getAttributes();

      Map<String, Object> tinkerpopAttribs = allAttribs.entrySet().stream()
          .filter((entry -> entry.getKey().startsWith(queryAttribPrefixStr)))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      final Map<String, String> attributes = new HashMap<>(allAttribs);
      final StringBuilder strbuild = new StringBuilder("[");

      session.read(localFlowFile, in -> {
        try
        {
          String queryStr = IOUtils.toString(in, Charset.defaultCharset());

          ResultSet res = client.submit(queryStr, tinkerpopAttribs);
          CompletableFuture<List<Result>> resFuture = res.all();

          if (resFuture.isCompletedExceptionally())
          {
            resFuture.exceptionally((Throwable throwable) -> {
              getLogger().error(
                  "Server Error " + throwable.getMessage() + " orig msg: " + res.getOriginalRequestMessage()
                      .toString());
              //                                    session.transfer(tempFlowFile, REL_FAILURE);

              throw new ProcessException(throwable);
            }).join();

          }

          List<Result> results = resFuture.get();

          GraphSONWriter writer = GraphSONWriter.build().create();

          int counter = 0;

          final ByteArrayOutputStream out = new ByteArrayOutputStream();

          for (Result res1 : results)
          {
            if (counter != 0)
            {
              strbuild.append(',');
            }
            writer.writeObject(out, res1.getObject());
            final String json = out.toString();
            strbuild.append(json);
            counter++;
          }

          strbuild.append(']');

          attributes.put("query.res", Integer.toString(counter));

          //          GraphSONUtility

          UUID reqUUID = res.getOriginalRequestMessage().getRequestId();

          attributes.put("reqUUID", reqUUID.toString());
        }
        catch (Exception e)
        {
          log.error("Failed to run query against Tinkerpop server; error: {}", e);
          throw new IOException(e);
        }
      });

      FlowFile retFlowFile = session.create();
      retFlowFile = session.putAllAttributes(retFlowFile, attributes);

      retFlowFile = session.write(retFlowFile, out1 -> out1.write(strbuild.toString().getBytes()));

      session.remove(localFlowFile);
      session.transfer(retFlowFile, REL_SUCCESS);

      return;

    }
    catch (Exception e)
    {
      handleError(e,localFlowFile,session,context);

      if (localFlowFile != null)
      {
        session.transfer(localFlowFile, REL_FAILURE);
      }
      else
      {
        session.transfer(session.create(), REL_FAILURE);
      }
    }

  }

}
