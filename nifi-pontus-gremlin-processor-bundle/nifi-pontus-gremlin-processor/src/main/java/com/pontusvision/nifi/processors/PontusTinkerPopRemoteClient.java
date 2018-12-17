/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import javax.script.Bindings;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Leo Martins
 */

@EventDriven @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({ @WritesAttribute(attribute = "reqUUID", description = "UUID from the query"),
    @WritesAttribute(attribute = "pontus.id.type", description = "The type of UUID (NEW, EXISTING)"),
    @WritesAttribute(attribute = "pontus.match.status", description = "The status associated with the record "
        + "(MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH) ") })

@Tags({ "pontus", "TINKERPOP", "GREMLIN" }) @CapabilityDescription(
    "Reads data from attributes, and puts it into a Remote TinkerPop Gremlin Server using a given query.  "
        + "Each of the schema fields is passed as a variable to the tinkerpop query string.  "
        + "As an example, if the server has a graph created as g1, then we can create an alias pointing \"g1\" to \"g\" by "
        + "using the alias option." + "Then, the tinkerpop query looks like this:\n"
        + "'v1 = g.addV(\"person\").property(id, tp_userID1).property(\"name\", tp_userName1).property(\"age\", tp_userAge1).next()\n"
        + "v2 = g.addV(\"software\").property(id, tp_userID2).property(\"name\", tp_userName2).property(\"lang\", tp_userLang2).next()\n"
        + "g.addE(\"created\").from(v1).to(v2).property(id, tp_relId1).property(\"weight\", tp_relWeight1)\n', then the "
        + "variables tp_userID1, tp_userName1, tp_userID2, tp_userName2, tp_userLang2, tp_relId1, tp_relWeight1 would all be taken from the "
        + "flowfile attributes that start with the prefix set in the tinkerpop query parameter prefix (e.g. tp_).")

public class PontusTinkerPopRemoteClient extends PontusTinkerPopClient
{

  final static String TINKERPOP_CLIENT_CONTROLLER_SERVICE_STR = "Tinkerpop Client Controller Service";
  final PropertyDescriptor TINKERPOP_CLIENT_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
      .name(TINKERPOP_CLIENT_CONTROLLER_SERVICE_STR)
      .description("Specifies the controller service to connect to a remote tinkerpop service.").required(false)
      .addValidator(StandardValidators.URI_VALIDATOR)
      .identifiesControllerService(PontusTinkerpopControllerServiceInterface.class).build();

  public static final PropertyDescriptor RETRY_COUNT= new PropertyDescriptor.Builder().name("Retry Count")
      .description("Number of times a failed query will be re-tried.  Zero means no retries.").required(true)
      .defaultValue("0").addValidator(StandardValidators.NUMBER_VALIDATOR).build();

  public static final PropertyDescriptor RETRY_MIN_DELAY_MS= new PropertyDescriptor.Builder().name("Retry Min Delay")
      .description("Minimum delay in ms between retries.  The first retry will sleep for this amount, doubling up to the maximum delay.").required(true)
      .defaultValue("0").addValidator(StandardValidators.NUMBER_VALIDATOR).build();

  public static final PropertyDescriptor RETRY_MAX_DELAY_MS= new PropertyDescriptor.Builder().name("Retry Max Delay")
      .description("Maximum delay in ms between retries.").required(true)
      .defaultValue("10").addValidator(StandardValidators.NUMBER_VALIDATOR).build();

  PontusTinkerpopControllerServiceInterface service = null;

  protected int retryCount = 5;
  protected int retryMinDelayMs = 0;
  protected int retryMaxDelayMs = 10;

  public PontusTinkerPopRemoteClient()
  {
    relationships.add(REL_FAILURE);
    relationships.add(REL_SUCCESS);

  }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(TINKERPOP_CLIENT_CONTROLLER_SERVICE);
    properties.add(TINKERPOP_QUERY_STR);
    properties.add(WAITING_TIME);
    properties.add(TINKERPOP_QUERY_PARAM_PREFIX);
    properties.add(RETRY_COUNT);
    properties.add(RETRY_MIN_DELAY_MS);
    properties.add(RETRY_MAX_DELAY_MS);
    return properties;
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(TINKERPOP_QUERY_STR))
    {
      queryStr = newValue;
    }
    else if (descriptor.equals(TINKERPOP_QUERY_PARAM_PREFIX))
    {
      queryAttribPrefixStr = newValue;
    }
    else if (descriptor.equals(WAITING_TIME))
    {
      timeoutInSecs = Integer.parseInt(newValue);
    }
    else if (descriptor.equals(TINKERPOP_CLIENT_CONTROLLER_SERVICE))
    {
      service = null;
    }
    else if (descriptor.equals(RETRY_COUNT))
    {
      retryCount = Integer.parseInt(newValue);
    }
    else if (descriptor.equals(RETRY_MIN_DELAY_MS))
    {
      retryMinDelayMs = Integer.parseInt(newValue);
    }
    else if (descriptor.equals(RETRY_MAX_DELAY_MS))
    {
      retryMaxDelayMs = Integer.parseInt(newValue);
    }

  }

  @OnStopped public void stopped()
  {
    //    closeClient("stopped");
  }

  @Override
  public byte[] runQuery(Bindings bindings, String queryString) throws FileNotFoundException, URISyntaxException
  {

    Map<String, Object> props = new HashMap<>(bindings);

    ResultSet res = service.getService().getClient().submit(queryString, props);

    return getBytesFromResultSet(res);

  }

  public static long sleep (long sleepTime, long maxTime)
  {
    long nextSleep = Math.min(sleepTime * 2, maxTime);

    try
    {
      Thread.sleep(sleepTime);
    }
    catch (Throwable t)
    {
      // ignore
    }

    return nextSleep;
  }


  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {

    final ComponentLog log = this.getLogger();
    FlowFile localFlowFile = null;
    int counter = 0;

    try
    {
      final FlowFile flowfile = session.get();
      if (flowfile == null)
      {
        //        log.error("Got a NULL flow file");
        return;
      }


      if (service == null){
        service = context.getProperty(TINKERPOP_CLIENT_CONTROLLER_SERVICE_STR).asControllerService(PontusTinkerpopControllerServiceInterface.class);
      }


      final Bindings bindings = getBindings(flowfile,context, session);

      bindings.put("pg_retryCounter", counter);
      Map<String, String> allAttribs = flowfile.getAttributes();
      session.remove(flowfile);

      String queryString = getQueryStr(session);

      localFlowFile = session.create();
      localFlowFile = session.putAllAttributes(localFlowFile, allAttribs);
      byte[] res = null;
      long sleepMs = retryMinDelayMs;
      do
      {
        counter ++;

        try
        {
          res = runQuery(bindings, queryString);
          break;
        }
        catch (Throwable t)
        {
          log.warn("Tinkerpop - retryCounter = {}; Retrying query  {}", new Object[] {counter,queryString} );
          bindings.put("pg_lastErrorStr", t.getMessage());
          bindings.put("pg_retryCounter", counter);

          if (counter > retryCount)
          {
            throw t;
          }

          sleepMs = sleep(sleepMs, retryMaxDelayMs);
        }
      }while ( counter <= retryCount);

      final byte [] finalRes = res;
      localFlowFile = session.write(localFlowFile, out -> out.write(finalRes));

      session.transfer(localFlowFile, REL_SUCCESS);

    }
    catch (Throwable e)
    {
      handleError(e, localFlowFile, session, context);
      log.error("Failed to run query against Tinkerpop; counter = "+ counter + " error: {}", e);
    }

  }
  protected void handleError(Throwable e, FlowFile flowFile, ProcessSession session, ProcessContext context)
  {

    getLogger().error("Failed to process {}; will route to failure", new Object[] { flowFile, e });
    //    session.transfer(flowFile, REL_FAILURE);

//    closeClient("Error");
    if (flowFile != null)
    {
      flowFile = session.putAttribute(flowFile, "PontusTinkerPopClient.error", e.getMessage());
      flowFile = session.putAttribute(flowFile, "PontusTinkerPopClient.error.stacktrace", getStackTrace(e));
      flowFile = session.putAttribute(flowFile, "pg_lastErrorStr", e.getMessage());
      session.transfer(flowFile, REL_FAILURE);
    }
    else
    {
      FlowFile ff = session.create();
      ff = session.putAttribute(ff, "PontusTinkerPopClient.error", e.getMessage());
      ff = session.putAttribute(ff, "PontusTinkerPopClient.error.stacktrace", getStackTrace(e));
      ff = session.putAttribute(ff, "pg_lastErrorStr", e.getMessage());

      session.transfer(ff, REL_FAILURE);
    }
//    Throwable cause = e.getCause();
//    if (cause instanceof RuntimeException)
//    {
//      try
//      {
//        if (cause.getCause() instanceof TimeoutException)
//        {
//          createClient(confFileURI, useEmbeddedServer);
//        }
//        else if (cause.getCause() instanceof RuntimeException)
//        {
//          cause = cause.getCause();
//          if (cause.getCause() instanceof TimeoutException)
//          {
//            createClient(confFileURI, useEmbeddedServer);
//          }
//
//        }
//      }
//      catch (Throwable t)
//      {
//        getLogger().error("Failed to reconnect {}", new Object[] { t });
//
//      }
//    }
  }

}
