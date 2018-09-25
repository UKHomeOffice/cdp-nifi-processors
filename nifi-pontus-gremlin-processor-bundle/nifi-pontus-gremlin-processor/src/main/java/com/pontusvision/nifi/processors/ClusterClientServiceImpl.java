package com.pontusvision.nifi.processors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class ClusterClientServiceImpl implements ClusterClientService
{

  public static final String SEPARATOR = "=";
  public static final String GUIDS = "guids";

  private static final Logger logger = LoggerFactory.getLogger(ClusterClientServiceImpl.class);
  private Cluster cluster;
  private Client client;
  private int clientTimeoutInSeconds;
  private ExecuteQuery executeQuery;

  public ClusterClientServiceImpl(String clientYaml, int clientTimeoutInSeconds)
  {
    try
    {
      this.clientTimeoutInSeconds = clientTimeoutInSeconds;
      this.executeQuery = new ExecuteQuery();

      cluster = Cluster.build(new File(new URI(clientYaml))).create();

    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override public boolean isClosed()
  {
    return this.cluster.isClosed() || this.cluster.isClosing();
  }

  @Override public Map<String, String> getVids(List guidS, String binding, String gremlinQuery)
  {

    Map<String, Object> bindings = new HashMap();
    bindings.put(GUIDS, guidS);
    bindings.put(binding, guidS);

    Map<String, String> result = executeQuery.invoke(client, gremlinQuery, bindings, clientTimeoutInSeconds);

    return result;

  }

  @Override public Cluster getCluster()
  {
    return this.cluster;
  }

  @Override public Client getClient()
  {
    this.client = this.cluster.connect();
    this.client.init();

    return this.client;
  }

  private static void handleUnknownException(Throwable throwable)
  {
    try
    {
      if (throwable != null)
      {
        if (!isResponseException(ExceptionUtils.getRootCause(throwable)))
        {
          new RuntimeException("IDLookup :[ClusterClientServiceImpl]:  VID Lookup failed. ", throwable);
        }
      }
    }
    catch (Throwable t)
    {
      logger.warn("Error handleUnknownException : " + t.getMessage());
    }
  }

  private static boolean isResponseException(Throwable inner)
  {
    return inner.getClass().isAssignableFrom(org.apache.tinkerpop.gremlin.driver.exception.ResponseException.class);
  }

  @Override public void close(String event)
  {
    if (this.client != null)
    {
      client.closeAsync();
      logger.info("IDLookup :[ClusterClientServiceImpl]: Cluster client is closed. [event=" + event + "]");
      client = null;
    }
    if (this.cluster != null)
    {
      cluster.closeAsync();
      logger.info("IDLookup :[ClusterClientServiceImpl]: and Cluster is closed as well.");
      cluster = null;
    }
  }

  private static class ExecuteQuery
  {

    public static Map<String, String> invoke(Client client, final String gremlinQuery,
                                             final Map<String, Object> bindings, int clientTimeoutInSeconds)
    {
      Map<String, String> map = new HashMap<>();
      ResultSet results = client.submit(gremlinQuery, bindings);
      CompletableFuture<List<Result>> completableFuture = results.all();

      if (completableFuture.isCompletedExceptionally())
      {
        completableFuture.exceptionally((Throwable throwable) -> {
          handleUnknownException(throwable);
          return null;
        });
      }
      else
      {
        try
        {
          map.putAll(completableFuture.get(clientTimeoutInSeconds, TimeUnit.SECONDS).stream().map(Result::getString)
              .map(p -> p.split(SEPARATOR)).collect(Collectors.toMap((a) -> a[0], (a) -> a[1])));
        }
        catch (Throwable throwable)
        {
          handleUnknownException(throwable);
        }
      }
      return map;
    }
  }
}