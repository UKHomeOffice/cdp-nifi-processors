package com.pontusvision.nifi.processors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

class ClusterClientServiceImpl implements ClusterClientService
{

  private static final Logger logger = LoggerFactory.getLogger(ClusterClientServiceImpl.class);
  public Cluster cluster;
  public  Client client;

  private String clientYaml;

  public ClusterClientServiceImpl(String clientYaml)
  {
    try
    {
      this.clientYaml = clientYaml;

      createClient();

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


  @Override public Cluster getCluster()
  {
    return this.cluster;
  }

  @Override public Client getClient()
  {

    return this.client;

  }
  @Override public Client createClient()
  {
    try
    {
      if (this.cluster == null)
      {

        cluster = Cluster.build(new File(new URI(clientYaml))).create();
        cluster.init();

      }
      this.client = this.cluster.connect();
      this.client.init();

    }
    catch (Exception e)
    {
      handleUnknownException(e);
    }

    return this.client;

  }

  private static void handleUnknownException(Throwable throwable)
  {

    if (throwable != null)
    {
      if (!isResponseException(ExceptionUtils.getRootCause(throwable)))
      {
        throw new RuntimeException("ClusterClientServiceImpl]:  connection failed ", throwable);
      }
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
      logger.info("PontusTinkerpop :[ClusterClientServiceImpl]: Cluster client is closed. [event=" + event + "]");
      client = null;
    }
    if (this.cluster != null)
    {
      cluster.closeAsync();
      logger.info("PontusTinkerpop :[ClusterClientServiceImpl]: and Cluster is closed as well.");
      cluster = null;
    }
  }


}