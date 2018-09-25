package com.pontusvision.nifi.processors;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.List;
import java.util.Map;

public interface ClusterClientService {

  boolean isClosed();

  Map<String, String> getVids(List guidS, String binding, String gremlinQuery);

  Cluster getCluster ();

  Client getClient ();

  void close(String event);
}