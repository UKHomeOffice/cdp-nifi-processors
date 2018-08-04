package com.pontusvision.nifi.processors;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import uk.gov.homeoffice.pontus.JWTClaim;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static uk.gov.homeoffice.pontus.JWTStore.JWT_ZK_PATH_DEFVAL;

/**
 * Created by leo on 21/03/2017.
 */

public class JWTStoreClientProcessor extends AbstractProcessor
{

  public static final PropertyDescriptor JWT_ZK_PATH = new PropertyDescriptor.Builder().name("JWT Zookeeper Path")
      .description("The path where the JWT will be added to Zookeeper. ").defaultValue(JWT_ZK_PATH_DEFVAL)
      .required(true).expressionLanguageSupported(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

  public static final PropertyDescriptor JWT_ZK_HOST = new PropertyDescriptor.Builder().name("JWT Zookeeper Connection")
      .description(
          "The connection string (host:port<,host2:port>/<opt root>) to the Zookeeper cluster where the JWT will be added. ")
      .defaultValue("localhost").required(true).expressionLanguageSupported(false)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

  public static final PropertyDescriptor JWT_JSON = new PropertyDescriptor.Builder().name("JWT JSON String")
      .description("The JSON  string representation of the JWT payload, which will be added to the Zookeeper cluster. ")
      .defaultValue("").required(true).expressionLanguageSupported(true)
      .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();

  public static final PropertyDescriptor JWT_USER_JSON = new PropertyDescriptor.Builder().name("JWT USER JSON String")
      .description("The User with which to key the JWT")
      .defaultValue("${\"http.headers.Authorization\":jsonPath('$.preferred_username')}").required(true)
      .expressionLanguageSupported(true).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
      .build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();

  public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("Failure relationship").build();

  protected List<PropertyDescriptor> properties;
  protected Set<Relationship> relationships;

  String zkPath = JWT_ZK_PATH_DEFVAL;
  String zkConnection = "localhost";
  ZooKeeper zoo = null;

  // Method to connect zookeeper ensemble.
  public static ZooKeeper connect(String host) throws IOException, InterruptedException
  {

    final CountDownLatch connectedSignal = new CountDownLatch(1);
    ZooKeeper zoo = new ZooKeeper(host, 5000, new Watcher()
    {
      public void process(WatchedEvent we)
      {
        if (we.getState() == Event.KeeperState.SyncConnected)
        {
          connectedSignal.countDown();
        }
      }
    });

    connectedSignal.await();
    return zoo;
  }

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException
  {

    if (args.length != 2)
    {
      System.err.printf("This utility sets the JWT json for a user\n\nUsage: java ... <json with JWT>\n\n");
      System.err
          .printf("Example:\n\n java ... hbase '{ \"sub\":\"hbase\", \"bizctx\":\"/uk.police/investigator\"}'\n\n\n");

      System.exit(-1);
    }

    JWTClaim sampleClaim = JWTClaim.fromJson(args[0]);

    StringBuffer strBuf = new StringBuffer(JWT_ZK_PATH_DEFVAL).append("/").append(sampleClaim.getSub());

    JWTStoreClientProcessor cli = new JWTStoreClientProcessor();

    cli.connect("localhost");

    if (cli.exists(strBuf.toString()) == null)
    {
      cli.create(strBuf.toString(), args[0].getBytes());
    }
    else
    {
      cli.update(strBuf.toString(), args[0].getBytes());
    }

    System.out.println("Successfully updated user " + args[0]);

    cli.close();

  }

  public void create(String path, byte[] data) throws KeeperException, InterruptedException
  {

    String[] parts = path.split("/");
    StringBuffer strBuf = new StringBuffer();
    // LPPM - the first level is empty, as we start with a slash.
    // skip it by setting i and j = 1.
    for (int i = 1, ilen = parts.length, lastItem = parts.length - 1; i < ilen; i++)
    {
      strBuf.setLength(0);
      for (int j = 1; j <= i; j++)
      {
        strBuf.append("/").append(parts[j]);
      }
      String partialPath = strBuf.toString();
      if (exists(partialPath) == null)
      {
        CreateMode mode;
        if (i == lastItem)
        {
          mode = CreateMode.PERSISTENT;
        }
        else
        {
          mode = CreateMode.PERSISTENT;
        }
        List<ACL> perms = new ArrayList<>();
        if (UserGroupInformation.isSecurityEnabled())
        {
          perms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
          perms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        }
        else
        {
          perms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        }

        zoo.create(partialPath, data, perms, mode);

      }
    }
  }

  public Stat exists(String path) throws KeeperException, InterruptedException
  {
    return zoo.exists(path, true);
  }

  public void update(String path, byte[] data) throws KeeperException, InterruptedException
  {
    zoo.setData(path, data, zoo.exists(path, true).getVersion());
  }

  // Method to disconnect from zookeeper server
  public void close() throws InterruptedException
  {
    zoo.close();
  }

  @Override public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    FlowFile flowfile = session.get();

    if (flowfile == null)
    {
      session.transfer(flowfile, FAILURE);
      return;

    }

    if (zkPath == null)
    {
      zkPath = context.getProperty(JWT_ZK_PATH).getValue();
    }

    try
    {
      if (zoo == null)
      {
        zkConnection = context.getProperty(JWT_ZK_HOST).getValue();
        zoo = connect(zkConnection);

      }
    }
    catch (Exception e)
    {
      log.error("Failed to connect to zookeeper  " + zkConnection.toString() + ": ", e);
      session.transfer(flowfile, FAILURE);
      return;
    }

    try
    {

      String jsonStr = context.getProperty(JWT_JSON).evaluateAttributeExpressions(flowfile).getValue();

      JWTClaim sampleClaim = JWTClaim.fromJson(jsonStr);

      String user = context.getProperty(JWT_USER_JSON).evaluateAttributeExpressions(flowfile).getValue();

      sampleClaim.setSub(user);

      StringBuffer strBuf = new StringBuffer(JWT_ZK_PATH_DEFVAL).append("/").append(sampleClaim.getSub());

      String fullPath = strBuf.toString();

      if (exists(fullPath) == null)
      {
        create(fullPath, jsonStr.getBytes());
      }
      else
      {
        update(fullPath, jsonStr.getBytes());
      }

      if (log.isDebugEnabled())
      {
        log.debug("Successfully updated JWT " + jsonStr);
      }

    }
    catch (Exception e)
    {
      log.error("Failed to connect to zookeeper  " + zkConnection.toString() + ": ", e);
      session.transfer(flowfile, FAILURE);
      return;
    }

    session.transfer(flowfile, SUCCESS);

  }

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(JWT_ZK_HOST);
    properties.add(JWT_ZK_PATH);
    properties.add(JWT_JSON);
    properties.add(JWT_USER_JSON);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(JWT_ZK_HOST))
    {
      zoo = null;
    }
    else if (descriptor.equals(JWT_ZK_PATH))
    {
      zkPath = null;
    }

  }

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }
}
