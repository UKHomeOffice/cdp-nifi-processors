/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.javatuples.Pair;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.shaded.jackson.core.JsonEncoding.UTF8;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

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

@EventDriven @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({ @WritesAttribute(attribute = "reqUUID", description = "UUID from the query"),
    @WritesAttribute(attribute = "pontus.id.type", description = "The type of UUID (NEW, EXISTING)"),
    @WritesAttribute(attribute = "pontus.match.status", description = "The status associated with the record "
        + "(MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH) ") })

@Tags({ "pontus", "TINKERPOP", "GREMLIN" }) @CapabilityDescription(
    "Reads data from attributes, and puts it into a TinkerPop Gremlin Server using a given query.  "
        + "Each of the schema fields is passed as a variable to the tinkerpop query string.  "
        + "As an example, if the server has a graph created as g1, then we can create an alias pointing \"g1\" to \"g\" by "
        + "using the alias option." + "Then, the tinkerpop query looks like this:\n"
        + "'v1 = g.addV(\"person\").property(id, tp_userID1).property(\"name\", tp_userName1).property(\"age\", tp_userAge1).next()\n"
        + "v2 = g.addV(\"software\").property(id, tp_userID2).property(\"name\", tp_userName2).property(\"lang\", tp_userLang2).next()\n"
        + "g.addE(\"created\").from(v1).to(v2).property(id, tp_relId1).property(\"weight\", tp_relWeight1)\n', then the "
        + "variables tp_userID1, tp_userName1, tp_userID2, tp_userName2, tp_userLang2, tp_relId1, tp_relWeight1 would all be taken from the "
        + "flowfile attributes that start with the prefix set in the tinkerpop query parameter prefix (e.g. tp_).")

public class PontusTinkerPopClient extends AbstractProcessor
{

  final PropertyDescriptor PONTUS_GRAPH_EMBEDDED_SERVER = new PropertyDescriptor.Builder()
      .name("Tinkerpop Embedded Server").description(
          "Specifies whether an embedded Pontus Graph server should be used inside nifi. "
              + " If this is set to true, the Tinkerpop Client configuration URI is used to point to the gremlin-server.yml"
              + " file that will configure an embedded server.").required(false)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true")
      //            .identifiesControllerService(HBaseClientService.class)
      .build();

  final PropertyDescriptor TINKERPOP_CLIENT_CONF_FILE_URI = new PropertyDescriptor.Builder()
      .name("Tinkerpop Client configuration URI").description(
          "Specifies the configuration file to configure this connection to tinkerpop (if embedded, this is the gremlin-server.yml file).")
      .required(false).addValidator(StandardValidators.URI_VALIDATOR)
      //            .identifiesControllerService(HBaseClientService.class)
      .build();

  final PropertyDescriptor TINKERPOP_QUERY_PARAM_PREFIX = new PropertyDescriptor.Builder()
      .name("Tinkerpop query parameter prefix").description(
          "Any flowfile attributes with this prefix will be sent to tinkerpop (with the prefix included)."
              + "  NOTE: This is ignored when using the Record-based client.").required(false).defaultValue("pg_")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      //            .identifiesControllerService(HBaseClientService.class)
      .build();

  final PropertyDescriptor TINKERPOP_ALIAS = new PropertyDescriptor.Builder().name("Tinkerpop graph alias")
      .description("This will create an alias of g to any pre-configured graph in the server.").required(true)
      .defaultValue("g1").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      //            .identifiesControllerService(HBaseClientService.class)
      .build();

  public static final PropertyDescriptor WAITING_TIME = new PropertyDescriptor.Builder().name("ClientTimeoutInSeconds")
      .description("Specifies client timeout (in seconds) waiting for a remote Gremlin query response.").required(true)
      .defaultValue("20").addValidator(StandardValidators.NUMBER_VALIDATOR).build();

  protected int timeoutInSecs = 20;

  //    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");

  //    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
  //            .name("Columns")
  //            .description("A comma-separated list of \"<colFamily>:<colQualifier>\" pairs to return when scanning. To return all columns " +
  //                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
  //            .required(false)
  //            .expressionLanguageSupported(false)
  //            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
  //            .build();

  final PropertyDescriptor TINKERPOP_QUERY_STR = new PropertyDescriptor.Builder().name("Tinkerpop Query")
      .description("A Tinkerpop 3.3.0 query.  ").required(true).expressionLanguageSupported(false).defaultValue(
          "v1 = g.addV(\"person\").property(id, userID1).property(\"name\", userName1).property(\"age\", userAge1).next()\n"
              + "v2 = g.addV(\"software\").property(id, userID2).property(\"name\", userName2).property(\"lang\", userLang2).next()\n"
              + "g.addE(\"created\").from(v1).to(v2).property(id, relId1).property(\"weight\", relWeight1)\n")
      .addValidator((subject, input, context) -> {
        boolean isAscii = CharMatcher.ASCII.matchesAllOf(input);
        ValidationResult.Builder builder = new ValidationResult.Builder();

        builder.valid(isAscii);
        builder.input(input);
        builder.subject(subject);

        if (!isAscii)
        {
          builder.explanation(
              "Found non-ascii characters in the string; perhaps you copied and pasted from a word doc or web site?");
        }

        ValidationResult res = builder.build();

        return res;
      }).build();

  final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
      .description("FlowFiles are routed to this relationship if the query was successful").build();

  final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
      .description("FlowFiles are routed to this relationship if the query was unsuccessful").build();

  //    private Pattern colonSplitPattern = Pattern.compile(":");

  String confFileURI = null;
  //  Configuration conf = null;

  String queryStr = null;
  String queryAttribPrefixStr = "pg_";
  String aliasStr = "g1";
  Client client = null;

  Boolean useEmbeddedServer = null;
  ServerGremlinExecutor embeddedServer = null;
  Cluster cluster = null;
  Set<Relationship> relationships = new HashSet<>();

  Settings settings;
  List<Settings.SerializerSettings> serializersSettings;
  protected final Map<String, MessageTextSerializer> serializers = new HashMap<>();

  public PontusTinkerPopClient()
  {
    relationships.add(REL_FAILURE);
    relationships.add(REL_SUCCESS);

  }

  protected void handleError(Throwable e, FlowFile flowFile, ProcessSession session, ProcessContext context)
  {

    getLogger().error("Failed to process {}; will route to failure", new Object[] { flowFile, e });
    //    session.transfer(flowFile, REL_FAILURE);

    closeClient("Error");
    if (flowFile != null)
    {
      flowFile = session.putAttribute(flowFile, "PontusTinkerPopClient.error", e.getMessage());
      flowFile = session.putAttribute(flowFile, "PontusTinkerPopClient.error.stacktrace", getStackTrace(e));
      session.transfer(flowFile, REL_FAILURE);
    }
    else
    {
      FlowFile ff = session.create();
      ff = session.putAttribute(ff, "PontusTinkerPopClient.error", e.getMessage());
      ff = session.putAttribute(ff, "PontusTinkerPopClient.error.stacktrace", getStackTrace(e));
      session.transfer(ff, REL_FAILURE);
    }
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException)
    {
      try
      {
        if (cause.getCause() instanceof TimeoutException)
        {
          parseProps(context);
        }
        else if (cause.getCause() instanceof RuntimeException)
        {
          cause = cause.getCause();
          if (cause.getCause() instanceof TimeoutException)
          {
            parseProps(context);
          }

        }
      }
      catch (Throwable t)
      {
        getLogger().error("Failed to reconnect {}", new Object[] { t });

      }
    }
  }

  //     new HashSet<>();
  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  //    @Override
  //    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
  ////        final String columns = validationContext.getProperty(COLUMNS).getValue();
  //
  //        final List<ValidationResult> problems = new ArrayList<>();
  //
  ////        if (StringUtils.isBlank(columns)) {
  ////            problems.add(new ValidationResult.Builder()
  ////                    .valid(false)
  ////                    .explanation("a filter expression can not be used in conjunction with the Columns property")
  ////                    .build());
  ////        }
  //
  //
  //
  //        return problems;
  //    }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(PONTUS_GRAPH_EMBEDDED_SERVER);
    properties.add(TINKERPOP_CLIENT_CONF_FILE_URI);
    properties.add(TINKERPOP_QUERY_STR);
    properties.add(WAITING_TIME);
    properties.add(TINKERPOP_QUERY_PARAM_PREFIX);
    return properties;
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(PONTUS_GRAPH_EMBEDDED_SERVER))
    {
      embeddedServer = null;
      useEmbeddedServer = null;
    }
    else if (descriptor.equals(TINKERPOP_CLIENT_CONF_FILE_URI))
    {
      confFileURI = null;
    }
    else if (descriptor.equals(TINKERPOP_QUERY_STR))
    {
      queryStr = null;
    }
    else if (descriptor.equals(TINKERPOP_QUERY_PARAM_PREFIX))
    {
      queryAttribPrefixStr = newValue;
    }
    else if (descriptor.equals(TINKERPOP_ALIAS))
    {
      aliasStr = null;
    }
    else if (descriptor.equals(WAITING_TIME))
    {
      timeoutInSecs = Integer.parseInt(newValue);
    }

  }

  public void setDefaultConfigs() throws Exception
  {

    if (useEmbeddedServer)
    {
      createEmbeddedServer();

    }

    //    else
    //    {
    //      DefaultConfigurationBuilder confBuilder = new DefaultConfigurationBuilder();
    //
    //      conf = confBuilder.getConfiguration(false);
    //      conf.setProperty("port", 8182);
    //      conf.setProperty("nioPoolSize", 1);
    //      conf.setProperty("workerPoolSize", 1);
    //      //                    conf.setProperty("username", "root");
    //      //                    conf.setProperty("password", "pa55word");
    //      //                    conf.setProperty("jaasEntry", "tinkerpop");
    //      //                    conf.setProperty("protocol", "GSSAPI");
    //      conf.setProperty("hosts", "127.0.0.1");
    //      conf.setProperty("serializer.className", "org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0");
    //      conf.setProperty("serializer.config",
    //          "{ ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0, org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] }");
    //
    //      //        conf.setProperty("serializer.className", "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0");
    //      //        conf.setProperty("serializer.config", "{ ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0, org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry] , useMapperFromGraph: graph}");
    //
    //      //        conf.setProperty("connectionPool.channelizer", "org.apache.tinkerpop.gremlin.driver.Channelizer.WebSocketChannelizer");
    //      //        conf.setProperty("connectionPool.enableSsl", false);
    //      //        conf.setProperty("connectionPool.trustCertChainFile", "");
    //      conf.setProperty("connectionPool.minSize", 1);
    //      conf.setProperty("connectionPool.maxSize", 1);
    //      conf.setProperty("connectionPool.minSimultaneousUsagePerConnection", 1);
    //      conf.setProperty("connectionPool.maxSimultaneousUsagePerConnection", 1);
    //      conf.setProperty("connectionPool.maxInProcessPerConnection", 1);
    //      conf.setProperty("connectionPool.minInProcessPerConnection", 1);
    //      conf.setProperty("connectionPool.maxSimultaneousUsagePerConnection", 1);
    //      //        conf.setProperty("connectionPool.maxWaitForConnection", 200000);
    //      conf.setProperty("connectionPool.maxContentLength", 2000000);
    //      //        conf.setProperty("connectionPool.reconnectInterval", 2000);
    //      //        conf.setProperty("connectionPool.resultIterationBatchSize", 200000);
    //      //        conf.setProperty("connectionPool.keepAliveInterval", 1800000);
    //    }
  }

  protected static String getAbsolutePath(final File configParent, final String file)
  {
    final File storeDirectory = new File(file);
    if (!storeDirectory.isAbsolute())
    {
      String newFile = configParent.getAbsolutePath() + File.separator + file;
      return newFile;
    }
    else
    {
      return file;
    }
  }

  protected static CommonsConfiguration getLocalConfiguration(File file, String user, String pass)
  {
    Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
        "Need to specify a readable configuration file, but was given: %s", file.toString());

    try
    {
      PropertiesConfiguration configuration = new PropertiesConfiguration(file);

      final File tmpParent = file.getParentFile();
      final File configParent;

      if (null == tmpParent)
      {
        /*
         * null usually means we were given a JanusGraph config file path
         * string like "foo.properties" that refers to the current
         * working directory of the process.
         */
        configParent = new File(System.getProperty("user.dir"));
      }
      else
      {
        configParent = tmpParent;
      }

      Preconditions.checkNotNull(configParent);
      Preconditions.checkArgument(configParent.isDirectory());

      // TODO this mangling logic is a relic from the hardcoded string days; it should be deleted and rewritten as a setting on ConfigOption
      final Pattern p = Pattern.compile(
          "(" + Pattern.quote(STORAGE_NS.getName()) + "\\..*" + "(" + Pattern.quote(STORAGE_DIRECTORY.getName()) + "|"
              + Pattern.quote(STORAGE_CONF_FILE.getName()) + ")" + "|" + Pattern.quote(INDEX_NS.getName()) + "\\..*"
              + "(" + Pattern.quote(INDEX_DIRECTORY.getName()) + "|" + Pattern.quote(INDEX_CONF_FILE.getName()) + ")"
              + ")");

      final Iterator<String> keysToMangle = Iterators
          .filter(configuration.getKeys(), key -> null != key && p.matcher(key).matches());

      while (keysToMangle.hasNext())
      {
        String k = keysToMangle.next();
        Preconditions.checkNotNull(k);
        final String s = configuration.getString(k);
        Preconditions.checkArgument(org.apache.commons.lang.StringUtils.isNotBlank(s),
            "Invalid Configuration: key %s has null empty value", k);
        configuration.setProperty(k, getAbsolutePath(configParent, s));
      }
      return new CommonsConfiguration(configuration);
    }
    catch (ConfigurationException e)
    {
      throw new IllegalArgumentException("Could not load configuration at: " + file, e);
    }
  }

  public ServerGremlinExecutor createEmbeddedServer() throws URISyntaxException, IOException
  {
    final ComponentLog log = this.getLogger();

    try
    {
      settings = Settings.read(new URI(confFileURI).toURL().openStream());
    }
    catch (Throwable t)
    {
      log.warn("Failed to open " + confFileURI
          + "; attempting default /opt/pontus/pontus-graph/current/conf/gremlin-server.yml; error: " + t.getMessage());

      settings = Settings
          .read(new URI("file:///opt/pontus/pontus-graph/current/conf/gremlin-server.yml").toURL().openStream());
    }
    serializersSettings = settings.serializers;

    String gconfFileStr = (String) settings.graphs
        .getOrDefault("graph", "/opt/pontus/pontus-graph/current/conf/janusgraph-hbase-es.properties");

    File gconfFile = new File(gconfFileStr);
    CommonsConfiguration conf = getLocalConfiguration(gconfFile, null, null);

    JanusGraph graph = JanusGraphFactory.open(conf);

    embeddedServer = new ServerGremlinExecutor(settings, null, null);
    embeddedServer.getGraphManager().putTraversalSource("g", graph.traversal());
    embeddedServer.getGraphManager().putGraph("graph", graph);

    configureSerializers();

    return embeddedServer;

  }

  @OnScheduled public void parseProps(final ProcessContext context) throws IOException
  {

    final ComponentLog log = this.getLogger();

    if (useEmbeddedServer == null)
    {
      useEmbeddedServer = context.getProperty(PONTUS_GRAPH_EMBEDDED_SERVER).asBoolean();

    }

    if (queryStr == null)
    {
      queryStr = context.getProperty(TINKERPOP_QUERY_STR).getValue();
    }
    if (aliasStr == null)
    {
      aliasStr = context.getProperty(TINKERPOP_ALIAS).getValue();
    }
    if (confFileURI == null)
    {
      PropertyValue confFileURIProp = context.getProperty(TINKERPOP_CLIENT_CONF_FILE_URI);
      confFileURI = confFileURIProp.getValue();

      if (StringUtils.isEmpty(confFileURI))
      {
        try
        {
          setDefaultConfigs();
        }
        catch (Exception e2)
        {
          log.error("Failed set Default URL config", e2);
          return;
        }
      }
      else
      {
        try
        {
          if (useEmbeddedServer)
          {
            createEmbeddedServer();
          }
        }
        catch (Exception e)
        {
          log.warn("Failed to read URL config; using default values", e);

          try
          {
            setDefaultConfigs();
          }
          catch (Exception e2)
          {
            log.error("Failed set Default URL config", e2);
            return;
          }
        }
      }
      if (client != null)
      {
        client.close();
      }
      if (cluster != null)
      {
        cluster.close();
      }

      if (!useEmbeddedServer)
      {

        try
        {
          createClient();
        }
        catch (URISyntaxException e)
        {
          log.error("Failed create client with  URI config " + confFileURI, e);
          return;

        }
      }
    }

  }

  protected void configureSerializers()
  {

    // grab some sensible defaults if no serializers are present in the config
    //    final List<Settings.SerializerSettings> serializerSettings =
    //        (null == this.settings.serializers || this.settings.serializers.isEmpty()) ? DEFAULT_SERIALIZERS : settings.serializers;

    serializersSettings.stream().map(config -> {
      try
      {
        final Class clazz = Class.forName(config.className);
        if (!MessageSerializer.class.isAssignableFrom(clazz))
        {
          //          logger.warn("The {} serialization class does not implement {} - it will not be available.", config.className, MessageSerializer.class.getCanonicalName());
          return Optional.<MessageSerializer>empty();
        }

        //        if (clazz.getAnnotation(Deprecated.class) != null)
        //          logger.warn("The {} serialization class is deprecated.", config.className);

        final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
        final Map<String, Graph> graphsDefinedAtStartup = new HashMap<>();
        for (String graphName : settings.graphs.keySet())
        {
          graphsDefinedAtStartup.put(graphName, this.embeddedServer.getGraphManager().getGraph(graphName));
        }

        if (config.config != null)
          serializer.configure(config.config, graphsDefinedAtStartup);

        return Optional.ofNullable(serializer);
      }
      catch (ClassNotFoundException cnfe)
      {
        //        logger.warn("Could not find configured serializer class - {} - it will not be available", config.className);
        return Optional.<MessageSerializer>empty();
      }
      catch (Exception ex)
      {
        //        logger.warn("Could not instantiate configured serializer class - {} - it will not be available. {}", config.className, ex.getMessage());
        return Optional.<MessageSerializer>empty();
      }
    }).filter(Optional::isPresent).map(Optional::get)
        .flatMap(ser -> Stream.of(ser.mimeTypesSupported()).map(mimeType -> Pair.with(mimeType, ser))).forEach(pair -> {
      final String mimeType = pair.getValue0();

      MessageSerializer ser = pair.getValue1();
      if (ser instanceof MessageTextSerializer)
      {
        final MessageTextSerializer serializer = (MessageTextSerializer) ser;

        if (!serializers.containsKey(mimeType))
        {
          //        logger.info("Configured {} with {}", mimeType, pair.getValue1().getClass().getName());
          serializers.put(mimeType, serializer);
        }
      }
    });

    if (serializers.size() == 0)
    {
      //      logger.error("No serializers were successfully configured - server will not start.");
      throw new RuntimeException("Serialization configuration error.");
    }
  }

  public void checkGraphStatus() throws FileNotFoundException, URISyntaxException
  {

    if (!useEmbeddedServer && this.cluster != null && (this.cluster.isClosed() || this.cluster.isClosing()))
    {

      closeClient("Recover from failure");

      createClient();

    }
  }

  public void createClient() throws URISyntaxException, FileNotFoundException
  {
    if (!useEmbeddedServer)
    {

      URI uri = new URI(confFileURI);

      cluster = Cluster.build(new File(uri)).create();

      //            Cluster.open(conf);

      client = cluster.connect();
    }

  }

  public void closeClient(String reason)
  {

    if (!useEmbeddedServer)
    {
      getLogger().info("Closing remote graph client - reason: " + reason);
      if (client != null)
      {
        client.close();
      }
      if (cluster != null)
      {
        cluster.close();
      }
    }
  }

  @OnStopped public void stopped()
  {
    closeClient("stopped");
  }

  public Bindings getBindings(FlowFile flowfile)
  {
    Map<String, String> allAttribs = flowfile.getAttributes();

    Map<String, Object> tinkerpopAttribs = allAttribs.entrySet().stream()
        .filter((entry -> entry.getKey().startsWith(queryAttribPrefixStr)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final Bindings bindings = new SimpleBindings(tinkerpopAttribs);

    if (embeddedServer != null)
    {
      bindings.putAll(embeddedServer.getGraphManager().getAsBindings());

    }
    return bindings;
  }

  public String getQueryStr(ProcessSession session)
  {
    return queryStr;
  }

  public byte[] runQuery(Bindings bindings, String queryString)
      throws ExecutionException, InterruptedException, IOException, URISyntaxException
  {

    if (useEmbeddedServer && embeddedServer == null)
    {
      embeddedServer = createEmbeddedServer();
    }
    if (useEmbeddedServer && embeddedServer != null)
    {
      final GremlinExecutor gremlinExecutor = embeddedServer.getGremlinExecutor();
      //        final MessageTextSerializer serializer = embeddedServer.getGraphManager();

      MessageTextSerializer serializer = serializers.get("application/json");

      final CompletableFuture<Object> evalFuture = gremlinExecutor
          .eval(queryString, null, bindings, FunctionUtils.wrapFunction(o -> {
            final ResponseMessage responseMessage = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS).result(IteratorUtils.asList(o)).create();

            try
            {
              return Unpooled
                  .wrappedBuffer(serializer.serializeResponseAsString(responseMessage).getBytes(UTF8.getJavaName()));
            }
            catch (Exception ex)
            {
              //                logger.warn(String.format("Error during serialization for %s", responseMessage), ex);
              throw ex;
            }
          }));

      evalFuture.exceptionally(t -> {
        if (t.getMessage() != null)
        {
          getLogger().error("Server Error " + t.getMessage());
          //                                    session.transfer(tempFlowFile, REL_FAILURE);

          throw new ProcessException(t);

          //            sendError(ctx, INTERNAL_SERVER_ERROR, t.getMessage(), Optional.of(t));
        }
        else
        {
          getLogger().error(String.format("Error encountered evaluating script: %s", queryStr, Optional.of(t)));
        }
        return null;
      });

      ByteBuf buf = (ByteBuf) evalFuture.get();

      if (buf != null)
      {
        return buf.array();
      }

    }
    else
    {
      Map<String, Object> props = new HashMap<>(bindings);
      getLogger().debug("Custer client : queryString ---> " + queryString + ", bindings ---> " + props);
      ResultSet res = client.submit(queryString, props);

      CompletableFuture<List<Result>> resFuture = res.all();

      if (resFuture.isCompletedExceptionally())
      {
        resFuture.exceptionally((Throwable throwable) -> {
          getLogger().error(
              "Server Error " + throwable.getMessage() + " orig msg: " + res.getOriginalRequestMessage().toString());
          throw new ProcessException(throwable);
        }).join();

      }

      try
      {
        final List<String> list = resFuture.get(timeoutInSecs, TimeUnit.SECONDS).stream()
            .map(result -> result.getString()).collect(Collectors.toList());

        final ResponseMessage responseMessage = ResponseMessage.build(UUID.randomUUID())
            .code(ResponseStatusCode.SUCCESS).result(list).create();

        final MessageTextSerializer messageTextSerializer = new GraphSONMessageSerializerV3d0();
        String responseAsString = messageTextSerializer.serializeResponseAsString(responseMessage);
        byte[] bytes = responseAsString.getBytes(UTF8.getJavaName());
        return Unpooled.wrappedBuffer(bytes).array();
      }
      catch (Exception ex)
      {
        getLogger().warn(String.format("Error: " + ex));
        throw new ProcessException(ex);
      }
    }

    return new byte[0];
  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {

    final ComponentLog log = this.getLogger();
    FlowFile localFlowFile = null;

    try
    {
      final FlowFile flowfile = session.get();
      if (flowfile == null)
      {
        log.error("Got a NULL flow file");
        return;
      }

      checkGraphStatus();

      final Bindings bindings = getBindings(flowfile);

      Map<String, String> allAttribs = flowfile.getAttributes();
      session.remove(flowfile);

      String queryString = getQueryStr(session);

      localFlowFile = session.create();
      localFlowFile = session.putAllAttributes(localFlowFile, allAttribs);

      byte[] res = runQuery(bindings, queryString);

      localFlowFile = session.write(localFlowFile, out -> out.write(res));

      session.transfer(localFlowFile, REL_SUCCESS);

    }
    catch (Throwable e)
    {
      handleError(e, localFlowFile, session, context);
      log.error("Failed to run query against Tinkerpop; error: {}", e);
    }

  }

  private String getStackTrace(Throwable e)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }
  
}