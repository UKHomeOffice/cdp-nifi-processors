/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.base.CharMatcher;
import com.pontusnetworks.utils.ConcurrentLinkedHashMap;
import com.pontusnetworks.utils.StringReplacer;
import de.codecentric.elasticsearch.plugin.kerberosrealm.client.KerberizedClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.AbstractElasticsearchTransportClientProcessor;
import org.apache.nifi.ssl.SSLContextService;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Leo Martins
 */


@TriggerSerially

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({
  @WritesAttribute(attribute = "pontus.id.uuid", description = "The UUID associated with the incoming record"),
  @WritesAttribute(attribute = "pontus.id.type", description = "The type of UUID (NEW, EXISTING)"),
  @WritesAttribute(attribute = "pontus.match.status", description = "The status associated with the record " +
    "(MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH) ")
})

@Tags({"pontus", "POLE", "GUID", "UUID", "elasticsearch"})
@CapabilityDescription("Gets the incoming object, checks Elastic to see whether there's a matching record, and if there " +
  "is, returns its GUID; if there isn't, it creates a new GUID.")
public class PontusElasticIdGenerator extends AbstractElasticsearchTransportClientProcessor {

  public static enum MatchStatus {
    MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH
  }

  public static enum UUIDType {
    NEW, EXISTING, NONE
  }

  static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
    .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

  static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
    .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

  static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
    .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
    .build();

  final PropertyDescriptor ENABLE_KERBEROS = new PropertyDescriptor.Builder()
    .name("Enable Kerberos Authentication")
    .description("Enables the use of Kerberos (defaults to true).  If set to false, all the Kerberos-related options are ignored.")
    .required(true)
    .defaultValue("true")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .build();

  final PropertyDescriptor ENABLE_ACTIVE_DIRECTORY = new PropertyDescriptor.Builder()
    .name("Enable Active Directory Authentication")
    .description("Enables the use of Active Directory (this trumps the  Enable Kerberos Authentication) for authentication.  If set to false, the logic in Enable Kerberos Authentication is used.")
    .required(true)
    .defaultValue("true")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .build();


  final PropertyDescriptor KERBEROS_INITIATOR_PRINCIPAL = new PropertyDescriptor.Builder()
    .name("Kerberos Initiator Principal")
    .description("Specifies the Kerberos Principal by the client used to connect to ElasticSearch using the Shield plugin with a Kerberos Realm.")
    .required(true)
    .defaultValue("hbase/sandbox.hortonworks.com@EU-WEST-1.COMPUTE.AMAZONAWS.COM")
    .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
    .expressionLanguageSupported(true)
    .dynamic(true)
    .build();

  final PropertyDescriptor KERBEROS_ACCEPTOR_PRINCIPAL = new PropertyDescriptor.Builder()
    .name("Kerberos Acceptor Principal")
    .description("Specifies the Kerberos Acceptor Principal by the server to accept the connection (this should match the shield.authc.realms.cc-kerberos.acceptor_principal option in the server's config file).")
    .required(true)
    .defaultValue("HTTP/sandbox.hortonworks.com@EU-WEST-1.COMPUTE.AMAZONAWS.COM")
    .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
    .expressionLanguageSupported(false)
    .build();


  final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder()
    .name("Kerberos Keytab Path")
    .description("Specifies the Kerberos Keytab used to authenticate the Kerberos Initiator Principal (only used if the Kerberos Password is not present) to ElasticSearch using the Shield plugin with a Kerberos Realm.")
    .required(false)
    .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
    .defaultValue("/etc/security/keytabs/hbase.service.keytab")
    .build();

  final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
    .name("Kerberos Password")
    .description("Specifies the Kerberos Password (if this is not present, the Kerberos Keytab Path is used) to authenticate the Kerberos Initiator Principal to ElasticSearch using the Shield plugin with a Kerberos Realm.")
    .required(false)
    .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
    .defaultValue("")
    .dynamic(true)
    .expressionLanguageSupported(true)
    .build();


  final PropertyDescriptor ELASTIC_YML_CONF = new PropertyDescriptor.Builder()
    .name("Elasticsearch.yml Path")
    .description("Specifies the full path to the elasticsearch.yml file.")
    .required(true)
    .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
    .defaultValue("/opt/elasticsearch-2.4.3-shield/config/elasticsearch.yml")
    .build();

  final PropertyDescriptor OUTPUT_PREFIX = new PropertyDescriptor.Builder()
    .name("Output Prefix")
    .description("Specifies the prefix to add to the output entries (e.g. if the default of pontus is used, this will create pontus.id.uuid, pontus.id.type, pontus.match.status) .")
    .required(true)
    .defaultValue("pontus")
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .expressionLanguageSupported(false)
    .build();


  final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
    .name("Index")
    .description("The name of the index to insert into")
    .required(true)
    .expressionLanguageSupported(false)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .build();

  final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
    .name("Type")
    .description("The type of this document (used by Elasticsearch for indexing and searching)")
    .required(true)
    .expressionLanguageSupported(false)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .build();

  final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
    .name("Index Operation")
    .description("The type of the operation used to index (index, update, upsert)")
    .required(true)
    .expressionLanguageSupported(false)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .defaultValue("index")
    .build();

  final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
    .name("Batch Size")
    .description("The preferred number of FlowFiles to put to the database in a single transaction")
    .required(true)
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .defaultValue("100")
    .build();


  final PropertyDescriptor UUID_NAMESPACE = new PropertyDescriptor.Builder()
    .name("UUID namespace (defaults to the mac address of the main NIC on the server")
    .defaultValue(EthernetAddress.fromInterface().toString())
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

//  static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
//    .name("Table Name")
//    .description("The name of the HBase Table to put data into")
//    .required(true)
//    .expressionLanguageSupported(false)
//    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//    .build();
//    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
//            .name("Columns")
//            .description("A comma-separated list of \"<colFamily>:<colQualifier>\" pairs to return when scanning. To return all columns " +
//                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
//            .required(false)
//            .expressionLanguageSupported(false)
//            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
//            .build();

  final Validator filterExpressionVal = new Validator() {
    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
      ValidationResult.Builder builder = new ValidationResult.Builder();
      boolean isAscii = CharMatcher.ASCII.matchesAllOf(input);
      builder.valid(isAscii);
      builder.input(input);
      builder.subject(subject);

      if (!isAscii) {
        builder.explanation("Found non-ascii characters in the string; perhaps you copied and pasted from a word doc or web site?");
      }


      ValidationResult res = builder.build();


      return res;
    }
  };

  final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder()
    .name("Filter Expression")
    .description("An ElasticSearch Filter String with replaceable tokens using ~~{attrib}~~ attributes from the incoming file flow")
    .required(true)
    .expressionLanguageSupported(false)
    .addValidator(filterExpressionVal)
    .build();


  final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
    .name("Character Set")
    .description("Specifies which character set is used to encode the data in HBase")
    .required(true)
    .defaultValue("UTF-8")
    .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
    .build();

  final PropertyDescriptor FORCE_NEW_GUID = new PropertyDescriptor.Builder()
    .name("Force new GUID")
    .description("Determines whether a new GUID is returned blindly without checking HBASE first.")
    .required(false)
    .defaultValue("false")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .build();

  final PropertyDescriptor PUT_OBJ_WITH_GUID = new PropertyDescriptor.Builder()
    .name("Add Object")
    .description("Determines whether the resulting object should be added to HBASE using the attribute names as columns.")
    .required(true)
    .defaultValue("true")
    .allowableValues("true", "false")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .build();


  private Pattern colonSplitPattern = Pattern.compile(":");


  TimeBasedGenerator uuidGen = null;

//    HBaseClientService hBaseClientService = null;

//  String tableName = null;

  String charSet = "UTF-8";

  String filterExpression = null;

  String rowKeyString = null;

  String kerberosInitiatorPrincipal = "hbase/sandbox.hortonworks.com@EU-WEST-1.COMPUTE.AMAZONAWS.COM";
  String kerberosAcceptorPrincipal = "HTTP/sandbox.hortonworks.com@EU-WEST-1.COMPUTE.AMAZONAWS.COM";

  String kerberosKeytab = "/etc/security/keytabs/hbase.service.keytab";
  String kerberosPassword = null;

  String elasticSearchYmlPathStr = "/opt/elasticsearch-2.4.3-shield/config/elasticsearch.yml";

  String index = null;

  String type = null;
  String outputPrefix = "pontus";

  String uuidAttrib = outputPrefix + "id.uuid";
  String typeAttrib = outputPrefix + "id.type";
  String matchAttrib = outputPrefix + "pontus.match.status";


  boolean enableKerberos = true;
  boolean enableAD = true;


  ConcurrentLinkedHashMap<String, AbstractClient> kerberizedClientCache = new ConcurrentLinkedHashMap<>(1000);

  boolean forceNewGUID = false;

  boolean putObjWithGUID = true;

  final static Set<Relationship> rels = new HashSet<>();

  static {
    rels.add(REL_SUCCESS);
    rels.add(REL_FAILURE);
    rels.add(REL_RETRY);
  }

  private String createUUID() {
    UUID uuid = uuidGen.generate();

    return uuid.toString();
  }

  @Override
  public Set<Relationship> getRelationships() {
    return rels;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(ENABLE_ACTIVE_DIRECTORY);
    properties.add(ENABLE_KERBEROS);

    properties.add(KERBEROS_ACCEPTOR_PRINCIPAL);
    properties.add(OUTPUT_PREFIX);
    properties.add(KERBEROS_INITIATOR_PRINCIPAL);
    properties.add(KERBEROS_PASSWORD);
    properties.add(KERBEROS_KEYTAB);

    properties.add(CHARSET);
    properties.add(FILTER_EXPRESSION);
    properties.add(UUID_NAMESPACE);
    properties.add(FORCE_NEW_GUID);
    properties.add(PUT_OBJ_WITH_GUID);

    properties.add(CLUSTER_NAME);
    properties.add(HOSTS);
    properties.add(PROP_SSL_CONTEXT_SERVICE);
    properties.add(PROP_SHIELD_LOCATION);
    properties.add(ELASTIC_YML_CONF);

    properties.add(PING_TIMEOUT);
    properties.add(SAMPLER_INTERVAL);

    properties.add(INDEX);
    properties.add(TYPE);
    properties.add(CHARSET);
    properties.add(BATCH_SIZE);
    properties.add(INDEX_OP);


    return properties;
  }

  @Override
  public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
    if (descriptor.equals(UUID_NAMESPACE)) {
      uuidGen = null;
    }
    else if (descriptor.equals(ENABLE_KERBEROS)) {
      enableKerberos = Boolean.parseBoolean(newValue);

    }
    else if (descriptor.equals(ENABLE_ACTIVE_DIRECTORY)){
      enableAD = Boolean.parseBoolean(newValue);
    }
    else if (descriptor.equals(INDEX)) {
      index = newValue;
    }
    else if (descriptor.equals(TYPE)) {
      type = newValue;
    }
    else if (descriptor.equals(OUTPUT_PREFIX)) {
      outputPrefix = newValue;
      uuidAttrib = outputPrefix + "id.uuid";
      typeAttrib = outputPrefix + "id.type";
      matchAttrib = outputPrefix + "pontus.match.status";
    }

    else if (descriptor.equals(KERBEROS_INITIATOR_PRINCIPAL)) {
      kerberosInitiatorPrincipal = newValue;
    }
    else if (descriptor.equals(KERBEROS_ACCEPTOR_PRINCIPAL)) {
      kerberosAcceptorPrincipal = newValue;
//      kerberizedClient = null;
    }
    else if (descriptor.equals(KERBEROS_KEYTAB)) {
      kerberosKeytab = newValue;
//      kerberizedClient = null;
    }
    else if (descriptor.equals(KERBEROS_PASSWORD)) {
      kerberosPassword = newValue;
//      kerberizedClient = null;
    }


    else if (descriptor.equals(FILTER_EXPRESSION)) {
      filterExpression = newValue;
    }
    else if (descriptor.equals(CHARSET)) {
      if (charSet == null) {
        charSet = "UTF-8";
      }
      else {
        charSet = newValue;
      }
    }
    else if (descriptor.equals(FORCE_NEW_GUID)) {
      forceNewGUID = Boolean.parseBoolean(newValue);
    }
    else if (descriptor.equals(PUT_OBJ_WITH_GUID)) {
      putObjWithGUID = Boolean.parseBoolean(newValue);
    }
    else if (descriptor.equals(ELASTIC_YML_CONF)) {
      elasticSearchYmlPathStr = newValue;
    }


  }

  private List<InetSocketAddress> getEsHosts(String hosts) {

    if (hosts == null) {
      return null;
    }
    final List<String> esList = Arrays.asList(hosts.split(","));
    List<InetSocketAddress> esHosts = new ArrayList<>();

    for (String item : esList) {

      String[] addresses = item.split(":");
      final String hostName = addresses[0].trim();
      final int port = Integer.parseInt(addresses[1].trim());

      esHosts.add(new InetSocketAddress(hostName, port));
    }
    return esHosts;
  }
  protected AbstractClient createADElasticsearchClient(ProcessContext context,String username,String password ) throws ProcessException {

    ComponentLog log = getLogger();
    TransportClient transportClient = null;

    log.debug("Creating ElasticSearch Client");
    try {
      final String clusterName = context.getProperty(CLUSTER_NAME).getValue();
      final String pingTimeout = context.getProperty(PING_TIMEOUT).getValue();
      final String samplerInterval = context.getProperty(SAMPLER_INTERVAL).getValue();


      final SSLContextService sslService =
        context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

      Settings.Builder settingsBuilder = Settings.settingsBuilder()
        .put("cluster.name", clusterName)
        .put("client.transport.ping_timeout", pingTimeout)
        .put("client.transport.nodes_sampler_interval", samplerInterval);

      String shieldUrl = context.getProperty(PROP_SHIELD_LOCATION).getValue();
      if (sslService != null) {
        settingsBuilder.put("shield.transport.ssl", "true")
          .put("shield.ssl.keystore.path", sslService.getKeyStoreFile())
          .put("shield.ssl.keystore.password", sslService.getKeyStorePassword())
          .put("shield.ssl.truststore.path", sslService.getTrustStoreFile())
          .put("shield.ssl.truststore.password", sslService.getTrustStorePassword());
      }

      // Set username and password for Shield
      if (username != null && username.length() > 0) {
        StringBuffer shieldUser = new StringBuffer(username);
        if (password != null && password.length() > 0) {
          shieldUser.append(":");
          shieldUser.append(password);
        }
        settingsBuilder.put("shield.user", shieldUser);

      }

      transportClient = getTransportClient(settingsBuilder, shieldUrl, username, password);

      final String hosts = context.getProperty(HOSTS).getValue();
      esHosts = getEsHosts(hosts);

      if (esHosts != null) {
        for (final InetSocketAddress host : esHosts) {
          try {
            transportClient.addTransportAddress(new InetSocketTransportAddress(host));
          } catch (IllegalArgumentException iae) {
            log.error("Could not add transport address {}", new Object[]{host});
          }
        }
      }


    } catch (Exception e) {
      log.error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
      throw new ProcessException(e);
    }
    return transportClient;
  }

  protected AbstractClient getKerberizedClient(ProcessContext context, String user,String passwd, boolean forceCreation) throws LoginException {
    final ComponentLog log = this.getLogger();
    authToken = null;
    AbstractClient kc = null;

    try {
      Client client;
//    Settings.Builder settings = Settings.settingsBuilder();
//    Path path = Paths.get(elasticSearchYmlPathStr);

//    settings.loadFromPath(path);

      if (enableAD) {

        kc = kerberizedClientCache.get(user);
        if (kc == null) {
          kc = createADElasticsearchClient(context, user,passwd);
          kerberizedClientCache.put(user, kc);
        }

        return kc;
      }
      createElasticsearchClient(context);

      if (!enableKerberos) {
        return (AbstractClient) esClient.get();
      }


      client = esClient.get();




      kc = kerberizedClientCache.get(user);

      if (passwd == null || passwd.length() == 0) {

        if (kc == null || forceCreation) {

          kc = new KerberizedClient(client, Paths.get(this.kerberosKeytab), user, kerberosAcceptorPrincipal);
          if (kc != null) {
            kerberizedClientCache.put(user, kc);
          }

        }
      }
      else {

        if (kc == null || forceCreation) {

          kc = new KerberizedClient(client, user, passwd, kerberosAcceptorPrincipal);

          if (kc != null) {
            kerberizedClientCache.put(user, kc);
          }
        }

      }
    }
    finally {
      authToken = null;
    }
    return kc;


  }

  @OnScheduled
  public void parseProps(final ProcessContext context) throws IOException {

    if (uuidGen == null) {
      PropertyValue separatorCharPropVal = context.getProperty(UUID_NAMESPACE);
      EthernetAddress addr = EthernetAddress.valueOf(separatorCharPropVal.getValue());
      uuidGen = Generators.timeBasedGenerator(addr);
    }
//    if (kerberizedClient == null) {
//      kerberizedClient = getKerberizedClient(context, false);
//    }
  }


  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ComponentLog log = this.getLogger();
    final FlowFile flowfile = session.get();
    final Map<String, String> attributes = new HashMap<>();

    if (forceNewGUID) {
      rowKeyString = createUUID();
      attributes.put(uuidAttrib, rowKeyString);
      attributes.put(typeAttrib, UUIDType.NEW.toString());
      attributes.put(matchAttrib, MatchStatus.NO_MATCH.toString());
      FlowFile localFlowFile = flowfile;
      localFlowFile = session.putAllAttributes(localFlowFile, attributes);
      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();
      return;
    }

    final Map<String, String> replacementMap = flowfile.getAttributes();


    final String startingFilter = StringReplacer.replaceTokens(replacementMap, filterExpression, 5);
    // we need to escape single quotes with a leading single quote, but because the escape char is the same as the single quote,
    // we need to do this in two steps.
    final String filter = StringReplacer.replaceAll(startingFilter, "\\", "'");

    final Set<String> uuidSet = new HashSet<>();

    rowKeyString = null;
    String user = null;
    AbstractClient kerberizedClient = null;
    try {
      user = context.getProperty(KERBEROS_INITIATOR_PRINCIPAL).evaluateAttributeExpressions(flowfile).getValue();
      String passwd = context.getProperty(KERBEROS_PASSWORD).evaluateAttributeExpressions(flowfile).getValue();

      kerberizedClient = getKerberizedClient(context,user, passwd, false);

//      QueryBuilder qb = QueryBuilders.wrapperQuery(filter);

//      QueryBuilder qb2 = QueryBuilders.boolQuery().must(qb);

      SearchResponse sr = kerberizedClient.prepareSearch().setIndices(index).setTypes(type).setQuery(filter).execute().actionGet();

      SearchHits hits = sr.getHits();

      for (SearchHit hit : hits) {
        rowKeyString = hit.getId();
        uuidSet.add(rowKeyString);
      }

      int numMatches = uuidSet.size();

      String provenanceReporterUri;
      if (numMatches == 1) {
        attributes.put(uuidAttrib, rowKeyString);
        attributes.put(typeAttrib, UUIDType.EXISTING.toString());
        attributes.put(matchAttrib, MatchStatus.MATCH.toString());

        provenanceReporterUri = ("elastic://" + index + "/?" + filter + "/" + rowKeyString);
      }
      else {
        rowKeyString = createUUID();
        attributes.put(uuidAttrib, rowKeyString);
        attributes.put(typeAttrib, UUIDType.NEW.toString());
        attributes.put(matchAttrib, (numMatches > 1) ? MatchStatus.MULTIPLE.toString() : MatchStatus.NO_MATCH.toString());

        provenanceReporterUri = ("guid://" + rowKeyString);

      }
      FlowFile localFlowFile = session.putAllAttributes(flowfile, attributes);

      session.getProvenanceReporter().receive(localFlowFile, provenanceReporterUri); // );

      if (putObjWithGUID) {

        Map<String, String> putAttribs = localFlowFile.getAttributes();

//        List<PutColumn> cols = new ArrayList<>(putAttribs.size());

        Set<Map.Entry<String, String>> putAttribsEntrySet = putAttribs.entrySet();

        StringBuilder sb = new StringBuilder();

//        for (int i = 0, ilen = hitsArray.length; i < ilen; i++) {
//          SearchHit hit = hitsArray[i];
//          String val = hit.getSourceAsString();
//          String id = hit.getId();
//          sb.append("{_id:\"").append(id).append("\", source:{").append(val).append("}");
//          results.add(i, sb.toString());
//        }
        sb.append("{");
        int colCount = 0;
        for (Map.Entry<String, String> entry : putAttribsEntrySet) {
          String key = entry.getKey();
          int delim = key.indexOf(':');

          if (delim >= 0) {
            final String colName = key.substring(delim + 1);
            final String val = entry.getValue();

            if (colCount != 0) {
              sb.append(",");
            }
            sb.append("\"").append(colName).append("\":\"").append(val).append("\"");

            colCount++;
          }


        }

        sb.append("}");

        BulkRequestBuilder brb = new BulkRequestBuilder(kerberizedClient, BulkAction.INSTANCE);

        IndexRequestBuilder irb = new IndexRequestBuilder(kerberizedClient, IndexAction.INSTANCE);

        IndexRequest ir = irb.setId(rowKeyString).setSource(sb.toString()).request();
        brb.add(ir);
        kerberizedClient.bulk(brb.request());

      }

      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();

      return;


    } catch (NoNodeAvailableException e) {
      log.error("Failed to receive data from Elastic error: {}", e);

      cleanupKerberosClient(user, kerberizedClient);

    } catch (Exception e) {
      log.error("Failed to receive data from Elastic error: {}", e);
    }

    session.rollback();
    return;


  }

  protected void cleanupKerberosClient(String user, AbstractClient kerberizedClient) {

    if (kerberizedClient != null){
      kerberizedClient.close();
      kerberizedClientCache.remove(user);
    }
    esClient.set(null);

  }

}
