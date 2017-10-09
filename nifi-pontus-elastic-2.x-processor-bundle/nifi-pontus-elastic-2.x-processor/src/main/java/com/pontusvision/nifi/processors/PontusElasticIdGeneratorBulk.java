/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.jayway.jsonpath.JsonPath;
import com.pontusnetworks.utils.StringReplacer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.codehaus.jackson.JsonNode;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

/**
 * @author phillip
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
public class PontusElasticIdGeneratorBulk extends PontusElasticIdGenerator {


  final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder()
    .name("Filter Expression")
    .description("An ElasticSearch Filter String with replaceable tokens using ~~{jsonPath}~~ patterns from the incoming file flow")
    .required(true)
    .expressionLanguageSupported(false)
    .addValidator(filterExpressionVal)
    .build();


  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(KERBEROS_ACCEPTOR_PRINCIPAL);
    properties.add(OUTPUT_PREFIX);
    properties.add(ENABLE_KERBEROS);

    properties.add(KERBEROS_INITIATOR_PRINCIPAL);
    properties.add(KERBEROS_PASSWORD);
    properties.add(KERBEROS_KEYTAB);

    properties.add(CHARSET);
    properties.add(FILTER_EXPRESSION);

    properties.add(CLUSTER_NAME);
    properties.add(HOSTS);
    properties.add(PROP_SSL_CONTEXT_SERVICE);
    properties.add(PROP_SHIELD_LOCATION);
    properties.add(ELASTIC_YML_CONF);
//    properties.add(USERNAME);
//    properties.add(PASSWORD);
    properties.add(PING_TIMEOUT);
    properties.add(SAMPLER_INTERVAL);
//    properties.add(ID_ATTRIBUTE);
    properties.add(INDEX);
    properties.add(TYPE);
    properties.add(CHARSET);


    return properties;
  }

//
//  static JsonNode readJson(ProcessSession processSession, FlowFile flowFile) {
//    JsonNode rootNode = null; // or URL, Stream, Reader, String, byte[]
//
//    // Parse the document once into an associated context to support multiple path evaluations if specified
//    final AtomicReference<JsonNode> contextHolder = new AtomicReference<>(null);
//    processSession.read(flowFile, new InputStreamCallback() {
//      @Override
//      public void process(InputStream in) throws IOException {
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode rootNode = mapper.readValue(bufferedInputStream, JsonNode.class);
//        contextHolder.set(rootNode);
//      }
//    });
//
//    return contextHolder.get();
//  }

  public static void addSearchRequest(MultiSearchRequestBuilder msrb, final Map<String, JsonPath> replacementMap, JsonNode node, String filterExpression, AbstractClient kerberizedClient, String index, String type) {
    final String startingFilter = StringReplacer.replaceTokens(replacementMap, node, filterExpression, 5);
    // we need to escape single quotes with a leading single quote, but because the escape char is the same as the single quote,
    // we need to do this in two steps.
    final String filter = StringReplacer.replaceAll(startingFilter, "\\", "'");
//    QueryBuilder qb = QueryBuilders.wrapperQuery(filter);

    SearchRequest sr = kerberizedClient.prepareSearch().setIndices(index).setTypes(type).setQuery(filter).request();
    msrb.add(sr);

  }

  @Override
  public void parseProps(final ProcessContext context) throws IOException {

//      kerberizedClient = getKerberizedClient(context, false);

  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ComponentLog log = this.getLogger();
    final FlowFile flowfile = session.get();
    final Map<String, String> attributes = new HashMap<>();

//    ObjectMapper mapper = new ObjectMapper();


    AbstractClient kerberizedClient = null;
    String user = null;
    try {
      user = context.getProperty(KERBEROS_INITIATOR_PRINCIPAL).evaluateAttributeExpressions(flowfile).getValue();
      String passwd = context.getProperty(KERBEROS_PASSWORD).evaluateAttributeExpressions(flowfile).getValue();

      kerberizedClient = getKerberizedClient(context, user, passwd, false);
      final JsonNode docCtx = StringReplacer.readJson(session, flowfile);
      final Map<String, JsonPath> replacementMap = StringReplacer.parseJSONPaths(filterExpression);

      final int numResults = docCtx.size();


      MultiSearchRequestBuilder msrb = kerberizedClient.prepareMultiSearch();
      if (docCtx.isArray()) {
        for (int i = 0, ilen = numResults; i < ilen; i++) {
          JsonNode node = docCtx.get(i);
          addSearchRequest(msrb, replacementMap, node, filterExpression, kerberizedClient, index, type);

        }
      }
      else {
        JsonNode node = docCtx;
        addSearchRequest(msrb, replacementMap, node, filterExpression, kerberizedClient, index, type);
      }


      final Set<String> uuidSet = new HashSet<>();

      final ArrayList<String> uuidAttribs = new ArrayList<>(numResults);
      final ArrayList<String> typeAttribs = new ArrayList<>(numResults);
      final ArrayList<String> matchAttribs = new ArrayList<>(numResults);
      rowKeyString = null;


//      msrb.execute().actionGet();
      MultiSearchResponse msr = msrb.execute().actionGet();
      MultiSearchResponse.Item[] responses = msr.getResponses();

      for (int i = 0, ilen = responses.length; i < ilen; i++) {
        SearchResponse sr = responses[i].getResponse();
        SearchHits hits = sr.getHits();
        uuidSet.clear();
        for (SearchHit hit : hits) {
          rowKeyString = hit.getId();
          uuidSet.add(rowKeyString);
        }
        int numMatches = uuidSet.size();

        if (numMatches == 1) {
          uuidAttribs.add(rowKeyString);
          typeAttribs.add(UUIDType.EXISTING.toString());
          matchAttribs.add(MatchStatus.MATCH.toString());
        }
        else if (numMatches > 0) {
          uuidAttribs.add(rowKeyString);
          typeAttribs.add(UUIDType.EXISTING.toString());
          matchAttribs.add(MatchStatus.MULTIPLE.toString());

        }
        else {
          rowKeyString = "";
          uuidAttribs.add(rowKeyString);
          typeAttribs.add(UUIDType.NONE.toString());
          matchAttribs.add(MatchStatus.NO_MATCH.toString());

        }
      }

      attributes.put(uuidAttrib, uuidAttribs.toString());
      attributes.put(typeAttrib, typeAttribs.toString());
      attributes.put(matchAttrib, matchAttribs.toString());

//          provenanceReporterUri = ("elastic://" + index + "/?" + filter + "/" + rowKeyString);


      FlowFile localFlowFile = session.putAllAttributes(flowfile, attributes);

//      session.getProvenanceReporter().receive(localFlowFile, provenanceReporterUri); // );


      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();

      return;


    } catch (NoNodeAvailableException e) {
      log.error("Failed to receive data from Elastic error: {}", e);

      cleanupKerberosClient(user, kerberizedClient);


    } catch (Exception e) {
      log.error("Failed to receive data from Elastic error: {}", e);
    }

    FlowFile errFF = flowfile;

    session.transfer(errFF == null? session.create(): errFF, REL_FAILURE);

    session.commit();
    return;


  }

}
